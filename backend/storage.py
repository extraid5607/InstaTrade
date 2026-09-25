"""
Insta Trade - SQLite Persistent Storage Manager
Stores user accounts, credentials, funds, positions, and orders with 100% data isolation.
Zero external database dependencies (uses standard library sqlite3).
"""

import os
import sqlite3
import hashlib
import secrets
import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "trading_terminal.db"


class DatabaseManager:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._ensure_dir()
        self.init_db()

    def _ensure_dir(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # 1. Users table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    password_hash TEXT NOT NULL,
                    display_name TEXT,
                    client_id TEXT,
                    token TEXT UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 2. User Funds table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_funds (
                    username TEXT PRIMARY KEY,
                    available_cash REAL DEFAULT 1000000.0,
                    used_margin REAL DEFAULT 0.0,
                    realized_pnl REAL DEFAULT 0.0,
                    FOREIGN KEY (username) REFERENCES users (username) ON DELETE CASCADE
                )
            """)

            # 3. User Positions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_positions (
                    id TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    product TEXT DEFAULT 'INTRADAY',
                    net_qty INTEGER NOT NULL,
                    buy_avg REAL DEFAULT 0.0,
                    sell_avg REAL DEFAULT 0.0,
                    margin_held REAL DEFAULT 0.0,
                    FOREIGN KEY (username) REFERENCES users (username) ON DELETE CASCADE
                )
            """)

            # 4. User Orders table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_orders (
                    id TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    order_time TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    product TEXT NOT NULL,
                    qty INTEGER NOT NULL,
                    price REAL NOT NULL,
                    status TEXT NOT NULL,
                    FOREIGN KEY (username) REFERENCES users (username) ON DELETE CASCADE
                )
            """)
            conn.commit()

        # Seed default user if database is new
        self._seed_default_user()

    @staticmethod
    def hash_password(password: str) -> str:
        # Standard SHA256 hashing
        salt = "insta_trade_salt_2026"
        return hashlib.sha256(f"{salt}{password}".encode("utf-8")).hexdigest()

    def _seed_default_user(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as cnt FROM users")
            row = cursor.fetchone()
            if row and row["cnt"] == 0:
                default_user = "aman_trader"
                default_pass = "password123"
                token = secrets.token_hex(24)
                cursor.execute("""
                    INSERT INTO users (username, password_hash, display_name, client_id, token)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    default_user,
                    self.hash_password(default_pass),
                    "Aman Pro",
                    "XH01499",
                    token
                ))
                cursor.execute("""
                    INSERT INTO user_funds (username, available_cash, used_margin, realized_pnl)
                    VALUES (?, 1000000.0, 0.0, 0.0)
                """, (default_user,))
                conn.commit()

    # ------------------ USER AUTH ------------------
    def register_user(self, username: str, password: str, display_name: Optional[str] = None, client_id: Optional[str] = None) -> Dict[str, Any]:
        clean_user = username.strip().lower()
        if not clean_user or len(clean_user) < 3:
            return {"s": "error", "message": "Username must be at least 3 characters long"}
        if not password or len(password) < 4:
            return {"s": "error", "message": "Password must be at least 4 characters long"}

        d_name = display_name.strip() if display_name else clean_user.capitalize()
        c_id = client_id.strip().upper() if client_id else f"USR{secrets.token_hex(3).upper()}"
        pwd_hash = self.hash_password(password)
        token = secrets.token_hex(24)

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO users (username, password_hash, display_name, client_id, token)
                    VALUES (?, ?, ?, ?, ?)
                """, (clean_user, pwd_hash, d_name, c_id, token))

                cursor.execute("""
                    INSERT INTO user_funds (username, available_cash, used_margin, realized_pnl)
                    VALUES (?, 1000000.0, 0.0, 0.0)
                """, (clean_user,))
                conn.commit()

            return {
                "s": "ok",
                "message": f"Account '{clean_user}' created successfully with Rs. 10 Lakhs Virtual Capital!",
                "data": {
                    "username": clean_user,
                    "display_name": d_name,
                    "client_id": c_id,
                    "token": token
                }
            }
        except sqlite3.IntegrityError:
            return {"s": "error", "message": f"Username '{clean_user}' already exists. Please choose a different login ID."}

    def authenticate_user(self, username: str, password: str) -> Dict[str, Any]:
        clean_user = username.strip().lower()
        pwd_hash = self.hash_password(password)

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT username, display_name, client_id, password_hash
                FROM users WHERE username = ?
            """, (clean_user,))
            row = cursor.fetchone()
            if not row:
                return {"s": "error", "message": "User not found. Please register your Login ID."}

            if row["password_hash"] != pwd_hash:
                return {"s": "error", "message": "Incorrect password. Please try again."}

            token = secrets.token_hex(24)
            cursor.execute("UPDATE users SET token = ? WHERE username = ?", (token, clean_user))
            conn.commit()

            return {
                "s": "ok",
                "message": f"Welcome back, {row['display_name'] or clean_user}!",
                "data": {
                    "username": clean_user,
                    "display_name": row["display_name"] or clean_user.capitalize(),
                    "client_id": row["client_id"] or "XH01499",
                    "token": token
                }
            }

    def get_user_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        if not token:
            return None
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT username, display_name, client_id FROM users WHERE token = ?", (token.strip(),))
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None

    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT username, display_name, client_id FROM users WHERE username = ?", (username.strip().lower(),))
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None

    def update_user_credentials(self, username: str, password: Optional[str] = None, display_name: Optional[str] = None, client_id: Optional[str] = None) -> Dict[str, Any]:
        clean_user = username.strip().lower()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            updates = []
            params = []
            if password and password.strip():
                updates.append("password_hash = ?")
                params.append(self.hash_password(password.strip()))
            if display_name:
                updates.append("display_name = ?")
                params.append(display_name.strip())
            if client_id:
                updates.append("client_id = ?")
                params.append(client_id.strip().upper())

            if updates:
                params.append(clean_user)
                cursor.execute(f"UPDATE users SET {', '.join(updates)} WHERE username = ?", params)
                conn.commit()

            cursor.execute("SELECT username, display_name, client_id FROM users WHERE username = ?", (clean_user,))
            row = cursor.fetchone()
            return dict(row) if row else {}

    # ------------------ FUNDS PER USER ------------------
    def get_funds(self, username: str) -> Dict[str, Any]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT available_cash, used_margin, realized_pnl FROM user_funds WHERE username = ?", (username,))
            row = cursor.fetchone()
            if row:
                return {
                    "available_cash": float(row["available_cash"]),
                    "used_margin": float(row["used_margin"]),
                    "realized_pnl": float(row["realized_pnl"]),
                }
            # Default
            return {"available_cash": 1000000.0, "used_margin": 0.0, "realized_pnl": 0.0}

    def update_funds(self, username: str, available_cash: float, used_margin: float, realized_pnl: float):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO user_funds (username, available_cash, used_margin, realized_pnl)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    available_cash = excluded.available_cash,
                    used_margin = excluded.used_margin,
                    realized_pnl = excluded.realized_pnl
            """, (username, available_cash, used_margin, realized_pnl))
            conn.commit()

    def reset_funds(self, username: str, capital: float = 1000000.0):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE user_funds SET available_cash = ?, used_margin = 0.0, realized_pnl = 0.0
                WHERE username = ?
            """, (capital, username))
            cursor.execute("DELETE FROM user_positions WHERE username = ?", (username,))
            cursor.execute("DELETE FROM user_orders WHERE username = ?", (username,))
            conn.commit()

    # ------------------ POSITIONS PER USER ------------------
    def get_positions(self, username: str) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, symbol, side, product, net_qty, buy_avg, sell_avg, margin_held
                FROM user_positions WHERE username = ?
            """, (username,))
            rows = cursor.fetchall()
            return [dict(r) for r in rows]

    def save_position(self, username: str, pos: Dict[str, Any]):
        pos_id = f"{username}:{pos['symbol']}"
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO user_positions (id, username, symbol, side, product, net_qty, buy_avg, sell_avg, margin_held)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    side = excluded.side,
                    product = excluded.product,
                    net_qty = excluded.net_qty,
                    buy_avg = excluded.buy_avg,
                    sell_avg = excluded.sell_avg,
                    margin_held = excluded.margin_held
            """, (
                pos_id,
                username,
                pos["symbol"],
                pos.get("side", "BUY"),
                pos.get("product", "INTRADAY"),
                int(pos["net_qty"]),
                float(pos.get("buy_avg", 0.0)),
                float(pos.get("sell_avg", 0.0)),
                float(pos.get("margin_held", 0.0))
            ))
            conn.commit()

    def delete_position(self, username: str, symbol: str):
        pos_id = f"{username}:{symbol}"
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM user_positions WHERE id = ?", (pos_id,))
            conn.commit()

    def clear_positions(self, username: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM user_positions WHERE username = ?", (username,))
            conn.commit()

    # ------------------ ORDERS PER USER ------------------
    def get_orders(self, username: str) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, order_time as time, symbol, side, order_type as type, product, qty, price, status
                FROM user_orders WHERE username = ? ORDER BY rowid DESC
            """, (username,))
            rows = cursor.fetchall()
            return [dict(r) for r in rows]

    def add_order(self, username: str, order: Dict[str, Any]):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO user_orders (id, username, order_time, symbol, side, order_type, product, qty, price, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                order["id"],
                username,
                order.get("time", datetime.datetime.now().strftime("%H:%M:%S")),
                order["symbol"],
                order["side"],
                order.get("type", "BUY"),
                order.get("product", "INTRADAY"),
                int(order["qty"]),
                float(order["price"]),
                order.get("status", "FILLED")
            ))
            conn.commit()


# Global DB instance
db = DatabaseManager()
