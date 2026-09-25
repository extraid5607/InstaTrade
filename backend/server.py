"""
Insta Trade - Fast, Clean Trading Terminal Backend
Features:
1. Multi-User Virtual Trading (Paper Trading) with Real Live Market Prices (Zero broker needed!)
2. 100% Data Isolation per User (SQLite Persistent Storage in data/trading_terminal.db)
3. Zero API Key Exposure - Pure secure demo trading terminal for multiple users
"""

import os
from pathlib import Path
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from backend.storage import db
from backend.virtual_engine import VirtualTradingEngine

app = FastAPI(title="Insta Trade", description="High-Speed Indian Market Virtual Trading Terminal")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"

virtual_engine = VirtualTradingEngine(initial_capital=1000000.0)


# ------------------ AUTH MODELS ------------------
class LoginPayload(BaseModel):
    username: str
    password: str


class RegisterPayload(BaseModel):
    username: str
    password: str
    display_name: Optional[str] = ""
    client_id: Optional[str] = ""


class UpdateCredentialsPayload(BaseModel):
    password: Optional[str] = ""
    display_name: Optional[str] = ""
    client_id: Optional[str] = ""


class OrderPayload(BaseModel):
    symbol: str
    qty: int
    side: int  # 1 for BUY, -1 for SELL
    order_type: int = 2  # 1=Limit, 2=Market, 3=SL-L, 4=SL-M
    product_type: str = "INTRADAY"  # "CNC", "INTRADAY", "MARGIN"
    limit_price: float = 0.0
    stop_price: float = 0.0


# ------------------ USER AUTH DEPENDENCY ------------------
def get_current_username(
    authorization: Optional[str] = Header(None),
    x_auth_token: Optional[str] = Header(None, alias="X-Auth-Token"),
) -> Optional[str]:
    token = None
    if authorization and authorization.startswith("Bearer "):
        token = authorization.split("Bearer ")[1].strip()
    elif x_auth_token:
        token = x_auth_token.strip()

    if token:
        user = db.get_user_by_token(token)
        if user:
            return user["username"]

    return None



def require_authenticated_user(
    authorization: Optional[str] = Header(None),
    x_auth_token: Optional[str] = Header(None, alias="X-Auth-Token"),
) -> str:
    token = None
    if authorization and authorization.startswith("Bearer "):
        token = authorization.split("Bearer ")[1].strip()
    elif x_auth_token:
        token = x_auth_token.strip()

    if token:
        user = db.get_user_by_token(token)
        if user:
            return user["username"]

    raise HTTPException(status_code=401, detail="Please sign in with your Login ID and Password to trade.")


# ------------------ STATUS & SYSTEM ------------------
@app.get("/api/status")
def get_status():
    # Never expose any broker API keys or secrets
    return {
        "status": "online",
        "trading_mode": "virtual",
        "market": "NSE / BSE Live Feeds",
        "storage": "Local SQLite Database (data/trading_terminal.db)",
    }


# ------------------ USER AUTH ENDPOINTS ------------------
@app.post("/api/auth/register")
def register_account(payload: RegisterPayload):
    return db.register_user(
        username=payload.username,
        password=payload.password,
        display_name=payload.display_name,
        client_id=payload.client_id,
    )


@app.post("/api/auth/login")
def login_account(payload: LoginPayload):
    return db.authenticate_user(username=payload.username, password=payload.password)


@app.get("/api/auth/me")
def get_current_user_profile(username: str = Depends(require_authenticated_user)):
    user = db.get_user(username)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    funds = virtual_engine.get_funds_for_user(username)
    return {
        "s": "ok",
        "user": user,
        "funds": funds,
    }


@app.post("/api/auth/update")
def update_credentials(payload: UpdateCredentialsPayload, username: str = Depends(require_authenticated_user)):
    user = db.update_user_credentials(
        username=username,
        password=payload.password,
        display_name=payload.display_name,
        client_id=payload.client_id,
    )
    return {
        "s": "ok",
        "message": "Account credentials updated successfully!",
        "user": user,
    }


@app.get("/api/profile")
def get_profile(username: Optional[str] = Depends(get_current_username)):
    if not username:
        return {"s": "ok", "authenticated": False, "data": None}
    user = db.get_user(username) or {}
    return {
        "s": "ok",
        "authenticated": True,
        "data": {
            "username": user.get("username", username),
            "display_name": user.get("display_name", username),
            "client_id": user.get("client_id", "XH01499"),
            "mode": "Virtual Demo Paper Trading (Real Live NSE Data)",
        },
    }


@app.post("/api/profile")
def save_profile(payload: UpdateCredentialsPayload, username: str = Depends(require_authenticated_user)):
    user = db.update_user_credentials(
        username=username,
        password=payload.password,
        display_name=payload.display_name,
        client_id=payload.client_id,
    )
    return {
        "s": "ok",
        "message": f"Credentials saved for '{username}'!",
        "data": user,
    }


# ------------------ MARKET DATA ENDPOINTS (STREAMING) ------------------
@app.get("/api/optionchain")
def get_option_chain(
    underlying: str = Query("NIFTY"),
    expiry: Optional[str] = Query(None),
    strikecount: int = Query(15),
):
    return virtual_engine.get_option_chain(underlying, expiry=expiry, strike_count=strikecount)


@app.get("/api/search")
def search_symbols(q: str = Query(..., min_length=1, description="Search query string")):
    results = virtual_engine.search_symbols(q)
    return {"s": "ok", "results": results}


@app.get("/api/quotes")
def get_quotes(symbols: str = Query(..., description="Comma-separated symbols")):
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
    quotes = virtual_engine.get_quotes(sym_list)
    return {"s": "ok", "mode": "virtual", "data": quotes}


@app.get("/api/history")
def get_history(
    symbol: str = Query("NSE:NIFTY50-INDEX"),
    resolution: str = Query("5"),
    days: int = Query(3),
):
    candles = virtual_engine.get_history(symbol=symbol, resolution=resolution)
    return {"s": "ok", "mode": "virtual", "candles": candles}


# ------------------ USER-ISOLATED TRADING ENDPOINTS ------------------
@app.get("/api/funds")
def get_funds(username: Optional[str] = Depends(get_current_username)):
    if not username:
        return {
            "s": "ok",
            "authenticated": False,
            "username": None,
            "data": {
                "available_cash": 1000000.0,
                "used_margin": 0.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
            },
        }
    return {
        "s": "ok",
        "authenticated": True,
        "username": username,
        "data": virtual_engine.get_funds_for_user(username),
    }


@app.post("/api/virtual/reset")
def reset_virtual_funds(capital: float = Query(1000000.0), username: str = Depends(require_authenticated_user)):
    return virtual_engine.reset_account_for_user(username=username, capital=capital)


@app.get("/api/positions")
def get_positions(username: Optional[str] = Depends(get_current_username)):
    if not username:
        return {
            "s": "ok",
            "authenticated": False,
            "username": None,
            "positions": [],
        }
    return {
        "s": "ok",
        "authenticated": True,
        "username": username,
        "positions": virtual_engine.get_positions_for_user(username),
    }


@app.delete("/api/positions")
def exit_positions(symbol: Optional[str] = Query(None), username: str = Depends(require_authenticated_user)):
    return virtual_engine.exit_position_for_user(username=username, symbol=symbol)


@app.get("/api/orders")
def get_orders(username: Optional[str] = Depends(get_current_username)):
    if not username:
        return {
            "s": "ok",
            "authenticated": False,
            "username": None,
            "orders": [],
        }
    return {
        "s": "ok",
        "authenticated": True,
        "username": username,
        "orders": virtual_engine.get_orders_for_user(username),
    }


@app.post("/api/orders")
def place_order(order: OrderPayload, username: str = Depends(require_authenticated_user)):
    return virtual_engine.place_order_for_user(
        username=username,
        symbol=order.symbol,
        qty=order.qty,
        side=order.side,
        order_type=order.order_type,
        product=order.product_type,
        limit_price=order.limit_price,
    )


@app.get("/api/holdings")
def get_holdings(username: Optional[str] = Depends(get_current_username)):
    if not username:
        return {
            "s": "ok",
            "authenticated": False,
            "username": None,
            "holdings": [],
        }
    return {
        "s": "ok",
        "authenticated": True,
        "username": username,
        "holdings": virtual_engine.get_holdings_for_user(username),
    }


# ------------------ FRONTEND MOUNT ------------------
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

    @app.get("/")
    def serve_index():
        index_file = FRONTEND_DIR / "index.html"
        if index_file.exists():
            return FileResponse(str(index_file))
        return {"message": "Insta Trade API is running. frontend/index.html not found."}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    uvicorn.run("backend.server:app", host=host, port=port)

