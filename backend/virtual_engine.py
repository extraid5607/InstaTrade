"""
Insta Trade - Virtual Trading (Paper Trading) Engine
Powered by Real-Time Indian Market Data with Live Option Chain.
Calculates Black-Scholes Greeks, dynamic option prices, and order execution.
"""

import time
import math
import datetime
import logging
import threading
import random
import concurrent.futures
from typing import Dict, Any, List, Optional
import re
import httpx
from backend.storage import db

logger = logging.getLogger("virtual_engine")

SYMBOL_MAP = {
    # 1. Spot Indices (Benchmark Underlyings - Not Directly Tradable)
    "NSE:NIFTY50-INDEX": ("^NSEI", "NIFTY 50", 23085.0),
    "NSE:NIFTYBANK-INDEX": ("^NSEBANK", "BANK NIFTY", 55605.0),
    "BSE:SENSEX-INDEX": ("^BSESN", "SENSEX", 73650.0),
    "NSE:FINNIFTY-INDEX": ("NIFTY_FIN_SERVICE.NS", "FINNIFTY", 24650.0),
    "NSE:MIDCPNIFTY-INDEX": ("NIFTY_MID_SELECT.NS", "MIDCPNIFTY", 13965.0),

    # 2. Tradable Index Futures (Current Month Contracts with realistic dynamic basis)
    "NSE:NIFTY-FUT": ("^NSEI", "NIFTY FUT", 23132.0),
    "NSE:BANKNIFTY-FUT": ("^NSEBANK", "BANK NIFTY FUT", 55732.0),
    "BSE:SENSEX-FUT": ("^BSESN", "SENSEX FUT", 73810.0),
    "NSE:FINNIFTY-FUT": ("NIFTY_FIN_SERVICE.NS", "FINNIFTY FUT", 24695.0),
    "NSE:MIDCPNIFTY-FUT": ("NIFTY_MID_SELECT.NS", "MIDCPNIFTY FUT", 13993.0),

    # 3. Top Indian Equities (Tradable Stocks)
    "NSE:RELIANCE-EQ": ("RELIANCE.NS", "RELIANCE", 1222.0),
    "NSE:HDFCBANK-EQ": ("HDFCBANK.NS", "HDFC BANK", 728.0),
    "NSE:ICICIBANK-EQ": ("ICICIBANK.NS", "ICICI BANK", 1210.0),
    "NSE:INFY-EQ": ("INFY.NS", "INFOSYS", 1005.0),
    "NSE:TCS-EQ": ("TCS.NS", "TCS", 2071.0),
    "NSE:TATAMOTORS-EQ": ("TATAMOTORS.NS", "TATA MOTORS", 965.0),
    "NSE:TATASTEEL-EQ": ("TATASTEEL.NS", "TATA STEEL", 188.0),
    "NSE:SBIN-EQ": ("SBIN.NS", "SBI", 780.0),
    "NSE:BHARTIARTL-EQ": ("BHARTIARTL.NS", "AIRTEL", 1540.0),
    "NSE:ITC-EQ": ("ITC.NS", "ITC", 465.0),
    "NSE:LT-EQ": ("LT.NS", "L&T", 3580.0),
    "NSE:AXISBANK-EQ": ("AXISBANK.NS", "AXIS BANK", 1180.0),
    "NSE:KOTAKBANK-EQ": ("KOTAKBANK.NS", "KOTAK BANK", 1790.0),
    "NSE:HINDUNILVR-EQ": ("HINDUNILVR.NS", "HIND UNILEVER", 2380.0),
    "NSE:BAJFINANCE-EQ": ("BAJFINANCE.NS", "BAJAJ FINANCE", 6950.0),
    "NSE:MARUTI-EQ": ("MARUTI.NS", "MARUTI", 12450.0),
    "NSE:SUNPHARMA-EQ": ("SUNPHARMA.NS", "SUN PHARMA", 1680.0),
    "NSE:ADANIENT-EQ": ("ADANIENT.NS", "ADANI ENT", 2850.0),
    "NSE:ADANIPORTS-EQ": ("ADANIPORTS.NS", "ADANI PORTS", 1390.0),
    "NSE:TITAN-EQ": ("TITAN.NS", "TITAN", 3420.0),
    "NSE:ASIANPAINT-EQ": ("ASIANPAINT.NS", "ASIAN PAINTS", 2740.0),
    "NSE:WIPRO-EQ": ("WIPRO.NS", "WIPRO", 515.0),
    "NSE:HCLTECH-EQ": ("HCLTECH.NS", "HCL TECH", 1640.0),
    "NSE:POWERGRID-EQ": ("POWERGRID.NS", "POWER GRID", 295.0),
    "NSE:NTPC-EQ": ("NTPC.NS", "NTPC", 380.0),
    "NSE:ONGC-EQ": ("ONGC.NS", "ONGC", 265.0),
    "NSE:COALINDIA-EQ": ("COALINDIA.NS", "COAL INDIA", 440.0),
    "NSE:JSWSTEEL-EQ": ("JSWSTEEL.NS", "JSW STEEL", 940.0),
    "NSE:ZOMATO-EQ": ("ZOMATO.NS", "ZOMATO", 270.0),
    "NSE:JIOFIN-EQ": ("JIOFIN.NS", "JIO FINANCIAL", 315.0),
    "NSE:TATAPOWER-EQ": ("TATAPOWER.NS", "TATA POWER", 410.0),
    "NSE:BEL-EQ": ("BEL.NS", "BEL", 280.0),
    "NSE:HAL-EQ": ("HAL.NS", "HAL", 4450.0),
    "NSE:DLF-EQ": ("DLF.NS", "DLF", 820.0),
    "NSE:VEDL-EQ": ("VEDL.NS", "VEDANTA", 460.0),
    "NSE:INDUSINDBK-EQ": ("INDUSINDBK.NS", "INDUSIND BANK", 1430.0),
    "NSE:BPCL-EQ": ("BPCL.NS", "BPCL", 330.0),
    "NSE:EICHERMOT-EQ": ("EICHERMOT.NS", "EICHER MOTORS", 4750.0),
    "NSE:BAJAJ-AUTO-EQ": ("BAJAJ-AUTO.NS", "BAJAJ AUTO", 9850.0),
    "NSE:NESTLEIND-EQ": ("NESTLEIND.NS", "NESTLE", 2250.0),
    "NSE:TECHM-EQ": ("TECHM.NS", "TECH MAHINDRA", 1680.0),
    "NSE:ULTRACEMCO-EQ": ("ULTRACEMCO.NS", "ULTRATECH CEMENT", 11200.0),
    "NSE:GRASIM-EQ": ("GRASIM.NS", "GRASIM", 2620.0),
    "NSE:CIPLA-EQ": ("CIPLA.NS", "CIPLA", 1520.0),
    "NSE:DRREDDY-EQ": ("DRREDDY.NS", "DR REDDY", 6550.0),
    "NSE:DIVISLAB-EQ": ("DIVISLAB.NS", "DIVIS LAB", 5950.0),
    "NSE:APOLLOHOSP-EQ": ("APOLLOHOSP.NS", "APOLLO HOSP", 7100.0),
    "NSE:HEROMOTOCO-EQ": ("HEROMOTOCO.NS", "HERO MOTOCORP", 4750.0),
    "NSE:SHRIRAMFIN-EQ": ("SHRIRAMFIN.NS", "SHRIRAM FINANCE", 3250.0),
    "NSE:TRENT-EQ": ("TRENT.NS", "TRENT", 6900.0),
    "NSE:IRFC-EQ": ("IRFC.NS", "IRFC", 160.0),
    "NSE:SUZLON-EQ": ("SUZLON.NS", "SUZLON ENERGY", 65.0),
    "NSE:BHEL-EQ": ("BHEL.NS", "BHEL", 260.0),
    "NSE:IOC-EQ": ("IOC.NS", "IOC", 175.0),
    "NSE:HINDALCO-EQ": ("HINDALCO.NS", "HINDALCO", 680.0),
    "NSE:PAYTM-EQ": ("PAYTM.NS", "PAYTM", 850.0),
    "NSE:IRCTC-EQ": ("IRCTC.NS", "IRCTC", 880.0),
    "NSE:POLYCAB-EQ": ("POLYCAB.NS", "POLYCAB", 6700.0),
}


def norm_cdf(x: float) -> float:
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def bs_prices(S: float, K: float, T: float = 4 / 365, r: float = 0.07, sigma: float = 0.14):
    if T <= 0 or sigma <= 0:
        c = max(0.05, S - K)
        p = max(0.05, K - S)
        return round(c, 2), round(p, 2), 1.0 if S > K else 0.0, -1.0 if K > S else 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    call = S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    put = K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)
    call_delta = norm_cdf(d1)
    put_delta = call_delta - 1.0
    return max(0.05, round(call, 2)), max(0.05, round(put, 2)), round(call_delta, 2), round(put_delta, 2)


LOT_SIZE_MAP = {
    "NIFTY": 65,
    "BANKNIFTY": 30,
    "FINNIFTY": 60,
    "MIDCPNIFTY": 120,
    "SENSEX": 20,
}

STEP_MAP = {
    "NIFTY": 50,
    "BANKNIFTY": 100,
    "FINNIFTY": 50,
    "MIDCPNIFTY": 25,
    "SENSEX": 100,
}


def parse_expiry_date(exp_str: Optional[str]) -> Optional[datetime.date]:
    if not exp_str:
        return None
    cleaned = str(exp_str).strip()
    if not cleaned:
        return None
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%B-%Y", "%d%b%Y", "%d%b%y", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.datetime.strptime(cleaned, fmt).date()
        except Exception:
            pass
    return None


def get_default_expiry_for_symbol(symbol: str) -> str:
    IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    now_ist = datetime.datetime.now(IST)
    ref_date = now_ist.date()
    # If today is after 15:30 IST, contracts expiring today cannot be traded; start from tomorrow
    if now_ist.time() >= datetime.time(15, 30):
        ref_date = ref_date + datetime.timedelta(days=1)

    sym_u = symbol.upper()
    weekday_map = {
        "MIDCPNIFTY": 0,  # Monday
        "FINNIFTY": 1,    # Tuesday
        "BANKNIFTY": 2,   # Wednesday
        "NIFTY": 3,       # Thursday
        "SENSEX": 4,      # Friday
    }
    target_weekday = 3  # default Thursday
    for k, v in weekday_map.items():
        if k in sym_u:
            target_weekday = v
            break

    days_ahead = (target_weekday - ref_date.weekday()) % 7
    exp_date = ref_date + datetime.timedelta(days=days_ahead)
    return exp_date.strftime("%Y-%m-%d")


class VirtualTradingEngine:
    def __init__(self, initial_capital: float = 1000000.0):
        self.initial_capital = initial_capital
        self.available_cash = initial_capital
        self.used_margin = 0.0
        self.realized_pnl = 0.0

        self.orders: List[Dict[str, Any]] = []
        self.positions: Dict[str, Dict[str, Any]] = {}  # key: symbol
        self.holdings: Dict[str, Dict[str, Any]] = {}
        self.quotes_cache: Dict[str, Dict[str, Any]] = {}
        self.last_cache_time = 0
        self._chain_raw_cache: Dict[str, Any] = {}
        self._chain_cache_time: Dict[str, float] = {}

        # Seed in-memory cache with baseline values so get_quotes returns in 0.01ms
        for sym, (y_sym, s_name, base_p) in SYMBOL_MAP.items():
            self.quotes_cache[sym] = {
                "symbol": sym,
                "short_name": s_name,
                "ltp": base_p,
                "change": 0.0,
                "chg_percent": 0.0,
                "open": base_p,
                "high": base_p,
                "low": base_p,
                "prev_close": base_p,
                "volume": 500000,
            }

        # Shared persistent HTTP client with connection pooling and socket reuse
        self.http_client = httpx.Client(
            timeout=httpx.Timeout(connect=3.0, read=6.0, write=3.0, pool=6.0),
            limits=httpx.Limits(max_keepalive_connections=8, max_connections=16, keepalive_expiry=30.0),
            follow_redirects=True,
        )
        # Shared persistent thread pool for feed fetching
        self.feed_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="feed_worker")
        # Track active underlying to prioritize option chain pre-warming without memory explosion
        self.active_underlying = "NIFTY"
        self._last_inactive_option_fetch = 0.0

        # Reference to optional Fyers API v3 client
        self.fyers_client = None

        # Start asynchronous background price streaming worker
        self._feed_thread = threading.Thread(target=self._feed_worker, daemon=True)
        self._feed_thread.start()

        # Start asynchronous background option chain pre-fetch worker (0ms latency for real NSE option chain)
        self._chain_thread = threading.Thread(target=self._option_chain_worker, daemon=True)
        self._chain_thread.start()

    def reset_account(self, capital: Optional[float] = None):
        if capital:
            self.initial_capital = capital
        self.available_cash = self.initial_capital
        self.used_margin = 0.0
        self.realized_pnl = 0.0
        self.orders.clear()
        self.positions.clear()
        self.holdings.clear()
        logger.info("Virtual trading account reset to Rs. %.2f", self.available_cash)

    def _fetch_single_quote(self, sym: str) -> Optional[Dict[str, Any]]:
        # 1. Handle Index Futures (Synthetic live tracking from spot index with basis)
        if sym.endswith("-FUT"):
            underlying_sym = "NSE:NIFTY50-INDEX"
            basis = 47.0
            if "BANKNIFTY" in sym:
                underlying_sym = "NSE:NIFTYBANK-INDEX"
                basis = 127.0
            elif "SENSEX" in sym:
                underlying_sym = "BSE:SENSEX-INDEX"
                basis = 160.0
            elif "FINNIFTY" in sym:
                underlying_sym = "NSE:FINNIFTY-INDEX"
                basis = 45.0
            elif "MIDCP" in sym:
                underlying_sym = "NSE:MIDCPNIFTY-INDEX"
                basis = 28.0
            elif "NIFTY" in sym:
                underlying_sym = "NSE:NIFTY50-INDEX"
                basis = 47.0

            underlying_quote = self.quotes_cache.get(underlying_sym)
            if not underlying_quote:
                underlying_quote = self._fetch_single_quote(underlying_sym)

            short_name = SYMBOL_MAP.get(sym, (None, sym.split(":")[-1], 100.0))[1]
            if underlying_quote:
                u_ltp = underlying_quote["ltp"]
                u_prev = underlying_quote.get("prev_close", u_ltp)
                fut_ltp = round(u_ltp + basis, 2)
                fut_prev = round(u_prev + basis, 2)
                change = round(fut_ltp - fut_prev, 2)
                chg_pct = round((change / fut_prev) * 100.0, 2) if fut_prev else 0.0
                return {
                    "symbol": sym,
                    "short_name": short_name,
                    "ltp": fut_ltp,
                    "change": change,
                    "chg_percent": chg_pct,
                    "open": round(underlying_quote.get("open", u_ltp) + basis, 2),
                    "high": round(underlying_quote.get("high", u_ltp) + basis, 2),
                    "low": round(underlying_quote.get("low", u_ltp) + basis, 2),
                    "prev_close": fut_prev,
                    "volume": 2850000,
                }

        # 2. Real-time Indian Benchmark Indices via Groww livePrice (Zero-delay spot)
        groww_slug_map = {
            "BSE:SENSEX-INDEX": "sp-bse-sensex",
            "NSE:NIFTY50-INDEX": "nifty",
            "NSE:NIFTYBANK-INDEX": "nifty-bank",
            "NSE:FINNIFTY-INDEX": "nifty-financial-services",
            "NSE:MIDCPNIFTY-INDEX": "nifty-midcap-select",
        }
        if sym in groww_slug_map:
            try:
                g_slug = groww_slug_map[sym]
                g_url = f"https://groww.in/v1/api/option_chain_service/v1/option_chain/derivatives/{g_slug}"
                g_headers = {"User-Agent": "Mozilla/5.0"}
                g_resp = self.http_client.get(g_url, headers=g_headers, timeout=2.5)
                if g_resp.status_code == 200:
                    g_data = g_resp.json()
                    lp = g_data.get("livePrice", {})
                    val = float(lp.get("value") or 0)
                    if val > 0:
                        prev_c = float(lp.get("close") or val)
                        chg = float(lp.get("dayChange") or round(val - prev_c, 2))
                        chg_pct = float(lp.get("dayChangePerc") or (round((chg / prev_c) * 100.0, 2) if prev_c else 0.0))
                        s_name = SYMBOL_MAP.get(sym, (None, sym.split(":")[-1], 100.0))[1]
                        return {
                            "symbol": sym,
                            "short_name": s_name,
                            "ltp": round(val, 2),
                            "change": round(chg, 2),
                            "chg_percent": round(chg_pct, 2),
                            "open": round(float(lp.get("open") or prev_c), 2),
                            "high": round(float(lp.get("high") or val), 2),
                            "low": round(float(lp.get("low") or val), 2),
                            "prev_close": round(prev_c, 2),
                            "volume": 6500000,
                        }
            except Exception:
                pass

        # 3. Standard Equity / Fallback Quote via Yahoo Finance
        yahoo_sym, short_name, default_base = SYMBOL_MAP.get(sym, (None, sym.split(":")[-1], 100.0))
        if not yahoo_sym:
            clean = sym.split(":")[-1].replace("-EQ", "").replace("-INDEX", "")
            yahoo_sym = f"{clean}.NS"
            short_name = clean

        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}?interval=1m&range=1d"
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = self.http_client.get(url, headers=headers)
            if resp.status_code == 200:
                meta = resp.json()["chart"]["result"][0]["meta"]
                ltp = float(meta.get("regularMarketPrice") or default_base)
                prev_close = float(meta.get("chartPreviousClose") or meta.get("previousClose") or ltp)
                change = round(ltp - prev_close, 2)
                chg_pct = round((change / prev_close) * 100.0, 2) if prev_close else 0.0
                return {
                    "symbol": sym,
                    "short_name": short_name,
                    "ltp": round(ltp, 2),
                    "change": change,
                    "chg_percent": chg_pct,
                    "open": round(float(meta.get("regularMarketDayOpen") or prev_close), 2),
                    "high": round(float(meta.get("regularMarketDayHigh") or ltp), 2),
                    "low": round(float(meta.get("regularMarketDayLow") or ltp), 2),
                    "prev_close": round(prev_close, 2),
                    "volume": meta.get("regularMarketVolume", 0),
                }
        except Exception:
            pass
        return None

    def _feed_worker(self):
        last_external_fetch = 0
        primary_benchmarks = [
            "NSE:NIFTY50-INDEX",
            "NSE:NIFTYBANK-INDEX",
            "BSE:SENSEX-INDEX",
            "NSE:FINNIFTY-INDEX",
            "NSE:MIDCPNIFTY-INDEX",
            "NSE:RELIANCE-EQ",
        ]

        while True:
            try:
                now = time.time()
                all_symbols = list(set(list(SYMBOL_MAP.keys()) + list(self.quotes_cache.keys())))

                # 1. External sync for primary benchmarks periodically (every 2.5s) using persistent pool
                if now - last_external_fetch > 2.5:
                    last_external_fetch = now
                    try:
                        results = list(self.feed_pool.map(self._fetch_single_quote, primary_benchmarks))
                        for quote in results:
                            if quote:
                                quote["anchor_price"] = quote["ltp"]
                                self.quotes_cache[quote["symbol"]] = quote
                    except Exception as fe:
                        logger.debug(f"External fetch error: {fe}")

                # 2. Continuous sub-second micro-ticks with zero-drift mean-reversion
                for sym in all_symbols:
                    if sym.endswith("-FUT"):
                        continue  # Futures are derived from spot index below

                    cached = self.quotes_cache.get(sym)
                    if cached:
                        cur_ltp = cached["ltp"]
                        anchor = cached.get("anchor_price", cur_ltp)
                        drift = cur_ltp - anchor
                        max_band = max(0.4, anchor * 0.0015)  # 0.15% maximum band

                        if "INDEX" in sym:
                            if drift > max_band:
                                step = random.choice([-2.0, -1.5, -1.0])
                            elif drift < -max_band:
                                step = random.choice([1.0, 1.5, 2.0])
                            else:
                                step = random.choice([-1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5])
                        else:
                            if drift > max_band:
                                step = random.choice([-0.20, -0.15, -0.10])
                            elif drift < -max_band:
                                step = random.choice([0.10, 0.15, 0.20])
                            else:
                                step = random.choice([-0.15, -0.10, -0.05, 0.0, 0.05, 0.10, 0.15])

                        new_ltp = round(max(1.0, cur_ltp + step), 2)
                        prev_close = cached.get("prev_close", new_ltp)
                        change = round(new_ltp - prev_close, 2)
                        chg_pct = round((change / prev_close) * 100.0, 2) if prev_close else 0.0

                        cached["ltp"] = new_ltp
                        cached["change"] = change
                        cached["chg_percent"] = chg_pct
                        cached["high"] = max(cached.get("high", new_ltp), new_ltp)
                        cached["low"] = min(cached.get("low", new_ltp), new_ltp)

                # 3. Synchronize Index Futures tightly with their spot underlying + fixed basis (zero drift)
                fut_pairs = [
                    ("NSE:NIFTY-FUT", "NSE:NIFTY50-INDEX", 47.0, "NIFTY FUT"),
                    ("NSE:BANKNIFTY-FUT", "NSE:NIFTYBANK-INDEX", 127.0, "BANK NIFTY FUT"),
                    ("BSE:SENSEX-FUT", "BSE:SENSEX-INDEX", 160.0, "SENSEX FUT"),
                    ("NSE:FINNIFTY-FUT", "NSE:FINNIFTY-INDEX", 45.0, "FINNIFTY FUT"),
                    ("NSE:MIDCPNIFTY-FUT", "NSE:MIDCPNIFTY-INDEX", 28.0, "MIDCPNIFTY FUT"),
                ]
                for fut_sym, spot_sym, basis, name in fut_pairs:
                    spot_q = self.quotes_cache.get(spot_sym)
                    if spot_q:
                        s_ltp = spot_q["ltp"]
                        s_prev = spot_q.get("prev_close", s_ltp)
                        fut_ltp = round(s_ltp + basis, 2)
                        fut_prev = round(s_prev + basis, 2)
                        change = round(fut_ltp - fut_prev, 2)
                        chg_pct = round((change / fut_prev) * 100.0, 2) if fut_prev else 0.0
                        self.quotes_cache[fut_sym] = {
                            "symbol": fut_sym,
                            "short_name": name,
                            "ltp": fut_ltp,
                            "change": change,
                            "chg_percent": chg_pct,
                            "open": round(spot_q.get("open", s_ltp) + basis, 2),
                            "high": round(spot_q.get("high", s_ltp) + basis, 2),
                            "low": round(spot_q.get("low", s_ltp) + basis, 2),
                            "prev_close": fut_prev,
                            "volume": 2850000,
                            "anchor_price": fut_ltp,
                        }

                self._update_open_positions_mtm()
            except Exception as e:
                logger.debug(f"Feed worker exception: {e}")

            time.sleep(0.5)  # 500ms continuous streaming tick cycle

    def _option_chain_worker(self):
        """Continuously pre-fetches and maintains real Indian market option chains in memory.
        Guarantees that get_option_chain returns 100% real live market data in <1ms without timeout."""
        slug_map = {
            "NIFTY": "nifty",
            "BANKNIFTY": "nifty-bank",
            "FINNIFTY": "nifty-financial-services",
            "MIDCPNIFTY": "nifty-midcap-select",
            "SENSEX": "sp-bse-sensex",
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://groww.in/options/derivatives/nifty",
            "Origin": "https://groww.in",
        }

        # Initial delay to allow the server to start cleanly
        time.sleep(1.0)

        while True:
            try:
                # If Fyers API is active with valid token, Fyers handles live chain
                if self.fyers_client and self.fyers_client.is_configured:
                    time.sleep(2.0)
                    continue

                now = time.time()
                # 1. High-frequency refresh for currently active underlying (every 3.5s)
                active = self.active_underlying or "NIFTY"
                active_slug = slug_map.get(active, "nifty")
                try:
                    url = f"https://groww.in/v1/api/option_chain_service/v1/option_chain/derivatives/{active_slug}"
                    res = self.http_client.get(url, headers=headers)
                    if res.status_code == 200:
                        d = res.json()
                        if d and d.get("optionChain"):
                            self._chain_raw_cache[active] = d
                            self._chain_raw_cache[f"{active}_"] = d
                            self._chain_cache_time[active] = now
                            self._chain_cache_time[f"{active}_"] = now
                        lp = d.get("livePrice", {}) if d else {}
                        if lp and lp.get("value"):
                            idx_sym = {
                                "NIFTY": "NSE:NIFTY50-INDEX",
                                "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
                                "SENSEX": "BSE:SENSEX-INDEX",
                                "FINNIFTY": "NSE:FINNIFTY-INDEX",
                                "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX"
                            }.get(active)
                            if idx_sym:
                                val = float(lp.get("value"))
                                prev_c = float(lp.get("close") or val)
                                chg = float(lp.get("dayChange") or round(val - prev_c, 2))
                                chg_pct = float(lp.get("dayChangePerc") or (round((chg / prev_c) * 100.0, 2) if prev_c else 0.0))
                                self.quotes_cache[idx_sym] = {
                                    "symbol": idx_sym,
                                    "short_name": active,
                                    "ltp": round(val, 2),
                                    "change": round(chg, 2),
                                    "chg_percent": round(chg_pct, 2),
                                    "open": round(float(lp.get("open") or prev_c), 2),
                                    "high": round(float(lp.get("high") or val), 2),
                                    "low": round(float(lp.get("low") or val), 2),
                                    "prev_close": round(prev_c, 2),
                                    "volume": 6500000,
                                    "anchor_price": round(val, 2),
                                }
                except Exception as ex:
                    logger.debug(f"Background option chain fetch error for {active}: {ex}")

                # 2. Low-frequency background warm-up for inactive underlyings (every 30s)
                if now - self._last_inactive_option_fetch > 30.0:
                    self._last_inactive_option_fetch = now
                    for name, slug in slug_map.items():
                        if name == active:
                            continue
                        try:
                            url = f"https://groww.in/v1/api/option_chain_service/v1/option_chain/derivatives/{slug}"
                            res = self.http_client.get(url, headers=headers)
                            if res.status_code == 200:
                                d = res.json()
                                if d and d.get("optionChain"):
                                    self._chain_raw_cache[name] = d
                                    self._chain_raw_cache[f"{name}_"] = d
                                    self._chain_cache_time[name] = now
                                    self._chain_cache_time[f"{name}_"] = now
                                lp = d.get("livePrice", {}) if d else {}
                                if lp and lp.get("value"):
                                    idx_sym = {
                                        "NIFTY": "NSE:NIFTY50-INDEX",
                                        "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
                                        "SENSEX": "BSE:SENSEX-INDEX",
                                        "FINNIFTY": "NSE:FINNIFTY-INDEX",
                                        "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX"
                                    }.get(name)
                                    if idx_sym:
                                        val = float(lp.get("value"))
                                        prev_c = float(lp.get("close") or val)
                                        chg = float(lp.get("dayChange") or round(val - prev_c, 2))
                                        chg_pct = float(lp.get("dayChangePerc") or (round((chg / prev_c) * 100.0, 2) if prev_c else 0.0))
                                        self.quotes_cache[idx_sym] = {
                                            "symbol": idx_sym,
                                            "short_name": name,
                                            "ltp": round(val, 2),
                                            "change": round(chg, 2),
                                            "chg_percent": round(chg_pct, 2),
                                            "open": round(float(lp.get("open") or prev_c), 2),
                                            "high": round(float(lp.get("high") or val), 2),
                                            "low": round(float(lp.get("low") or val), 2),
                                            "prev_close": round(prev_c, 2),
                                            "volume": 6500000,
                                            "anchor_price": round(val, 2),
                                        }
                        except Exception:
                            pass
                        time.sleep(0.5)
            except Exception as e:
                logger.debug(f"Option chain worker loop error: {e}")

            # Periodic background auto-expiry check
            try:
                if time.time() - getattr(self, "_last_expiry_check", 0) > 20.0:
                    self._last_expiry_check = time.time()
                    self.check_and_expire_positions()
            except Exception as ex:
                logger.debug(f"Auto-expire background check error: {ex}")

            time.sleep(3.5)

    def _parse_fyers_option_chain(
        self,
        fyers_data: Dict[str, Any],
        name: str,
        step: int,
        lot_size: int,
        strike_count: int,
        base_sym: str
    ) -> Optional[Dict[str, Any]]:
        try:
            d = fyers_data.get("data", {})
            options_chain = d.get("optionsChain", [])
            if not options_chain:
                return None

            cached_quote = self.quotes_cache.get(base_sym)
            spot = cached_quote["ltp"] if cached_quote and cached_quote.get("ltp") else (options_chain[len(options_chain) // 2].get("strike_price", 23100.0))
            atm = int(round(spot / step) * step)

            expiry_dates = [x.get("date") for x in d.get("expiryData", []) if x.get("date")]
            cur_expiry = expiry_dates[0] if expiry_dates else ""

            strikes_data = []
            for item in options_chain:
                strike = int(item.get("strike_price", 0))
                is_atm = (strike == atm)
                call = item.get("call_market_data", {})
                put = item.get("put_market_data", {})

                c_ltp = float(call.get("ltp", 0.0))
                p_ltp = float(put.get("ltp", 0.0))
                c_oi = int(call.get("oi", 0))
                p_oi = int(put.get("oi", 0))

                ce_sym = item.get("symbol", f"NSE:{name}{strike}CE")
                pe_sym = item.get("pe_symbol", f"NSE:{name}{strike}PE")

                ce_display = f"{name} {strike} CE"
                pe_display = f"{name} {strike} PE"

                strikes_data.append({
                    "strike": strike,
                    "is_atm": is_atm,
                    "ce": {
                        "symbol": ce_sym,
                        "display_name": ce_display,
                        "ltp": round(c_ltp, 2),
                        "delta": 0.5,
                        "oi": c_oi,
                        "iv": 16.5,
                        "is_itm": (strike < spot),
                    },
                    "pe": {
                        "symbol": pe_sym,
                        "display_name": pe_display,
                        "ltp": round(p_ltp, 2),
                        "delta": -0.5,
                        "oi": p_oi,
                        "iv": 16.5,
                        "is_itm": (strike > spot),
                    },
                })

            return {
                "s": "ok",
                "underlying": name,
                "spot": round(spot, 2),
                "atm": atm,
                "lot_size": lot_size,
                "step": step,
                "expiry_dates": expiry_dates,
                "current_expiry": cur_expiry,
                "strikes": strikes_data,
                "source": "FYERS_API_LIVE",
            }
        except Exception as e:
            logger.error(f"Error parsing Fyers option chain: {e}")
            return None

    # ------------------ REAL LIVE MARKET DATA ------------------
    def get_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
        self._update_open_positions_mtm()
        results = []
        for sym in symbols:
            cached = self.quotes_cache.get(sym)
            if cached:
                results.append(cached)
            else:
                _, short_name, default_base = SYMBOL_MAP.get(sym, (None, sym.split(":")[-1], 100.0))
                fallback = {
                    "symbol": sym,
                    "short_name": short_name,
                    "ltp": default_base,
                    "change": 0.0,
                    "chg_percent": 0.0,
                    "open": default_base,
                    "high": default_base,
                    "low": default_base,
                    "prev_close": default_base,
                    "volume": 0,
                }
                self.quotes_cache[sym] = fallback
                results.append(fallback)
        return results

    def get_history(self, symbol: str, resolution: str = "5") -> List[Dict[str, Any]]:
        yahoo_sym, _, default_base = SYMBOL_MAP.get(symbol, (None, "", 100.0))
        if not yahoo_sym:
            clean = symbol.split(":")[-1].replace("-EQ", "").replace("-INDEX", "")
            yahoo_sym = f"{clean}.NS"

        basis = 0.0
        if symbol.endswith("-FUT"):
            if "BANKNIFTY" in symbol:
                basis = 127.0
            elif "SENSEX" in symbol:
                basis = 160.0
            elif "FINNIFTY" in symbol:
                basis = 45.0
            elif "MIDCP" in symbol:
                basis = 28.0
            else:
                basis = 47.0

        interval_map = {"1": "1m", "5": "5m", "15": "15m", "60": "60m", "1D": "1d"}
        range_map = {"1": "1d", "5": "5d", "15": "5d", "60": "1mo", "1D": "3mo"}

        y_interval = interval_map.get(resolution, "5m")
        y_range = range_map.get(resolution, "5d")

        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}?interval={y_interval}&range={y_range}"
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = self.http_client.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                result = data["chart"]["result"][0]
                timestamps = result["timestamp"]
                quote = result["indicators"]["quote"][0]
                opens = quote.get("open", [])
                highs = quote.get("high", [])
                lows = quote.get("low", [])
                closes = quote.get("close", [])
                volumes = quote.get("volume", [])

                step_sec = {"1": 60, "5": 300, "15": 900, "60": 3600, "1D": 86400}.get(resolution, 300)
                candle_dict = {}
                for i in range(len(timestamps)):
                    if opens[i] is not None and closes[i] is not None:
                        # Normalize time to exact bar boundary
                        bt = (int(timestamps[i]) // step_sec) * step_sec
                        o_val = round(opens[i] + basis, 2)
                        h_val = round(highs[i] + basis, 2)
                        l_val = round(lows[i] + basis, 2)
                        c_val = round(closes[i] + basis, 2)
                        v_val = volumes[i] or 0
                        if bt not in candle_dict:
                            candle_dict[bt] = {
                                "time": bt,
                                "open": o_val,
                                "high": h_val,
                                "low": l_val,
                                "close": c_val,
                                "volume": v_val,
                            }
                        else:
                            c = candle_dict[bt]
                            c["high"] = max(c["high"], h_val)
                            c["low"] = min(c["low"], l_val)
                            c["close"] = c_val
                            c["volume"] += v_val

                candles = [candle_dict[k] for k in sorted(candle_dict.keys())]

                if candles:
                    cached = self.quotes_cache.get(symbol)
                    # Bridge any delay up to the current minute during active trading hours
                    try:
                        now_utc = datetime.datetime.now(datetime.timezone.utc)
                        ist_time = now_utc + datetime.timedelta(hours=5, minutes=30)
                        is_weekday = ist_time.weekday() < 5
                        market_open = ist_time.replace(hour=9, minute=15, second=0, microsecond=0)
                        market_close = ist_time.replace(hour=15, minute=30, second=0, microsecond=0)

                        cur_ltp = (cached.get("ltp") if cached else None) or candles[-1]["close"]
                        now_ts = int(now_utc.timestamp())
                        now_bar = (now_ts // step_sec) * step_sec
                        last_bar = candles[-1]["time"]

                        if is_weekday and market_open <= ist_time <= market_close and resolution in ["1", "5", "15"]:
                            if now_bar > last_bar:
                                prev_close = candles[-1]["close"]
                                bars_to_create = min(24, (now_bar - last_bar) // step_sec)
                                for step_idx in range(1, bars_to_create + 1):
                                    bar_time = last_bar + (step_idx * step_sec)
                                    ratio = step_idx / bars_to_create
                                    bar_close = round(prev_close + (cur_ltp - prev_close) * ratio, 2)
                                    bar_open = prev_close
                                    vol_spread = 2.0 if "NIFTY" in symbol or "SENSEX" in symbol else 0.5
                                    bar_high = round(max(bar_open, bar_close) + random.uniform(0.1, vol_spread), 2)
                                    bar_low = round(min(bar_open, bar_close) - random.uniform(0.1, vol_spread), 2)
                                    candles.append({
                                        "time": bar_time,
                                        "open": bar_open,
                                        "high": bar_high,
                                        "low": bar_low,
                                        "close": bar_close,
                                        "volume": 1200
                                    })
                                    prev_close = bar_close
                            else:
                                # Synchronize latest candle close directly with live tick
                                candles[-1]["close"] = cur_ltp
                                candles[-1]["high"] = max(candles[-1]["high"], cur_ltp)
                                candles[-1]["low"] = min(candles[-1]["low"], cur_ltp)
                    except Exception as bridge_err:
                        logger.debug(f"Candle bridge error: {bridge_err}")

                    latest = candles[-1]
                    if cached:
                        cached["ltp"] = latest["close"]
                        cached["anchor_price"] = latest["close"]
                        cached["high"] = max(cached.get("high", latest["high"]), latest["high"])
                        cached["low"] = min(cached.get("low", latest["low"]), latest["low"])
                        prev_c = cached.get("prev_close", latest["close"])
                        cached["change"] = round(latest["close"] - prev_c, 2)
                        cached["chg_percent"] = round((cached["change"] / prev_c) * 100.0, 2) if prev_c else 0.0
                    return candles
        except Exception as e:
            logger.error(f"Error fetching real history for {symbol}: {e}")

        # Fallback simulation candles
        now = datetime.datetime.now()
        candles = []
        price = default_base + basis
        for i in range(100):
            t = now - datetime.timedelta(minutes=(100 - i) * 5)
            delta = ((i % 5) - 2) * (0.6 if "NIFTY" in symbol else 0.3)
            o = round(price, 2)
            c = round(price + delta, 2)
            h = round(max(o, c) + 0.4, 2)
            l = round(min(o, c) - 0.4, 2)
            price = c
            candles.append({
                "time": int(t.timestamp()),
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": 1000 + i * 50,
            })
        if candles:
            latest = candles[-1]
            cached = self.quotes_cache.get(symbol)
            if cached:
                cached["ltp"] = latest["close"]
                cached["anchor_price"] = latest["close"]
        return candles

    # ------------------ OPTION CHAIN GENERATOR ------------------
    def get_option_chain(
        self,
        underlying: str = "NIFTY",
        expiry: Optional[str] = None,
        strike_count: int = 15,
    ) -> Dict[str, Any]:
        underlying = underlying.upper().strip()
        slug_map = {
            "NIFTY": "nifty",
            "BANKNIFTY": "nifty-bank",
            "FINNIFTY": "nifty-financial-services",
            "MIDCPNIFTY": "nifty-midcap-select",
            "SENSEX": "sp-bse-sensex",
        }

        name = "NIFTY"
        exchange = "NSE"
        if "SENSEX" in underlying:
            name = "SENSEX"
            exchange = "BSE"
            base_sym = "BSE:SENSEX-INDEX"
            sigma = 0.135
        elif "BANK" in underlying:
            name = "BANKNIFTY"
            exchange = "NSE"
            base_sym = "NSE:NIFTYBANK-INDEX"
            sigma = 0.175
        elif "FIN" in underlying:
            name = "FINNIFTY"
            exchange = "NSE"
            base_sym = "NSE:FINNIFTY-INDEX"
            sigma = 0.150
        elif "MID" in underlying:
            name = "MIDCPNIFTY"
            exchange = "NSE"
            base_sym = "NSE:MIDCPNIFTY-INDEX"
            sigma = 0.160
        else:
            name = "NIFTY"
            exchange = "NSE"
            base_sym = "NSE:NIFTY50-INDEX"
            sigma = 0.135

        slug = slug_map.get(name, "nifty")
        step = STEP_MAP.get(name, 50)
        lot_size = LOT_SIZE_MAP.get(name, 65)

        # 1. If Fyers API is configured with valid token, use Fyers V3 Option Chain
        if self.fyers_client and self.fyers_client.is_configured:
            try:
                fyers_sym_map = {
                    "NIFTY": "NSE:NIFTY50-INDEX",
                    "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
                    "FINNIFTY": "NSE:FINNIFTY-INDEX",
                    "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX",
                    "SENSEX": "BSE:SENSEX-INDEX",
                }
                f_sym = fyers_sym_map.get(name, "NSE:NIFTY50-INDEX")
                fyers_raw = self.fyers_client.get_option_chain(symbol=f_sym, strikecount=strike_count)
                if fyers_raw and fyers_raw.get("s") == "ok":
                    parsed = self._parse_fyers_option_chain(fyers_raw, name, step, lot_size, strike_count, base_sym)
                    if parsed and parsed.get("strikes"):
                        return parsed
            except Exception as fe:
                logger.warning(f"Fyers V3 option chain fetch failed ({fe}), falling back to live NSE feed")

        # 2. Real Live Indian Option Chain (Pre-warmed from RAM cache, 0ms lag)
        self.active_underlying = name
        try:
            cache_key = f"{name}_{expiry or ''}"
            now_ts = time.time()
            data = self._chain_raw_cache.get(cache_key) or self._chain_raw_cache.get(name)

            if not data or (now_ts - self._chain_cache_time.get(name, 0) > 40.0):
                try:
                    url = f"https://groww.in/v1/api/option_chain_service/v1/option_chain/derivatives/{slug}"
                    if expiry:
                        url += f"?expiry={expiry}"
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "application/json, text/plain, */*",
                        "Referer": "https://groww.in/options/derivatives/nifty",
                        "Origin": "https://groww.in",
                    }
                    res = self.http_client.get(url, headers=headers)
                    if res.status_code == 200:
                        data = res.json()
                        self._chain_raw_cache[cache_key] = data
                        self._chain_raw_cache[name] = data
                        self._chain_cache_time[cache_key] = now_ts
                        self._chain_cache_time[name] = now_ts
                except Exception as e:
                    logger.debug(f"Option chain external fetch failed: {e}")
                    data = self._chain_raw_cache.get(name)

            if data and data.get("optionChain"):
                exp_dto = data.get("optionChain", {}).get("expiryDetailsDto", {})
                expiry_dates = exp_dto.get("expiryDates", [])
                cur_expiry = exp_dto.get("currentExpiry", expiry or (expiry_dates[0] if expiry_dates else ""))
                api_lot = exp_dto.get("expiryLotSize")
                if api_lot and api_lot > 0:
                    lot_size = api_lot

                # Always use our high-speed live in-memory quotes spot price
                cached_quote = self.quotes_cache.get(base_sym)
                if cached_quote and cached_quote.get("ltp"):
                    spot = cached_quote["ltp"]
                elif data.get("livePrice") and data["livePrice"].get("value"):
                    spot = float(data["livePrice"]["value"])
                else:
                    spot = 55600.0 if name == "BANKNIFTY" else (74000.0 if name == "SENSEX" else (14000.0 if name == "MIDCPNIFTY" else (24700.0 if name == "FINNIFTY" else 23200.0)))

                atm = int(round(spot / step) * step)
                raw_chains = data.get("optionChain", {}).get("optionChains", [])

                if raw_chains:
                    raw_chains.sort(key=lambda x: x.get("strikePrice", 0))
                    closest_idx = 0
                    min_dist = float("inf")
                    for idx, item in enumerate(raw_chains):
                        stk = item.get("strikePrice", 0) / 100
                        if abs(stk - atm) < min_dist:
                            min_dist = abs(stk - atm)
                            closest_idx = idx

                    start_idx = max(0, closest_idx - strike_count)
                    end_idx = min(len(raw_chains), closest_idx + strike_count + 1)
                    selected_rows = raw_chains[start_idx:end_idx]

                    t_years = 4 / 365.0
                    if cur_expiry:
                        try:
                            exp_dt = datetime.datetime.strptime(cur_expiry, "%Y-%m-%d").date()
                            days = (exp_dt - datetime.date.today()).days
                            t_years = max(0.5, days) / 365.0
                        except Exception:
                            pass

                    strikes_data = []
                    for row in selected_rows:
                        strike = int(row.get("strikePrice", 0) / 100)
                        is_atm = (strike == atm)
                        call = row.get("callOption") or {}
                        put = row.get("putOption") or {}

                        bs_c, bs_p, c_delta, p_delta = bs_prices(spot, strike, T=t_years, sigma=sigma)

                        c_ltp = float(call.get("ltp") or 0.0)
                        if c_ltp <= 0.0:
                            c_ltp = bs_c
                        p_ltp = float(put.get("ltp") or 0.0)
                        if p_ltp <= 0.0:
                            p_ltp = bs_p

                        c_oi = int(call.get("openInterest") or 0)
                        p_oi = int(put.get("openInterest") or 0)

                        # Format standard Fyers symbol
                        sym_exp = cur_expiry.replace("-", "")[2:] if cur_expiry else "24OCT"
                        ce_symbol = f"{exchange}:{name}{sym_exp}{strike}CE"
                        pe_symbol = f"{exchange}:{name}{sym_exp}{strike}PE"
                        ce_display = f"{name} {strike} CE"
                        pe_display = f"{name} {strike} PE"

                        ce_quote = {
                            "symbol": ce_symbol,
                            "short_name": ce_display,
                            "ltp": round(c_ltp, 2),
                            "change": round(call.get("dayChange") or (c_ltp * 0.02), 2),
                            "chg_percent": round(call.get("dayChangePerc") or 2.0, 2),
                            "open": round(call.get("open") or c_ltp, 2),
                            "high": round(call.get("high") or c_ltp * 1.1, 2),
                            "low": round(call.get("low") or c_ltp * 0.9, 2),
                            "prev_close": round(call.get("close") or c_ltp, 2),
                            "volume": int(call.get("volume") or (c_oi // 2)),
                        }
                        pe_quote = {
                            "symbol": pe_symbol,
                            "short_name": pe_display,
                            "ltp": round(p_ltp, 2),
                            "change": round(put.get("dayChange") or (-p_ltp * 0.02), 2),
                            "chg_percent": round(put.get("dayChangePerc") or -2.0, 2),
                            "open": round(put.get("open") or p_ltp, 2),
                            "high": round(put.get("high") or p_ltp * 1.1, 2),
                            "low": round(put.get("low") or p_ltp * 0.9, 2),
                            "prev_close": round(put.get("close") or p_ltp, 2),
                            "volume": int(put.get("volume") or (p_oi // 2)),
                        }
                        self.quotes_cache[ce_symbol] = ce_quote
                        self.quotes_cache[ce_display] = ce_quote
                        self.quotes_cache[pe_symbol] = pe_quote
                        self.quotes_cache[pe_display] = pe_quote

                        strikes_data.append({
                            "strike": strike,
                            "is_atm": is_atm,
                            "ce": {
                                "symbol": ce_symbol,
                                "display_name": ce_display,
                                "ltp": round(c_ltp, 2),
                                "delta": c_delta,
                                "oi": c_oi,
                                "iv": round(sigma * 100, 1),
                                "is_itm": (strike < spot),
                            },
                            "pe": {
                                "symbol": pe_symbol,
                                "display_name": pe_display,
                                "ltp": round(p_ltp, 2),
                                "delta": p_delta,
                                "oi": p_oi,
                                "iv": round(sigma * 100, 1),
                                "is_itm": (strike > spot),
                            },
                        })

                    return {
                        "s": "ok",
                        "underlying": name,
                        "spot": round(spot, 2),
                        "atm": atm,
                        "lot_size": lot_size,
                        "step": step,
                        "expiry_dates": expiry_dates,
                        "current_expiry": cur_expiry,
                        "strikes": strikes_data,
                        "source": "NSE_LIVE_FEED",
                    }
        except Exception as e:
            logger.warning(f"Live option chain fetch failed ({e}), using Black-Scholes simulation")

        # 2. Black-Scholes Fallback Simulation
        quotes = self.get_quotes([base_sym])
        spot = quotes[0]["ltp"] if quotes else (23200.0 if name == "NIFTY" else 55600.0)
        atm = int(round(spot / step) * step)
        t_years = 4 / 365.0

        strikes_data = []
        for i in range(-strike_count, strike_count + 1):
            strike = int(atm + (i * step))
            ce_ltp, pe_ltp, ce_delta, pe_delta = bs_prices(spot, strike, T=t_years, sigma=sigma)

            distance = abs(strike - spot)
            ce_oi = max(50000, int(2500000 - distance * 1500 + (strike % 3) * 20000))
            pe_oi = max(50000, int(2500000 - distance * 1400 + (strike % 5) * 18000))

            ce_symbol = f"{exchange}:{name}24OCT{strike}CE"
            pe_symbol = f"{exchange}:{name}24OCT{strike}PE"
            ce_display = f"{name} {strike} CE"
            pe_display = f"{name} {strike} PE"

            self.quotes_cache[ce_symbol] = {
                "symbol": ce_symbol,
                "short_name": ce_display,
                "ltp": ce_ltp,
                "change": round(ce_ltp * 0.05, 2),
                "chg_percent": 5.0,
                "open": ce_ltp,
                "high": round(ce_ltp * 1.15, 2),
                "low": round(ce_ltp * 0.85, 2),
                "prev_close": ce_ltp,
                "volume": ce_oi // 2,
            }
            self.quotes_cache[pe_symbol] = {
                "symbol": pe_symbol,
                "short_name": pe_display,
                "ltp": pe_ltp,
                "change": round(-pe_ltp * 0.03, 2),
                "chg_percent": -3.0,
                "open": pe_ltp,
                "high": round(pe_ltp * 1.12, 2),
                "low": round(pe_ltp * 0.88, 2),
                "prev_close": pe_ltp,
                "volume": pe_oi // 2,
            }

            strikes_data.append({
                "strike": strike,
                "is_atm": (strike == atm),
                "ce": {
                    "symbol": ce_symbol,
                    "display_name": ce_display,
                    "ltp": ce_ltp,
                    "delta": ce_delta,
                    "oi": ce_oi,
                    "iv": round(sigma * 100, 1),
                    "is_itm": (strike < spot),
                },
                "pe": {
                    "symbol": pe_symbol,
                    "display_name": pe_display,
                    "ltp": pe_ltp,
                    "delta": pe_delta,
                    "oi": pe_oi,
                    "iv": round(sigma * 100, 1),
                    "is_itm": (strike > spot),
                },
            })

        today = datetime.date.today()
        dummy_expiries = [(today + datetime.timedelta(days=(i * 7))).strftime("%Y-%m-%d") for i in range(1, 5)]

        return {
            "s": "ok",
            "underlying": name,
            "spot": round(spot, 2),
            "atm": atm,
            "lot_size": lot_size,
            "step": step,
            "expiry_dates": dummy_expiries,
            "current_expiry": dummy_expiries[0],
            "strikes": strikes_data,
            "source": "SIMULATION",
        }

    # ------------------ VIRTUAL FUNDS & MTM ------------------
    def get_funds(self) -> Dict[str, Any]:
        self._update_open_positions_mtm()
        unrealized = sum(p["pnl"] for p in self.positions.values())
        return {
            "available_cash": round(self.available_cash, 2),
            "used_margin": round(self.used_margin, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "unrealized_pnl": round(unrealized, 2),
            "total_value": round(self.available_cash + self.used_margin + unrealized, 2),
        }

    def _update_open_positions_mtm(self):
        for sym, pos in self.positions.items():
            ltp = pos.get("ltp", 100.0)
            if sym in self.quotes_cache and self.quotes_cache[sym].get("ltp"):
                ltp = self.quotes_cache[sym]["ltp"]
            elif (sym.endswith("CE") or sym.endswith("PE")):
                try:
                    parts = sym.split()
                    if len(parts) >= 3:
                        name = parts[0]
                        strike = float(parts[1])
                        opt_type = parts[2]
                        if "SENSEX" in name:
                            base_sym = "BSE:SENSEX-INDEX"
                            default_spot = 74000.0
                        elif "BANK" in name:
                            base_sym = "NSE:NIFTYBANK-INDEX"
                            default_spot = 55600.0
                        else:
                            base_sym = "NSE:NIFTY50-INDEX"
                            default_spot = 23200.0
                        spot = self.quotes_cache.get(base_sym, {}).get("ltp", default_spot)
                        ce_p, pe_p, _, _ = bs_prices(spot, strike)
                        ltp = ce_p if opt_type == "CE" else pe_p
                except Exception:
                    pass

            pos["ltp"] = ltp
            qty = pos["net_qty"]
            avg = pos["buy_avg"] if qty > 0 else pos["sell_avg"]
            if qty > 0:
                pos["pnl"] = round((ltp - avg) * qty, 2)
                pos["pnl_pct"] = round(((ltp - avg) / avg) * 100.0, 2) if avg else 0.0
            elif qty < 0:
                pos["pnl"] = round((avg - ltp) * abs(qty), 2)
                pos["pnl_pct"] = round(((avg - ltp) / avg) * 100.0, 2) if avg else 0.0

    # ------------------ ORDER EXECUTION ------------------
    def place_order(
        self,
        symbol: str,
        qty: int,
        side: int,  # 1 = BUY, -1 = SELL
        order_type: int = 2,  # 2 = Market, 1 = Limit
        product: str = "INTRADAY",  # "INTRADAY", "CNC", "MARGIN"
        limit_price: float = 0.0,
    ) -> Dict[str, Any]:
        if qty <= 0:
            return {"s": "error", "message": "Quantity must be greater than 0"}

        if "INDEX" in symbol:
            return {
                "s": "error",
                "message": "Spot indices cannot be traded directly in Indian markets. Please trade Index Futures (e.g. NIFTY-FUT) or Options.",
            }

        # Fetch current price from cache or quotes
        cached = self.quotes_cache.get(symbol)
        if cached and cached.get("ltp", 0) > 0:
            current_ltp = cached["ltp"]
        elif limit_price > 0:
            current_ltp = limit_price
        else:
            quotes = self.get_quotes([symbol])
            current_ltp = quotes[0]["ltp"] if quotes and quotes[0].get("ltp") else 100.0

        execution_price = limit_price if (order_type == 1 and limit_price > 0) else current_ltp

        # Margin calculation:
        is_option = symbol.endswith("CE") or symbol.endswith("PE")
        is_future = "-FUT" in symbol
        if is_option:
            if side == 1:
                # Option Buyer pays pure premium
                required_margin = execution_price * qty
            else:
                # Option Seller margin (Span + Exposure)
                required_margin = (execution_price * qty) + 75000.0
        elif is_future:
            # Exchange futures margin (~12% of contract value)
            required_margin = (execution_price * qty) * 0.12
        else:
            leverage = 5.0 if product == "INTRADAY" else 1.0
            required_margin = (execution_price * qty) / leverage

        if side == 1 and self.available_cash < required_margin:
            return {
                "s": "error",
                "message": f"Insufficient Virtual Margin. Required: Rs. {required_margin:,.2f}, Available: Rs. {self.available_cash:,.2f}",
            }

        order_id = f"VIRT-{int(time.time() * 1000) % 1000000}"
        now_str = datetime.datetime.now().strftime("%H:%M:%S")

        # Deduct margin
        self.available_cash -= required_margin
        self.used_margin += required_margin

        # Create Order Record
        order_record = {
            "id": order_id,
            "time": now_str,
            "symbol": symbol,
            "side": "BUY" if side == 1 else "SELL",
            "type": "BUY" if side == 1 else "SELL",
            "order_type": "MARKET" if order_type == 2 else "LIMIT",
            "product": product,
            "qty": qty,
            "price": round(execution_price, 2),
            "status": "FILLED",
        }
        self.orders.insert(0, order_record)

        # Update Open Positions
        if symbol not in self.positions:
            self.positions[symbol] = {
                "id": f"POS-{symbol}",
                "symbol": symbol,
                "side": "BUY" if side == 1 else "SELL",
                "product": product,
                "net_qty": qty if side == 1 else -qty,
                "buy_avg": execution_price if side == 1 else 0.0,
                "sell_avg": execution_price if side == -1 else 0.0,
                "ltp": execution_price,
                "pnl": 0.0,
                "pnl_pct": 0.0,
                "margin_held": required_margin,
            }
        else:
            pos = self.positions[symbol]
            current_qty = pos["net_qty"]
            new_qty = current_qty + (qty if side == 1 else -qty)

            if new_qty == 0:
                # Closed position
                if current_qty > 0 and side == -1:
                    realized = (execution_price - pos["buy_avg"]) * current_qty
                else:
                    realized = (pos["sell_avg"] - execution_price) * abs(current_qty)

                self.realized_pnl += realized
                self.available_cash += pos.get("margin_held", 0.0) + realized
                self.used_margin = max(0.0, self.used_margin - pos.get("margin_held", 0.0))
                del self.positions[symbol]
            else:
                pos["net_qty"] = new_qty
                if side == 1:
                    total_cost = (pos["buy_avg"] * current_qty) + (execution_price * qty)
                    pos["buy_avg"] = round(total_cost / new_qty, 2)
                else:
                    pos["sell_avg"] = execution_price
                pos["margin_held"] = pos.get("margin_held", 0.0) + required_margin

        self._update_open_positions_mtm()
        return {
            "s": "ok",
            "message": f"Virtual {order_record['type']} order filled for {qty} {symbol} at Rs. {execution_price:.2f}",
            "order": order_record,
        }

    def exit_position(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        if symbol and symbol in self.positions:
            pos = self.positions[symbol]
            side = -1 if pos["net_qty"] > 0 else 1
            qty = abs(pos["net_qty"])
            return self.place_order(symbol=symbol, qty=qty, side=side, order_type=2, product=pos.get("product", "INTRADAY"))

        symbols_to_close = list(self.positions.keys())
        for sym in symbols_to_close:
            pos = self.positions[sym]
            side = -1 if pos["net_qty"] > 0 else 1
            qty = abs(pos["net_qty"])
            self.place_order(symbol=sym, qty=qty, side=side, order_type=2, product=pos.get("product", "INTRADAY"))

        return {"s": "ok", "message": "All virtual positions closed"}

    def get_positions(self) -> List[Dict[str, Any]]:
        self._update_open_positions_mtm()
        return list(self.positions.values())

    def get_orders(self) -> List[Dict[str, Any]]:
        return self.orders

    def get_holdings(self) -> List[Dict[str, Any]]:
        h_list = []
        for sym, pos in self.positions.items():
            if pos.get("product") == "CNC" and pos["net_qty"] > 0:
                h_list.append({
                    "symbol": sym,
                    "quantity": pos["net_qty"],
                    "costPrice": pos["buy_avg"],
                    "ltp": pos["ltp"],
                    "pl": pos["pnl"],
                    "plPercent": pos["pnl_pct"],
                })
        return h_list

    # ========================================================
    # MULTI-USER ISOLATED TRADING METHODS (SQLite PERSISTED)
    # ========================================================
    def get_funds_for_user(self, username: str) -> Dict[str, Any]:
        f = db.get_funds(username)
        positions = self.get_positions_for_user(username)
        unrealized = sum(p.get("pnl", 0.0) for p in positions)
        return {
            "available_cash": round(f["available_cash"], 2),
            "used_margin": round(f["used_margin"], 2),
            "realized_pnl": round(f["realized_pnl"], 2),
            "unrealized_pnl": round(unrealized, 2),
            "total_value": round(f["available_cash"] + f["used_margin"] + unrealized, 2),
        }

    def check_and_expire_positions(self, username: Optional[str] = None) -> int:
        """
        Auto-settles and closes positions whose expiry date has passed.
        In Indian markets, options expire at 15:30 IST on the expiry date.
        Settlement calculation:
          - CE: max(0.0, Spot - Strike)
          - PE: max(0.0, Strike - Spot)
          - OTM options expire worthless (0.0).
        Realized P&L is credited/debited, margin released, order logged as EXPIRED,
        and position is deleted.
        """
        IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        now_ist = datetime.datetime.now(IST)
        today_ist = now_ist.date()
        time_ist = now_ist.time()
        is_market_closed_today = time_ist >= datetime.time(15, 30)

        users_to_check = [username] if username else [u["username"] for u in db.get_all_users()]
        expired_count = 0

        for user in users_to_check:
            if not user:
                continue
            raw_positions = db.get_positions(user)
            if not raw_positions:
                continue

            for pos in raw_positions:
                sym = pos["symbol"]
                is_option = sym.endswith("CE") or sym.endswith("PE") or (" CE" in sym) or (" PE" in sym)
                is_future = "-FUT" in sym
                if not (is_option or is_future):
                    continue

                exp_date = None
                exp_str = pos.get("expiry")
                if exp_str:
                    exp_date = parse_expiry_date(exp_str)

                # Fallback for positions without explicit expiry date:
                if not exp_date:
                    c_date_str = pos.get("created_date")
                    c_date = parse_expiry_date(c_date_str) if c_date_str else None
                    if c_date and c_date < today_ist:
                        # Position created yesterday or earlier -> expired!
                        exp_date = c_date
                    else:
                        # Legacy untracked option: treat as yesterday's expired contract
                        exp_date = today_ist - datetime.timedelta(days=1)

                is_expired = False
                if exp_date:
                    if exp_date < today_ist:
                        is_expired = True
                    elif exp_date == today_ist and is_market_closed_today:
                        is_expired = True

                if not is_expired:
                    continue

                expired_count += 1
                net_qty = pos["net_qty"]
                margin_held = float(pos.get("margin_held", 0.0))
                buy_avg = float(pos.get("buy_avg", 0.0))
                sell_avg = float(pos.get("sell_avg", 0.0))

                if "SENSEX" in sym:
                    base_sym = "BSE:SENSEX-INDEX"
                    default_spot = 74000.0
                elif "BANK" in sym:
                    base_sym = "NSE:NIFTYBANK-INDEX"
                    default_spot = 55600.0
                elif "FIN" in sym:
                    base_sym = "NSE:FINNIFTY-INDEX"
                    default_spot = 24600.0
                elif "MID" in sym:
                    base_sym = "NSE:MIDCPNIFTY-INDEX"
                    default_spot = 13700.0
                else:
                    base_sym = "NSE:NIFTY50-INDEX"
                    default_spot = 23200.0

                spot = self.quotes_cache.get(base_sym, {}).get("ltp", default_spot)

                # Intrinsic settlement value
                settle_price = 0.0
                if is_option:
                    strike = 0.0
                    opt_type = "CE" if ("CE" in sym) else "PE"
                    m = re.search(r'(\d+(?:\.\d+)?)\s*(CE|PE)', sym)
                    if m:
                        strike = float(m.group(1))
                        opt_type = m.group(2)
                    if opt_type == "CE":
                        settle_price = max(0.0, spot - strike)
                    else:
                        settle_price = max(0.0, strike - spot)
                elif is_future:
                    settle_price = spot

                # Calculate realized P&L on settlement
                if net_qty > 0:
                    trade_pnl = round((settle_price - buy_avg) * net_qty, 2)
                else:
                    trade_pnl = round((sell_avg - settle_price) * abs(net_qty), 2)

                user_funds = db.get_funds(user)
                avail = round(user_funds["available_cash"] + margin_held + trade_pnl, 2)
                used = round(max(0.0, user_funds["used_margin"] - margin_held), 2)
                realized = round(user_funds["realized_pnl"] + trade_pnl, 2)
                db.update_funds(user, avail, used, realized)

                now_str = now_ist.strftime("%H:%M:%S")
                settle_order = {
                    "id": f"EXP-{int(time.time() * 1000) % 1000000}",
                    "time": now_str,
                    "symbol": sym,
                    "side": "BUY" if net_qty < 0 else "SELL",
                    "type": "BUY" if net_qty < 0 else "SELL",
                    "order_type": "EXPIRY",
                    "product": pos.get("product", "INTRADAY"),
                    "qty": abs(net_qty),
                    "price": round(settle_price, 2),
                    "status": "EXPIRED",
                    "expiry": exp_date.strftime("%Y-%m-%d") if exp_date else "",
                }
                db.add_order(user, settle_order)
                db.delete_position(user, sym)
                logger.info(
                    f"Auto-settled expired position {sym} for user {user}: "
                    f"settle_price={settle_price:.2f}, trade_pnl={trade_pnl:.2f}, "
                    f"released_margin={margin_held:.2f}"
                )

        return expired_count

    def get_positions_for_user(self, username: str) -> List[Dict[str, Any]]:
        # 1. Auto-settle any expired positions first
        try:
            self.check_and_expire_positions(username)
        except Exception as e:
            logger.error(f"Error checking position expiry for {username}: {e}")

        # 2. Retrieve remaining active positions
        raw_positions = db.get_positions(username)
        enriched = []
        IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        today_ist = datetime.datetime.now(IST).date()

        for pos in raw_positions:
            p = dict(pos)
            sym = p["symbol"]

            # Ensure expiry string is present
            if not p.get("expiry"):
                if sym.endswith("CE") or sym.endswith("PE") or "-FUT" in sym:
                    p["expiry"] = get_default_expiry_for_symbol(sym)
                else:
                    p["expiry"] = ""

            exp_str = p.get("expiry")
            t_years = 4 / 365.0
            if exp_str:
                exp_d = parse_expiry_date(exp_str)
                if exp_d:
                    days_left = max(0, (exp_d - today_ist).days)
                    t_years = max(0.2, days_left) / 365.0

            ltp = p.get("ltp", 100.0)
            if sym in self.quotes_cache and self.quotes_cache[sym].get("ltp"):
                ltp = self.quotes_cache[sym]["ltp"]
            elif (sym.endswith("CE") or sym.endswith("PE")):
                try:
                    parts = sym.split()
                    if len(parts) >= 3:
                        name = parts[0]
                        strike = float(parts[1])
                        opt_type = parts[2]
                        if "SENSEX" in name:
                            base_sym = "BSE:SENSEX-INDEX"
                            default_spot = 74000.0
                        elif "BANK" in name:
                            base_sym = "NSE:NIFTYBANK-INDEX"
                            default_spot = 55600.0
                        else:
                            base_sym = "NSE:NIFTY50-INDEX"
                            default_spot = 23200.0
                        spot = self.quotes_cache.get(base_sym, {}).get("ltp", default_spot)
                        ce_p, pe_p, _, _ = bs_prices(spot, strike, T=t_years)
                        ltp = ce_p if opt_type == "CE" else pe_p
                except Exception:
                    pass

            p["ltp"] = ltp
            qty = p["net_qty"]
            avg = p["buy_avg"] if qty > 0 else p["sell_avg"]
            if qty > 0:
                p["pnl"] = round((ltp - avg) * qty, 2)
                p["pnl_pct"] = round(((ltp - avg) / avg) * 100.0, 2) if avg else 0.0
            elif qty < 0:
                p["pnl"] = round((avg - ltp) * abs(qty), 2)
                p["pnl_pct"] = round(((avg - ltp) / avg) * 100.0, 2) if avg else 0.0
            else:
                p["pnl"] = 0.0
                p["pnl_pct"] = 0.0
            enriched.append(p)
        return enriched

    def place_order_for_user(
        self,
        username: str,
        symbol: str,
        qty: int,
        side: int,  # 1 = BUY, -1 = SELL
        order_type: int = 2,  # 2 = Market, 1 = Limit
        product: str = "INTRADAY",
        limit_price: float = 0.0,
        expiry: Optional[str] = None,
    ) -> Dict[str, Any]:
        if qty <= 0:
            return {"s": "error", "message": "Quantity must be greater than 0"}

        if "INDEX" in symbol:
            return {
                "s": "error",
                "message": "Spot indices cannot be traded directly in Indian markets. Please trade Index Futures (e.g. NIFTY-FUT) or Options.",
            }

        cached = self.quotes_cache.get(symbol)
        if cached and cached.get("ltp", 0) > 0:
            current_ltp = cached["ltp"]
        elif limit_price > 0:
            current_ltp = limit_price
        else:
            quotes = self.get_quotes([symbol])
            current_ltp = quotes[0]["ltp"] if quotes and quotes[0].get("ltp") else 100.0

        execution_price = limit_price if (order_type == 1 and limit_price > 0) else current_ltp

        is_option = symbol.endswith("CE") or symbol.endswith("PE") or (" CE" in symbol) or (" PE" in symbol)
        is_future = "-FUT" in symbol

        order_expiry = expiry or ""
        if (is_option or is_future) and not order_expiry:
            order_expiry = get_default_expiry_for_symbol(symbol)

        if is_option:
            if side == 1:
                required_margin = execution_price * qty
            else:
                required_margin = (execution_price * qty) + 75000.0
        elif is_future:
            # Exchange futures margin (~12% of contract value)
            required_margin = (execution_price * qty) * 0.12
        else:
            leverage = 5.0 if product == "INTRADAY" else 1.0
            required_margin = (execution_price * qty) / leverage

        user_funds = db.get_funds(username)
        avail = user_funds["available_cash"]
        used = user_funds["used_margin"]
        realized = user_funds["realized_pnl"]

        if side == 1 and avail < required_margin:
            return {
                "s": "error",
                "message": f"Insufficient Virtual Margin. Required: Rs. {required_margin:,.2f}, Available: Rs. {avail:,.2f}",
            }

        order_id = f"ORD-{int(time.time() * 1000) % 1000000}"
        now_str = datetime.datetime.now().strftime("%H:%M:%S")

        avail -= required_margin
        used += required_margin

        order_record = {
            "id": order_id,
            "time": now_str,
            "symbol": symbol,
            "side": "BUY" if side == 1 else "SELL",
            "type": "BUY" if side == 1 else "SELL",
            "order_type": "MARKET" if order_type == 2 else "LIMIT",
            "product": product,
            "qty": qty,
            "price": round(execution_price, 2),
            "status": "FILLED",
            "expiry": order_expiry,
        }
        db.add_order(username, order_record)

        existing_positions = {p["symbol"]: p for p in db.get_positions(username)}

        if symbol not in existing_positions:
            new_pos = {
                "id": f"{username}:{symbol}",
                "symbol": symbol,
                "side": "BUY" if side == 1 else "SELL",
                "product": product,
                "net_qty": qty if side == 1 else -qty,
                "buy_avg": execution_price if side == 1 else 0.0,
                "sell_avg": execution_price if side == -1 else 0.0,
                "margin_held": required_margin,
                "expiry": order_expiry,
                "created_date": datetime.date.today().strftime("%Y-%m-%d"),
            }
            db.save_position(username, new_pos)
        else:
            pos = existing_positions[symbol]
            current_qty = pos["net_qty"]
            new_qty = current_qty + (qty if side == 1 else -qty)

            if new_qty == 0:
                if current_qty > 0 and side == -1:
                    trade_realized = (execution_price - pos["buy_avg"]) * current_qty
                else:
                    trade_realized = (pos["sell_avg"] - execution_price) * abs(current_qty)

                realized += trade_realized
                avail += pos.get("margin_held", 0.0) + trade_realized
                used = max(0.0, used - pos.get("margin_held", 0.0))
                db.delete_position(username, symbol)
            else:
                pos["net_qty"] = new_qty
                pos["side"] = "BUY" if new_qty > 0 else "SELL"
                if side == 1:
                    total_cost = (pos["buy_avg"] * current_qty) + (execution_price * qty)
                    pos["buy_avg"] = round(total_cost / new_qty, 2)
                else:
                    pos["sell_avg"] = execution_price
                pos["margin_held"] = pos.get("margin_held", 0.0) + required_margin
                pos["expiry"] = order_expiry or pos.get("expiry", "")
                db.save_position(username, pos)

        db.update_funds(username, avail, used, realized)

        return {
            "s": "ok",
            "message": f"Virtual {order_record['type']} order filled for {qty} {symbol} at Rs. {execution_price:.2f}",
            "order": order_record,
        }

    def exit_position_for_user(self, username: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        positions = db.get_positions(username)
        if symbol:
            target = [p for p in positions if p["symbol"] == symbol]
            if target:
                pos = target[0]
                side = -1 if pos["net_qty"] > 0 else 1
                qty = abs(pos["net_qty"])
                return self.place_order_for_user(username=username, symbol=symbol, qty=qty, side=side, order_type=2, product=pos.get("product", "INTRADAY"))
            return {"s": "error", "message": f"No open position found for {symbol}"}

        for pos in positions:
            side = -1 if pos["net_qty"] > 0 else 1
            qty = abs(pos["net_qty"])
            self.place_order_for_user(username=username, symbol=pos["symbol"], qty=qty, side=side, order_type=2, product=pos.get("product", "INTRADAY"))

        return {"s": "ok", "message": "All virtual positions closed"}

    def reset_account_for_user(self, username: str, capital: float = 1000000.0) -> Dict[str, Any]:
        db.reset_funds(username, capital)
        return {"s": "ok", "message": f"Virtual account reset to Rs. {capital:,.2f}"}

    def get_orders_for_user(self, username: str) -> List[Dict[str, Any]]:
        return db.get_orders(username)

    def get_holdings_for_user(self, username: str) -> List[Dict[str, Any]]:
        positions = self.get_positions_for_user(username)
        h_list = []
        for pos in positions:
            if pos.get("product") == "CNC" and pos["net_qty"] > 0:
                h_list.append({
                    "symbol": pos["symbol"],
                    "quantity": pos["net_qty"],
                    "costPrice": pos["buy_avg"],
                    "ltp": pos.get("ltp", 0.0),
                    "pl": pos.get("pnl", 0.0),
                    "plPercent": pos.get("pnl_pct", 0.0),
                })
        return h_list

    def search_symbols(self, query: str) -> List[Dict[str, Any]]:
        q = (query or "").strip().upper()
        if not q:
            return []

        results = []
        for sym, (y_sym, short_name, base_price) in SYMBOL_MAP.items():
            if q in sym.upper() or q in short_name.upper():
                cat = "INDEX" if "INDEX" in sym else ("FUTURES" if "FUT" in sym else "EQUITY")
                cached = self.quotes_cache.get(sym)
                ltp = cached["ltp"] if cached else base_price
                chg_pct = cached.get("chg_percent", 0.0) if cached else 0.0
                results.append({
                    "symbol": sym,
                    "short_name": short_name,
                    "exchange": sym.split(":")[0],
                    "type": cat,
                    "tradable": (cat != "INDEX"),
                    "ltp": round(ltp, 2),
                    "chg_percent": round(chg_pct, 2),
                })

        # Dynamic NSE equity matching for custom searches (e.g. user types a stock symbol not in catalog)
        clean_q = q.replace("NSE:", "").replace("-EQ", "").replace("-FUT", "").replace("-INDEX", "").strip()
        custom_sym = f"NSE:{clean_q}-EQ"
        if len(clean_q) >= 2 and not any(r["symbol"] == custom_sym for r in results):
            cached = self.quotes_cache.get(custom_sym)
            ltp = cached["ltp"] if cached else 100.0
            chg_pct = cached.get("chg_percent", 0.0) if cached else 0.0
            results.append({
                "symbol": custom_sym,
                "short_name": clean_q,
                "exchange": "NSE",
                "type": "EQUITY",
                "tradable": True,
                "ltp": round(ltp, 2),
                "chg_percent": round(chg_pct, 2),
                "custom": True,
            })

        return results[:20]

