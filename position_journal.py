"""
Position journal — tracks when APEX opened positions to enforce minimum hold times.
Stored in trading.db for persistence across restarts.
"""
import sqlite3
import os
from datetime import datetime, timezone, timedelta

_DB = os.path.join(os.path.dirname(__file__), "trading.db")

def _conn():
    db = sqlite3.connect(_DB)
    db.execute("""
        CREATE TABLE IF NOT EXISTS apex_positions (
            symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL DEFAULT 'option',
            option_type TEXT,
            strike REAL,
            expiration TEXT,
            entry_time TEXT NOT NULL,
            entry_price REAL,
            qty INTEGER,
            PRIMARY KEY (symbol, asset_type, strike, expiration)
        )
    """)
    db.commit()
    return db

def record_entry(symbol: str, asset_type: str = "option", option_type: str = None,
                 strike: float = None, expiration: str = None,
                 entry_price: float = None, qty: int = None):
    """Record when APEX opened a position."""
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as db:
        db.execute("""
            INSERT OR REPLACE INTO apex_positions
            (symbol, asset_type, option_type, strike, expiration, entry_time, entry_price, qty)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (symbol.upper(), asset_type, option_type, strike, expiration, now, entry_price, qty))
    return now

def get_hold_hours(symbol: str, asset_type: str = "option",
                   strike: float = None, expiration: str = None) -> float:
    """Return how many hours APEX has held this position. 0 if not tracked."""
    try:
        with _conn() as db:
            row = db.execute("""
                SELECT entry_time FROM apex_positions
                WHERE symbol=? AND asset_type=? AND strike=? AND expiration=?
            """, (symbol.upper(), asset_type, strike, expiration)).fetchone()
            if not row:
                return 0.0
            entry = datetime.fromisoformat(row[0])
            if entry.tzinfo is None:
                entry = entry.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - entry).total_seconds() / 3600
    except Exception:
        return 0.0

def is_min_hold_met(symbol: str, min_hours: float = 4.0,
                    strike: float = None, expiration: str = None) -> bool:
    """True if position has been held long enough to consider exiting."""
    hold_hours = get_hold_hours(symbol, strike=strike, expiration=expiration)
    if hold_hours == 0:
        return True  # not tracked = allow exit (manually opened)
    return hold_hours >= min_hours

def clear_position(symbol: str, strike: float = None, expiration: str = None):
    """Remove position from journal when exited."""
    try:
        with _conn() as db:
            if strike and expiration:
                db.execute("DELETE FROM apex_positions WHERE symbol=? AND strike=? AND expiration=?",
                          (symbol.upper(), strike, expiration))
            else:
                db.execute("DELETE FROM apex_positions WHERE symbol=?", (symbol.upper(),))
    except Exception:
        pass

def get_all_positions() -> list:
    """Return all tracked positions with hold time."""
    try:
        with _conn() as db:
            rows = db.execute("SELECT * FROM apex_positions ORDER BY entry_time DESC").fetchall()
            now = datetime.now(timezone.utc)
            result = []
            for r in rows:
                entry = datetime.fromisoformat(r[5])
                if entry.tzinfo is None:
                    entry = entry.replace(tzinfo=timezone.utc)
                hold_h = (now - entry).total_seconds() / 3600
                result.append({
                    "symbol": r[0], "asset_type": r[1], "option_type": r[2],
                    "strike": r[3], "expiration": r[4], "entry_time": r[5],
                    "entry_price": r[6], "qty": r[7], "hold_hours": round(hold_h, 1)
                })
            return result
    except Exception:
        return []
