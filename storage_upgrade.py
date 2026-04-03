import sqlite3
import time
from pathlib import Path

DB_PATH = "/root/tradingbot/tradingbot.db"


def get_conn():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS positions (
            product_id TEXT PRIMARY KEY,
            base_qty_total REAL NOT NULL DEFAULT 0,
            base_qty_liquid REAL NOT NULL DEFAULT 0,
            base_qty_locked REAL NOT NULL DEFAULT 0,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS realized_pnl (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            pnl_usd REAL NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS asset_state (
            product_id TEXT PRIMARY KEY,
            avg_entry_price REAL NOT NULL DEFAULT 0,
            last_harvest_ts INTEGER NOT NULL DEFAULT 0,
            last_buy_ts INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    conn.commit()
    conn.close()


def record_realized_pnl(product_id, pnl_usd):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO realized_pnl (product_id, pnl_usd) VALUES (?, ?)",
        (product_id, float(pnl_usd or 0.0)),
    )
    conn.commit()
    conn.close()


def get_total_realized_pnl():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(pnl_usd), 0) AS total_pnl FROM realized_pnl")
    row = cur.fetchone()
    conn.close()
    return float(row["total_pnl"] or 0.0)


def reset_positions():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM positions")
    conn.commit()
    conn.close()


def upsert_position(product_id, base_qty_total, base_qty_liquid, base_qty_locked):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO positions (
            product_id,
            base_qty_total,
            base_qty_liquid,
            base_qty_locked,
            updated_at
        )
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(product_id) DO UPDATE SET
            base_qty_total = excluded.base_qty_total,
            base_qty_liquid = excluded.base_qty_liquid,
            base_qty_locked = excluded.base_qty_locked,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            product_id,
            float(base_qty_total or 0),
            float(base_qty_liquid or 0),
            float(base_qty_locked or 0),
        ),
    )
    conn.commit()
    conn.close()


def get_position(product_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT product_id, base_qty_total, base_qty_liquid, base_qty_locked, updated_at
        FROM positions
        WHERE product_id = ?
        """,
        (product_id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "product_id": row["product_id"],
        "base_qty_total": row["base_qty_total"],
        "base_qty_liquid": row["base_qty_liquid"],
        "base_qty_locked": row["base_qty_locked"],
        "updated_at": row["updated_at"],
    }


def get_all_positions():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT product_id, base_qty_total, base_qty_liquid, base_qty_locked, updated_at
        FROM positions
        ORDER BY product_id
        """
    )
    rows = cur.fetchall()
    conn.close()

    return [
        {
            "product_id": row["product_id"],
            "base_qty_total": row["base_qty_total"],
            "base_qty_liquid": row["base_qty_liquid"],
            "base_qty_locked": row["base_qty_locked"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def get_asset_state(product_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT product_id, avg_entry_price, last_harvest_ts, last_buy_ts, updated_at
        FROM asset_state
        WHERE product_id = ?
        """,
        (product_id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return {
            "product_id": product_id,
            "avg_entry_price": 0.0,
            "last_harvest_ts": 0,
            "last_buy_ts": 0,
            "updated_at": None,
        }

    return {
        "product_id": row["product_id"],
        "avg_entry_price": float(row["avg_entry_price"] or 0.0),
        "last_harvest_ts": int(row["last_harvest_ts"] or 0),
        "last_buy_ts": int(row["last_buy_ts"] or 0),
        "updated_at": row["updated_at"],
    }


def get_all_asset_states():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT product_id, avg_entry_price, last_harvest_ts, last_buy_ts, updated_at
        FROM asset_state
        ORDER BY product_id
        """
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "product_id": row["product_id"],
            "avg_entry_price": float(row["avg_entry_price"] or 0.0),
            "last_harvest_ts": int(row["last_harvest_ts"] or 0),
            "last_buy_ts": int(row["last_buy_ts"] or 0),
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def upsert_asset_state(product_id, avg_entry_price=None, last_harvest_ts=None, last_buy_ts=None):
    existing = get_asset_state(product_id)
    avg_entry_price = existing["avg_entry_price"] if avg_entry_price is None else float(avg_entry_price or 0.0)
    last_harvest_ts = existing["last_harvest_ts"] if last_harvest_ts is None else int(last_harvest_ts or 0)
    last_buy_ts = existing["last_buy_ts"] if last_buy_ts is None else int(last_buy_ts or 0)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO asset_state (product_id, avg_entry_price, last_harvest_ts, last_buy_ts, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(product_id) DO UPDATE SET
            avg_entry_price = excluded.avg_entry_price,
            last_harvest_ts = excluded.last_harvest_ts,
            last_buy_ts = excluded.last_buy_ts,
            updated_at = CURRENT_TIMESTAMP
        """,
        (product_id, avg_entry_price, last_harvest_ts, last_buy_ts),
    )
    conn.commit()
    conn.close()


def record_buy_fill(product_id, filled_base, avg_fill_price):
    filled_base = float(filled_base or 0.0)
    avg_fill_price = float(avg_fill_price or 0.0)
    if filled_base <= 0 or avg_fill_price <= 0:
        return get_asset_state(product_id)

    pos = get_position(product_id) or {}
    current_qty = max(0.0, float(pos.get("base_qty_total", 0.0) or 0.0) - filled_base)
    state = get_asset_state(product_id)
    current_avg = float(state.get("avg_entry_price", 0.0) or 0.0)

    if current_qty <= 0 or current_avg <= 0:
        new_avg = avg_fill_price
    else:
        new_avg = ((current_qty * current_avg) + (filled_base * avg_fill_price)) / (current_qty + filled_base)

    now_ts = int(time.time())
    upsert_asset_state(product_id, avg_entry_price=new_avg, last_buy_ts=now_ts)
    return get_asset_state(product_id)


def record_sell_fill(product_id, filled_base):
    filled_base = float(filled_base or 0.0)
    if filled_base <= 0:
        return get_asset_state(product_id)

    pos = get_position(product_id) or {}
    remaining_qty = max(0.0, float(pos.get("base_qty_total", 0.0) or 0.0) - filled_base)
    state = get_asset_state(product_id)

    if remaining_qty <= 1e-12:
        upsert_asset_state(product_id, avg_entry_price=0.0)

    return get_asset_state(product_id)


def mark_harvest(product_id, ts=None):
    ts = int(ts or time.time())
    upsert_asset_state(product_id, last_harvest_ts=ts)
    return get_asset_state(product_id)
