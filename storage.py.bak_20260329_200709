import os
import sqlite3
import time
from pathlib import Path
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash

DEFAULT_DB_PATH = "/root/tradingbot/trading.db"


def get_db_path():
    db_path = str(os.getenv("TRADINGBOT_DB_PATH", DEFAULT_DB_PATH) or "").strip()
    return db_path or DEFAULT_DB_PATH


def get_conn():
    db_path = get_db_path()
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def utcnow_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS positions (
        product_id TEXT PRIMARY KEY,
        base_qty_total REAL NOT NULL DEFAULT 0,
        base_qty_liquid REAL NOT NULL DEFAULT 0,
        base_qty_locked REAL NOT NULL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS realized_pnl (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT,
        pnl_usd REAL NOT NULL DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS asset_state (
        product_id TEXT PRIMARY KEY,
        avg_entry_price REAL NOT NULL DEFAULT 0,
        last_harvest_ts INTEGER NOT NULL DEFAULT 0,
        last_buy_ts INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        product_id TEXT NOT NULL,
        side TEXT NOT NULL,
        base_size REAL NOT NULL DEFAULT 0,
        price REAL NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'UNKNOWN',
        created_at INTEGER NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        email TEXT,
        password_hash TEXT NOT NULL,
        is_admin INTEGER NOT NULL DEFAULT 0,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        last_login_at TEXT
    )
    """)

    conn.commit()
    conn.close()


def init_user_table():
    init_db()


def create_user(username, password, email=None, is_admin=0, is_active=1):
    username = str(username or "").strip().lower()
    email = str(email or "").strip() or None

    if not username:
        raise ValueError("Username is required.")

    if not password or len(str(password)) < 8:
        raise ValueError("Password must be at least 8 characters long.")

    password_hash = generate_password_hash(password)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO users (
        username,
        email,
        password_hash,
        is_admin,
        is_active,
        created_at
    )
    VALUES (?, ?, ?, ?, ?, ?)
    """, (
        username,
        email,
        password_hash,
        int(is_admin),
        int(is_active),
        utcnow_iso(),
    ))

    conn.commit()
    user_id = cur.lastrowid
    conn.close()
    return user_id


def get_user_by_username(username):
    username = str(username or "").strip().lower()
    if not username:
        return None

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    SELECT id, username, email, password_hash, is_admin, is_active, created_at, last_login_at
    FROM users
    WHERE username = ?
    """, (username,))

    row = cur.fetchone()
    conn.close()
    return row


def get_user_by_id(user_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    SELECT id, username, email, password_hash, is_admin, is_active, created_at, last_login_at
    FROM users
    WHERE id = ?
    """, (int(user_id),))

    row = cur.fetchone()
    conn.close()
    return row


def update_last_login(user_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    UPDATE users
    SET last_login_at = ?
    WHERE id = ?
    """, (utcnow_iso(), int(user_id)))

    conn.commit()
    conn.close()


def verify_user(username, password):
    user = get_user_by_username(username)

    if not user:
        return None

    if int(user["is_active"] or 0) != 1:
        return None

    if not check_password_hash(user["password_hash"], str(password or "")):
        return None

    return user


def save_order_fill(order_id, product_id, side, base_size, price, status, created_at=None):
    order_id = str(order_id or "").strip()
    product_id = str(product_id or "").strip().upper()
    side = str(side or "").strip().upper()
    base_size = float(base_size or 0.0)
    price = float(price or 0.0)
    status = str(status or "UNKNOWN").strip().upper()
    created_at = int(created_at or time.time())

    if not order_id or not product_id or not side:
        return False

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO orders (
        order_id,
        product_id,
        side,
        base_size,
        price,
        status,
        created_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(order_id) DO UPDATE SET
        product_id = excluded.product_id,
        side = excluded.side,
        base_size = excluded.base_size,
        price = excluded.price,
        status = excluded.status,
        created_at = excluded.created_at
    """, (
        order_id,
        product_id,
        side,
        base_size,
        price,
        status,
        created_at
    ))

    conn.commit()
    conn.close()
    return True


def record_realized_pnl(product_id, pnl_usd):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO realized_pnl (product_id, pnl_usd) VALUES (?, ?)",
        (str(product_id or "").strip().upper(), float(pnl_usd or 0.0)),
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
    product_id = str(product_id or "").strip().upper()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
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
    """, (
        product_id,
        float(base_qty_total or 0.0),
        float(base_qty_liquid or 0.0),
        float(base_qty_locked or 0.0),
    ))

    conn.commit()
    conn.close()


def save_position(product_id, base_qty_total, base_qty_liquid=None, base_qty_locked=None):
    base_qty_total = float(base_qty_total or 0.0)
    if base_qty_liquid is None:
        base_qty_liquid = base_qty_total
    if base_qty_locked is None:
        base_qty_locked = 0.0

    upsert_position(
        product_id=product_id,
        base_qty_total=base_qty_total,
        base_qty_liquid=float(base_qty_liquid or 0.0),
        base_qty_locked=float(base_qty_locked or 0.0),
    )
    return True


def get_position(product_id):
    product_id = str(product_id or "").strip().upper()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    SELECT product_id, base_qty_total, base_qty_liquid, base_qty_locked, updated_at
    FROM positions
    WHERE product_id = ?
    """, (product_id,))

    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "product_id": row["product_id"],
        "base_qty_total": float(row["base_qty_total"] or 0.0),
        "base_qty_liquid": float(row["base_qty_liquid"] or 0.0),
        "base_qty_locked": float(row["base_qty_locked"] or 0.0),
        "updated_at": row["updated_at"],
    }


def get_all_positions():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    SELECT product_id, base_qty_total, base_qty_liquid, base_qty_locked, updated_at
    FROM positions
    ORDER BY product_id
    """)

    rows = cur.fetchall()
    conn.close()

    return [
        {
            "product_id": row["product_id"],
            "base_qty_total": float(row["base_qty_total"] or 0.0),
            "base_qty_liquid": float(row["base_qty_liquid"] or 0.0),
            "base_qty_locked": float(row["base_qty_locked"] or 0.0),
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def get_asset_state(product_id):
    product_id = str(product_id or "").strip().upper()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    SELECT product_id, avg_entry_price, last_harvest_ts, last_buy_ts, updated_at
    FROM asset_state
    WHERE product_id = ?
    """, (product_id,))

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

    cur.execute("""
    SELECT product_id, avg_entry_price, last_harvest_ts, last_buy_ts, updated_at
    FROM asset_state
    ORDER BY product_id
    """)

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
    product_id = str(product_id or "").strip().upper()
    existing = get_asset_state(product_id)

    avg_entry_price = existing["avg_entry_price"] if avg_entry_price is None else float(avg_entry_price or 0.0)
    last_harvest_ts = existing["last_harvest_ts"] if last_harvest_ts is None else int(last_harvest_ts or 0)
    last_buy_ts = existing["last_buy_ts"] if last_buy_ts is None else int(last_buy_ts or 0)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO asset_state (
        product_id,
        avg_entry_price,
        last_harvest_ts,
        last_buy_ts,
        updated_at
    )
    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT(product_id) DO UPDATE SET
        avg_entry_price = excluded.avg_entry_price,
        last_harvest_ts = excluded.last_harvest_ts,
        last_buy_ts = excluded.last_buy_ts,
        updated_at = CURRENT_TIMESTAMP
    """, (
        product_id,
        avg_entry_price,
        last_harvest_ts,
        last_buy_ts,
    ))

    conn.commit()
    conn.close()
    return get_asset_state(product_id)


def record_buy_fill(product_id, filled_base, avg_fill_price, order_id=None, status="FILLED", created_at=None):
    product_id = str(product_id or "").strip().upper()
    filled_base = float(filled_base or 0.0)
    avg_fill_price = float(avg_fill_price or 0.0)
    created_at = int(created_at or time.time())

    if not product_id or filled_base <= 0 or avg_fill_price <= 0:
        return {
            "ok": False,
            "reason": "invalid_buy_fill",
            "product_id": product_id,
        }

    pos = get_position(product_id) or {}
    current_total = float(pos.get("base_qty_total", 0.0) or 0.0)
    current_liquid = float(pos.get("base_qty_liquid", 0.0) or 0.0)
    current_locked = float(pos.get("base_qty_locked", 0.0) or 0.0)

    state = get_asset_state(product_id)
    current_avg = float(state.get("avg_entry_price", 0.0) or 0.0)

    new_total = current_total + filled_base
    new_liquid = current_liquid + filled_base

    if current_total <= 0 or current_avg <= 0:
        new_avg = avg_fill_price
    else:
        new_avg = ((current_total * current_avg) + (filled_base * avg_fill_price)) / new_total

    save_position(
        product_id=product_id,
        base_qty_total=new_total,
        base_qty_liquid=new_liquid,
        base_qty_locked=current_locked,
    )

    upsert_asset_state(
        product_id=product_id,
        avg_entry_price=new_avg,
        last_buy_ts=created_at,
    )

    order_id = str(order_id or "").strip() or f"LOCAL-BUY-{product_id}-{created_at}"
    save_order_fill(
        order_id=order_id,
        product_id=product_id,
        side="BUY",
        base_size=filled_base,
        price=avg_fill_price,
        status=status,
        created_at=created_at,
    )

    return {
        "ok": True,
        "product_id": product_id,
        "base_size": filled_base,
        "price": avg_fill_price,
        "order_id": order_id,
    }


def record_sell_fill(product_id, filled_base, avg_fill_price=None, order_id=None, status="FILLED", created_at=None):
    product_id = str(product_id or "").strip().upper()
    filled_base = float(filled_base or 0.0)
    avg_fill_price = float(avg_fill_price or 0.0)
    created_at = int(created_at or time.time())

    if not product_id or filled_base <= 0:
        return {
            "ok": False,
            "reason": "invalid_sell_fill",
            "product_id": product_id,
        }

    pos = get_position(product_id) or {}
    current_total = float(pos.get("base_qty_total", 0.0) or 0.0)
    current_liquid = float(pos.get("base_qty_liquid", 0.0) or 0.0)
    current_locked = float(pos.get("base_qty_locked", 0.0) or 0.0)

    state = get_asset_state(product_id)
    current_avg = float(state.get("avg_entry_price", 0.0) or 0.0)

    sold_qty = min(filled_base, current_total if current_total > 0 else filled_base)
    remaining_total = max(0.0, current_total - sold_qty)
    remaining_liquid = max(0.0, current_liquid - sold_qty)

    save_position(
        product_id=product_id,
        base_qty_total=remaining_total,
        base_qty_liquid=remaining_liquid,
        base_qty_locked=current_locked,
    )

    if remaining_total <= 1e-12:
        upsert_asset_state(product_id=product_id, avg_entry_price=0.0)
    else:
        upsert_asset_state(product_id=product_id, avg_entry_price=current_avg)

    if avg_fill_price > 0 and current_avg > 0 and sold_qty > 0:
        realized_pnl = sold_qty * (avg_fill_price - current_avg)
        record_realized_pnl(product_id, realized_pnl)

    order_id = str(order_id or "").strip() or f"LOCAL-SELL-{product_id}-{created_at}"
    save_order_fill(
        order_id=order_id,
        product_id=product_id,
        side="SELL",
        base_size=sold_qty,
        price=avg_fill_price,
        status=status,
        created_at=created_at,
    )

    return {
        "ok": True,
        "product_id": product_id,
        "base_size": sold_qty,
        "price": avg_fill_price,
        "order_id": order_id,
    }


def mark_harvest(product_id, ts=None):
    ts = int(ts or time.time())
    upsert_asset_state(product_id, last_harvest_ts=ts)
    return get_asset_state(product_id)
