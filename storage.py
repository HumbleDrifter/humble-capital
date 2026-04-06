import json
import os
import sqlite3
import time
from pathlib import Path
from datetime import datetime

from env_runtime import load_runtime_env, preferred_env_path
from werkzeug.security import generate_password_hash, check_password_hash

load_runtime_env(override=True)

DEFAULT_DB_PATH = str((preferred_env_path().parent / "trading.db").resolve())


def get_db_path():
    load_runtime_env(override=True)
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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        total_value_usd REAL NOT NULL,
        cash_value_usd REAL NOT NULL DEFAULT 0,
        positions_value_usd REAL NOT NULL DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_portfolio_history_ts
    ON portfolio_history (ts)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS config_proposals (
        id TEXT PRIMARY KEY,
        proposal_type TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT,
        status TEXT NOT NULL,
        fingerprint TEXT NOT NULL,
        proposal_json TEXT NOT NULL,
        summary_text TEXT NOT NULL,
        approved_at TEXT,
        approved_by TEXT,
        rejected_at TEXT,
        rejected_by TEXT,
        applied_at TEXT,
        applied_by TEXT,
        expired_at TEXT,
        superseded_at TEXT
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_config_proposals_status_created
    ON config_proposals (status, created_at DESC)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_config_proposals_fingerprint_status
    ON config_proposals (fingerprint, status)
    """)

    conn.commit()
    conn.close()
    ensure_options_tracking_tables()


def init_user_table():
    init_db()


def ensure_portfolio_history_table():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        total_value_usd REAL NOT NULL,
        cash_value_usd REAL NOT NULL DEFAULT 0,
        positions_value_usd REAL NOT NULL DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_portfolio_history_ts
    ON portfolio_history (ts)
    """)

    conn.commit()
    conn.close()


def ensure_config_proposals_table():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS config_proposals (
        id TEXT PRIMARY KEY,
        proposal_type TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT,
        status TEXT NOT NULL,
        fingerprint TEXT NOT NULL,
        proposal_json TEXT NOT NULL,
        summary_text TEXT NOT NULL,
        approved_at TEXT,
        approved_by TEXT,
        rejected_at TEXT,
        rejected_by TEXT,
        applied_at TEXT,
        applied_by TEXT,
        expired_at TEXT,
        superseded_at TEXT
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_config_proposals_status_created
    ON config_proposals (status, created_at DESC)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_config_proposals_fingerprint_status
    ON config_proposals (fingerprint, status)
    """)

    cur.execute("PRAGMA table_info(config_proposals)")
    columns = {str(row["name"]) for row in cur.fetchall() if row and row["name"]}

    if "approved_by" not in columns:
        cur.execute("ALTER TABLE config_proposals ADD COLUMN approved_by TEXT")

    if "rejected_by" not in columns:
        cur.execute("ALTER TABLE config_proposals ADD COLUMN rejected_by TEXT")

    if "applied_by" not in columns:
        cur.execute("ALTER TABLE config_proposals ADD COLUMN applied_by TEXT")

    conn.commit()
    conn.close()


def _next_config_proposal_id():
    ensure_config_proposals_table()
    prefix = f"CFG-{datetime.utcnow().strftime('%Y%m%d')}-"

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM config_proposals
        WHERE id LIKE ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (f"{prefix}%",),
    )
    row = cur.fetchone()
    conn.close()

    if not row or not row["id"]:
        return f"{prefix}001"

    last_id = str(row["id"])
    try:
        n = int(last_id.split("-")[-1])
    except Exception:
        n = 0
    return f"{prefix}{n + 1:03d}"


def save_config_proposal(proposal, summary_text, fingerprint, proposal_type="config_guardrail", expires_at=None, status="pending"):
    ensure_config_proposals_table()

    proposal_id = _next_config_proposal_id()
    created_at = utcnow_iso()
    payload = dict(proposal or {})
    payload["proposal_id"] = proposal_id
    payload["created_at"] = created_at

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO config_proposals (
            id,
            proposal_type,
            created_at,
            expires_at,
            status,
            fingerprint,
            proposal_json,
            summary_text,
            approved_at,
            rejected_at,
            applied_at,
            expired_at,
            superseded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL)
        """,
        (
            proposal_id,
            str(proposal_type or "config_guardrail"),
            created_at,
            str(expires_at or "").strip() or None,
            str(status or "pending"),
            str(fingerprint or "").strip(),
            json.dumps(payload, ensure_ascii=False),
            str(summary_text or "").strip(),
        ),
    )
    conn.commit()
    conn.close()
    return proposal_id


def get_config_proposal_by_id(proposal_id):
    ensure_config_proposals_table()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, proposal_type, created_at, expires_at, status, fingerprint,
               proposal_json, summary_text, approved_at, approved_by, rejected_at, rejected_by,
               applied_at, applied_by, expired_at, superseded_at
        FROM config_proposals
        WHERE id = ?
        LIMIT 1
        """,
        (str(proposal_id or "").strip(),),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return _normalize_config_proposal_row(row)


def _normalize_config_proposal_row(row):
    row = row if row else {}
    try:
        payload = json.loads(row["proposal_json"] or "{}")
    except Exception:
        payload = {}

    return {
        "id": row["id"],
        "proposal_type": row["proposal_type"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "status": row["status"],
        "fingerprint": row["fingerprint"],
        "proposal": payload if isinstance(payload, dict) else {},
        "summary_text": row["summary_text"],
        "approved_at": row["approved_at"],
        "approved_by": row["approved_by"],
        "rejected_at": row["rejected_at"],
        "rejected_by": row["rejected_by"],
        "applied_at": row["applied_at"],
        "applied_by": row["applied_by"],
        "expired_at": row["expired_at"],
        "superseded_at": row["superseded_at"],
    }


def get_latest_config_proposal_any_status(proposal_type="config_guardrail"):
    ensure_config_proposals_table()

    conn = get_conn()
    cur = conn.cursor()
    if proposal_type in (None, "", "all"):
        cur.execute(
            """
            SELECT id, proposal_type, created_at, expires_at, status, fingerprint,
                   proposal_json, summary_text, approved_at, approved_by, rejected_at, rejected_by,
                   applied_at, applied_by, expired_at, superseded_at
            FROM config_proposals
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """
        )
    else:
        cur.execute(
            """
            SELECT id, proposal_type, created_at, expires_at, status, fingerprint,
                   proposal_json, summary_text, approved_at, approved_by, rejected_at, rejected_by,
                   applied_at, applied_by, expired_at, superseded_at
            FROM config_proposals
            WHERE proposal_type = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (str(proposal_type or "config_guardrail"),),
        )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return _normalize_config_proposal_row(row)


def list_recent_config_proposals(limit=5, proposal_type="config_guardrail"):
    ensure_config_proposals_table()

    conn = get_conn()
    cur = conn.cursor()
    if proposal_type in (None, "", "all"):
        cur.execute(
            """
            SELECT id, proposal_type, created_at, expires_at, status, fingerprint,
                   proposal_json, summary_text, approved_at, approved_by, rejected_at, rejected_by,
                   applied_at, applied_by, expired_at, superseded_at
            FROM config_proposals
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (max(1, int(limit or 5)),),
        )
    else:
        cur.execute(
            """
            SELECT id, proposal_type, created_at, expires_at, status, fingerprint,
                   proposal_json, summary_text, approved_at, approved_by, rejected_at, rejected_by,
                   applied_at, applied_by, expired_at, superseded_at
            FROM config_proposals
            WHERE proposal_type = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (
                str(proposal_type or "config_guardrail"),
                max(1, int(limit or 5)),
            ),
        )
    rows = cur.fetchall()
    conn.close()

    return [_normalize_config_proposal_row(row) for row in rows or []]


def get_latest_pending_config_proposal(proposal_type="config_guardrail"):
    ensure_config_proposals_table()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM config_proposals
        WHERE status = 'pending'
          AND proposal_type = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (str(proposal_type or "config_guardrail"),),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return get_config_proposal_by_id(row["id"])


def find_pending_config_proposal_by_fingerprint(fingerprint, proposal_type="config_guardrail"):
    ensure_config_proposals_table()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM config_proposals
        WHERE status = 'pending'
          AND proposal_type = ?
          AND fingerprint = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (
            str(proposal_type or "config_guardrail"),
            str(fingerprint or "").strip(),
        ),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return get_config_proposal_by_id(row["id"])


def list_pending_config_proposals(proposal_type="config_guardrail"):
    ensure_config_proposals_table()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM config_proposals
        WHERE status = 'pending'
          AND proposal_type = ?
        ORDER BY created_at DESC, id DESC
        """,
        (str(proposal_type or "config_guardrail"),),
    )
    rows = cur.fetchall()
    conn.close()
    return [get_config_proposal_by_id(row["id"]) for row in rows if row and row["id"]]


def expire_pending_config_proposals(before_iso, proposal_type="config_guardrail"):
    ensure_config_proposals_table()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE config_proposals
        SET status = 'expired',
            expired_at = ?
        WHERE status = 'pending'
          AND proposal_type = ?
          AND expires_at IS NOT NULL
          AND expires_at <= ?
        """,
        (
            str(before_iso or "").strip() or utcnow_iso(),
            str(proposal_type or "config_guardrail"),
            str(before_iso or "").strip() or utcnow_iso(),
        ),
    )
    count = cur.rowcount or 0
    conn.commit()
    conn.close()
    return int(count)


def supersede_pending_config_proposals(proposal_type="config_guardrail", exclude_id=None):
    ensure_config_proposals_table()

    conn = get_conn()
    cur = conn.cursor()

    params = [utcnow_iso(), str(proposal_type or "config_guardrail")]
    sql = """
    UPDATE config_proposals
    SET status = 'superseded',
        superseded_at = ?
    WHERE status = 'pending'
      AND proposal_type = ?
    """

    if exclude_id:
        sql += " AND id != ?"
        params.append(str(exclude_id).strip())

    cur.execute(sql, tuple(params))
    count = cur.rowcount or 0
    conn.commit()
    conn.close()
    return int(count)


def set_config_proposal_status(proposal_id, status, timestamp_field, actor_field=None, actor=None, expected_current_status="pending"):
    ensure_config_proposals_table()

    allowed_timestamp_fields = {"approved_at", "rejected_at", "applied_at", "expired_at", "superseded_at"}
    allowed_actor_fields = {"approved_by", "rejected_by", "applied_by", None}

    if timestamp_field not in allowed_timestamp_fields:
        raise ValueError("invalid timestamp field")
    if actor_field not in allowed_actor_fields:
        raise ValueError("invalid actor field")

    timestamp = utcnow_iso()

    conn = get_conn()
    cur = conn.cursor()

    if actor_field:
        cur.execute(
            f"""
            UPDATE config_proposals
              SET status = ?,
                  {timestamp_field} = ?,
                  {actor_field} = ?
              WHERE id = ?
                AND status = ?
                AND (expires_at IS NULL OR expires_at > ?)
              """,
              (
                  str(status or "").strip(),
                  timestamp,
                  str(actor or "").strip() or None,
                  str(proposal_id or "").strip(),
                  str(expected_current_status or "pending").strip(),
                  timestamp,
              ),
          )
    else:
        cur.execute(
            f"""
              UPDATE config_proposals
              SET status = ?,
                  {timestamp_field} = ?
              WHERE id = ?
                AND status = ?
                AND (expires_at IS NULL OR expires_at > ?)
              """,
              (
                  str(status or "").strip(),
                  timestamp,
                  str(proposal_id or "").strip(),
                  str(expected_current_status or "pending").strip(),
                  timestamp,
              ),
          )

    updated = int(cur.rowcount or 0)
    conn.commit()
    conn.close()

    return {
        "ok": updated > 0,
        "updated": updated,
        "timestamp": timestamp,
    }


def insert_portfolio_snapshot(ts, total_value_usd, cash_value_usd=0.0, positions_value_usd=0.0):
    ensure_portfolio_history_table()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO portfolio_history (
            ts,
            total_value_usd,
            cash_value_usd,
            positions_value_usd
        )
        VALUES (?, ?, ?, ?)
        """,
        (
            int(ts or time.time()),
            float(total_value_usd or 0.0),
            float(cash_value_usd or 0.0),
            float(positions_value_usd or 0.0),
        ),
    )

    conn.commit()
    row_id = cur.lastrowid
    conn.close()
    return row_id


def get_portfolio_history_since(start_ts=None, limit=None):
    ensure_portfolio_history_table()

    conn = get_conn()
    cur = conn.cursor()

    where = []
    params = []

    if start_ts is not None:
        where.append("ts >= ?")
        params.append(int(start_ts))

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    if limit is not None:
        cur.execute(
            f"""
            SELECT ts, total_value_usd, cash_value_usd, positions_value_usd
            FROM (
                SELECT ts, total_value_usd, cash_value_usd, positions_value_usd
                FROM portfolio_history
                {where_sql}
                ORDER BY ts DESC, id DESC
                LIMIT ?
            )
            ORDER BY ts ASC
            """,
            params + [max(1, int(limit))],
        )
    else:
        cur.execute(
            f"""
            SELECT ts, total_value_usd, cash_value_usd, positions_value_usd
            FROM portfolio_history
            {where_sql}
            ORDER BY ts ASC, id ASC
            """,
            params,
        )

    rows = cur.fetchall()
    conn.close()

    return [
        {
            "ts": int(row["ts"] or 0),
            "total_value_usd": float(row["total_value_usd"] or 0.0),
            "cash_value_usd": float(row["cash_value_usd"] or 0.0),
            "positions_value_usd": float(row["positions_value_usd"] or 0.0),
        }
        for row in rows
    ]


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


def ensure_options_tracking_tables():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS options_executions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        ts INTEGER NOT NULL,
        proposal_id TEXT,
        underlying TEXT,
        strategy TEXT,
        broker TEXT,
        order_type TEXT,
        limit_price REAL NOT NULL DEFAULT 0,
        status TEXT,
        ok INTEGER NOT NULL DEFAULT 0,
        reason TEXT,
        order_id TEXT,
        reconnect_attempted INTEGER NOT NULL DEFAULT 0,
        connection_reused INTEGER NOT NULL DEFAULT 0,
        error TEXT,
        source TEXT,
        raw_json TEXT
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_options_executions_created
    ON options_executions (ts DESC, id DESC)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS options_orders (
        record_key TEXT PRIMARY KEY,
        broker_order_id TEXT,
        proposal_id TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        underlying TEXT,
        strategy TEXT,
        broker TEXT,
        asset_class TEXT,
        order_type TEXT,
        limit_price REAL NOT NULL DEFAULT 0,
        tif TEXT,
        status TEXT,
        source TEXT,
        contract_summary_json TEXT,
        legs_json TEXT,
        raw_json TEXT
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_options_orders_updated
    ON options_orders (updated_at DESC, broker_order_id DESC)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS options_positions (
        contract_key TEXT PRIMARY KEY,
        updated_at TEXT NOT NULL,
        broker TEXT,
        account TEXT,
        underlying TEXT,
        expiry TEXT,
        strike REAL NOT NULL DEFAULT 0,
        right_code TEXT,
        quantity REAL NOT NULL DEFAULT 0,
        side TEXT,
        avg_cost REAL NOT NULL DEFAULT 0,
        market_price REAL NOT NULL DEFAULT 0,
        market_value REAL NOT NULL DEFAULT 0,
        status TEXT,
        raw_json TEXT
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_options_positions_underlying
    ON options_positions (underlying, expiry, strike)
    """)

    conn.commit()
    conn.close()


def _normalize_options_execution_row(row):
    payload = {}
    try:
        payload = json.loads(row["raw_json"] or "{}")
    except Exception:
        payload = {}
    return {
        "id": int(row["id"] or 0),
        "created_at": row["created_at"],
        "ts": int(row["ts"] or 0),
        "proposal_id": str(row["proposal_id"] or "").strip(),
        "underlying": str(row["underlying"] or "").strip(),
        "strategy": str(row["strategy"] or "").strip(),
        "broker": str(row["broker"] or "").strip(),
        "order_type": str(row["order_type"] or "").strip(),
        "limit_price": float(row["limit_price"] or 0.0),
        "status": str(row["status"] or "").strip(),
        "ok": bool(int(row["ok"] or 0)),
        "reason": str(row["reason"] or "").strip(),
        "order_id": str(row["order_id"] or "").strip(),
        "reconnect_attempted": bool(int(row["reconnect_attempted"] or 0)),
        "connection_reused": bool(int(row["connection_reused"] or 0)),
        "error": str(row["error"] or "").strip(),
        "source": str(row["source"] or "").strip(),
        "raw": payload if isinstance(payload, dict) else {},
    }


def save_options_execution(record):
    ensure_options_tracking_tables()
    record = record if isinstance(record, dict) else {}
    ts = int(record.get("ts") or time.time())
    created_at = str(record.get("created_at") or "").strip() or utcnow_iso()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO options_executions (
            created_at, ts, proposal_id, underlying, strategy, broker, order_type,
            limit_price, status, ok, reason, order_id, reconnect_attempted,
            connection_reused, error, source, raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            created_at,
            ts,
            str(record.get("proposal_id") or "").strip() or None,
            str(record.get("underlying") or "").strip().upper() or None,
            str(record.get("strategy") or "").strip().lower() or None,
            str(record.get("broker") or "").strip().lower() or None,
            str(record.get("order_type") or "").strip().upper() or None,
            float(record.get("limit_price") or 0.0),
            str(record.get("status") or "").strip() or None,
            1 if bool(record.get("ok")) else 0,
            str(record.get("reason") or "").strip() or None,
            str(record.get("order_id") or "").strip() or None,
            1 if bool(record.get("reconnect_attempted")) else 0,
            1 if bool(record.get("connection_reused")) else 0,
            str(record.get("error") or "").strip() or None,
            str(record.get("source") or "").strip() or None,
            json.dumps(record, ensure_ascii=False),
        ),
    )
    conn.commit()
    row_id = cur.lastrowid
    conn.close()
    return int(row_id or 0)


def list_recent_options_executions(limit=10):
    ensure_options_tracking_tables()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, created_at, ts, proposal_id, underlying, strategy, broker, order_type,
               limit_price, status, ok, reason, order_id, reconnect_attempted,
               connection_reused, error, source, raw_json
        FROM options_executions
        ORDER BY ts DESC, id DESC
        LIMIT ?
        """,
        (max(1, int(limit or 10)),),
    )
    rows = cur.fetchall()
    conn.close()
    return [_normalize_options_execution_row(row) for row in rows or []]


def _options_order_record_key(record):
    record = record if isinstance(record, dict) else {}
    broker_order_id = str(record.get("broker_order_id") or record.get("order_id") or "").strip()
    if broker_order_id:
        return f"broker:{broker_order_id}"
    proposal_id = str(record.get("proposal_id") or "").strip()
    underlying = str(record.get("underlying") or "").strip().upper()
    strategy = str(record.get("strategy") or "").strip().lower()
    source = str(record.get("source") or "").strip().lower()
    created_at = str(record.get("created_at") or utcnow_iso()).strip()
    return f"proposal:{proposal_id}|{underlying}|{strategy}|{source}|{created_at}"


def save_options_order_record(item):
    ensure_options_tracking_tables()
    record = item if isinstance(item, dict) else {}
    created_at = str(record.get("created_at") or "").strip() or utcnow_iso()
    updated_at = str(record.get("updated_at") or "").strip() or created_at
    record_key = _options_order_record_key({**record, "created_at": created_at})
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO options_orders (
            record_key, broker_order_id, proposal_id, created_at, updated_at, underlying,
            strategy, broker, asset_class, order_type, limit_price, tif, status, source,
            contract_summary_json, legs_json, raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(record_key) DO UPDATE SET
            broker_order_id = excluded.broker_order_id,
            proposal_id = excluded.proposal_id,
            updated_at = excluded.updated_at,
            underlying = excluded.underlying,
            strategy = excluded.strategy,
            broker = excluded.broker,
            asset_class = excluded.asset_class,
            order_type = excluded.order_type,
            limit_price = excluded.limit_price,
            tif = excluded.tif,
            status = excluded.status,
            source = excluded.source,
            contract_summary_json = excluded.contract_summary_json,
            legs_json = excluded.legs_json,
            raw_json = excluded.raw_json
        """,
        (
            record_key,
            str(record.get("broker_order_id") or record.get("order_id") or "").strip() or None,
            str(record.get("proposal_id") or "").strip() or None,
            created_at,
            updated_at,
            str(record.get("underlying") or "").strip().upper() or None,
            str(record.get("strategy") or "").strip().lower() or None,
            str(record.get("broker") or "").strip().lower() or None,
            str(record.get("asset_class") or "option").strip().lower() or None,
            str(record.get("order_type") or "").strip().upper() or None,
            float(record.get("limit_price") or 0.0),
            str(record.get("tif") or "").strip().upper() or None,
            str(record.get("status") or "").strip() or None,
            str(record.get("source") or "").strip() or None,
            json.dumps(record.get("contract_summary") or {}, ensure_ascii=False),
            json.dumps(record.get("legs") or record.get("legs_summary") or [], ensure_ascii=False),
            json.dumps(record, ensure_ascii=False),
        ),
    )
    conn.commit()
    conn.close()
    return record_key


def list_recent_options_orders(limit=10):
    ensure_options_tracking_tables()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT record_key, broker_order_id, proposal_id, created_at, updated_at, underlying,
               strategy, broker, asset_class, order_type, limit_price, tif, status, source,
               contract_summary_json, legs_json, raw_json
        FROM options_orders
        ORDER BY updated_at DESC, created_at DESC
        LIMIT ?
        """,
        (max(1, int(limit or 10)),),
    )
    rows = cur.fetchall()
    conn.close()
    out = []
    for row in rows or []:
        try:
            contract_summary = json.loads(row["contract_summary_json"] or "{}")
        except Exception:
            contract_summary = {}
        try:
            legs = json.loads(row["legs_json"] or "[]")
        except Exception:
            legs = []
        try:
            raw = json.loads(row["raw_json"] or "{}")
        except Exception:
            raw = {}
        out.append(
            {
                "record_key": str(row["record_key"] or "").strip(),
                "broker_order_id": str(row["broker_order_id"] or "").strip(),
                "proposal_id": str(row["proposal_id"] or "").strip(),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "underlying": str(row["underlying"] or "").strip(),
                "strategy": str(row["strategy"] or "").strip(),
                "broker": str(row["broker"] or "").strip(),
                "asset_class": str(row["asset_class"] or "").strip(),
                "order_type": str(row["order_type"] or "").strip(),
                "limit_price": float(row["limit_price"] or 0.0),
                "tif": str(row["tif"] or "").strip(),
                "status": str(row["status"] or "").strip(),
                "source": str(row["source"] or "").strip(),
                "contract_summary": contract_summary if isinstance(contract_summary, dict) else {},
                "legs": legs if isinstance(legs, list) else [],
                "raw": raw if isinstance(raw, dict) else {},
            }
        )
    return out


def replace_options_positions_snapshot(positions):
    ensure_options_tracking_tables()
    rows = positions if isinstance(positions, list) else []
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM options_positions")
    for item in rows:
        item = item if isinstance(item, dict) else {}
        cur.execute(
            """
            INSERT INTO options_positions (
                contract_key, updated_at, broker, account, underlying, expiry, strike,
                right_code, quantity, side, avg_cost, market_price, market_value, status, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(item.get("contract_key") or "").strip(),
                str(item.get("updated_at") or utcnow_iso()).strip(),
                str(item.get("broker") or "").strip().lower() or None,
                str(item.get("account") or "").strip() or None,
                str(item.get("underlying") or "").strip().upper() or None,
                str(item.get("expiry") or "").strip() or None,
                float(item.get("strike") or 0.0),
                str(item.get("right_code") or item.get("right") or "").strip().upper() or None,
                float(item.get("quantity") or 0.0),
                str(item.get("side") or "").strip().upper() or None,
                float(item.get("avg_cost") or 0.0),
                float(item.get("market_price") or 0.0),
                float(item.get("market_value") or 0.0),
                str(item.get("status") or "").strip() or None,
                json.dumps(item, ensure_ascii=False),
            ),
        )
    conn.commit()
    conn.close()
    return len(rows)


def list_open_options_positions():
    ensure_options_tracking_tables()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT contract_key, updated_at, broker, account, underlying, expiry, strike,
               right_code, quantity, side, avg_cost, market_price, market_value, status, raw_json
        FROM options_positions
        ORDER BY underlying ASC, expiry ASC, strike ASC
        """
    )
    rows = cur.fetchall()
    conn.close()
    out = []
    for row in rows or []:
        try:
            raw = json.loads(row["raw_json"] or "{}")
        except Exception:
            raw = {}
        out.append(
            {
                "contract_key": str(row["contract_key"] or "").strip(),
                "updated_at": row["updated_at"],
                "broker": str(row["broker"] or "").strip(),
                "account": str(row["account"] or "").strip(),
                "underlying": str(row["underlying"] or "").strip(),
                "expiry": str(row["expiry"] or "").strip(),
                "strike": float(row["strike"] or 0.0),
                "right_code": str(row["right_code"] or "").strip(),
                "quantity": float(row["quantity"] or 0.0),
                "side": str(row["side"] or "").strip(),
                "avg_cost": float(row["avg_cost"] or 0.0),
                "market_price": float(row["market_price"] or 0.0),
                "market_value": float(row["market_value"] or 0.0),
                "status": str(row["status"] or "").strip(),
                "raw": raw if isinstance(raw, dict) else {},
            }
        )
    return out
