import sqlite3
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
