import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv("/root/tradingbot/.env", override=True)

from storage import get_db_path

BASE_DIR = os.getenv("TRADINGBOT_BASE_DIR", "/root/tradingbot").strip() or "/root/tradingbot"
DATA_DIR = os.path.join(BASE_DIR, "advisor_export")


def utcnow_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def ensure_data_dir():
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)


def atomic_write_json(path: str, payload):
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    os.replace(tmp_path, path)


def export_portfolio():
    from portfolio import get_portfolio_snapshot

    snapshot = get_portfolio_snapshot() or {}
    snapshot["exported_at"] = utcnow_iso()
    atomic_write_json(os.path.join(DATA_DIR, "portfolio_snapshot.json"), snapshot)


def export_config():
    from portfolio import load_asset_config

    config = load_asset_config() or {}
    config["_exported_at"] = utcnow_iso()
    atomic_write_json(os.path.join(DATA_DIR, "risk_config.json"), config)


def export_trades():
    db_path = get_db_path()

    if not os.path.exists(db_path):
        atomic_write_json(
            os.path.join(DATA_DIR, "recent_trades.json"),
            {
                "ok": False,
                "error": "Database not found",
                "db_path": db_path,
                "exported_at": utcnow_iso(),
            },
        )
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        result = {
            "ok": True,
            "source_db": db_path,
            "exported_at": utcnow_iso(),
            "recent_positions": [],
            "recent_realized_pnl": [],
        }

        try:
            position_rows = conn.execute(
                """
                SELECT product_id, base_qty_total, base_qty_liquid, base_qty_locked, updated_at
                FROM positions
                ORDER BY updated_at DESC
                LIMIT 100
                """
            ).fetchall()
            result["recent_positions"] = [dict(row) for row in position_rows]
        except sqlite3.Error as e:
            result["positions_error"] = str(e)

        try:
            realized_rows = conn.execute(
                """
                SELECT id, product_id, pnl_usd, created_at
                FROM realized_pnl
                ORDER BY created_at DESC, id DESC
                LIMIT 100
                """
            ).fetchall()
            result["recent_realized_pnl"] = [dict(row) for row in realized_rows]
        except sqlite3.Error as e:
            result["realized_pnl_error"] = str(e)

        result["positions_count"] = len(result["recent_positions"])
        result["realized_pnl_count"] = len(result["recent_realized_pnl"])

        atomic_write_json(os.path.join(DATA_DIR, "recent_trades.json"), result)

    finally:
        conn.close()


def export_system_status():
    status = {
        "time": utcnow_iso(),
        "trading_enabled": os.getenv("TRADING_ENABLED", "false"),
        "db_path": get_db_path(),
    }
    atomic_write_json(os.path.join(DATA_DIR, "system_status.json"), status)


if __name__ == "__main__":
    ensure_data_dir()
    export_portfolio()
    export_config()
    export_trades()
    export_system_status()
