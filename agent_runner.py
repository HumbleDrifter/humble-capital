"""
Standalone APEX agent runner — runs as separate process from gunicorn.
"""
import time
import os
import sys
from datetime import datetime, timezone, timedelta

# Load environment variables first
def load_env():
    env_path = "/root/tradingbot/.env"
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

load_env()
print("[agent_runner] APEX agent process started", flush=True)
print(f"[agent_runner] API key set: {bool(os.environ.get('ANTHROPIC_API_KEY'))}", flush=True)

# Add tradingbot to path
sys.path.insert(0, "/root/tradingbot")

while True:
    try:
        load_env()  # reload on each cycle in case keys changed
        import json
        with open("/root/tradingbot/asset_config.json") as f:
            cfg = json.load(f)
        agent_cfg = cfg.get("agent", {})

        if not agent_cfg.get("enabled", False):
            print("[agent_runner] agent disabled — sleeping 60s", flush=True)
            time.sleep(60)
            continue

        et_offset = timedelta(hours=-4)
        now = datetime.now(timezone(et_offset))

        is_market = (
            now.weekday() < 5 and
            (now.hour > 9 or (now.hour == 9 and now.minute >= 30)) and
            now.hour < 16
        )
        mode = "full" if is_market else "crypto"
        mins = int(agent_cfg.get("schedule_minutes", 30))
        sleep_mins = mins if is_market else max(mins * 2, 60)

        print(f"[agent_runner] running {mode} cycle", flush=True)
        from agent import run_agent_cycle
        run_agent_cycle(mode=mode)
        print(f"[agent_runner] cycle complete, sleeping {sleep_mins}min", flush=True)
        time.sleep(sleep_mins * 60)

    except Exception as e:
        print(f"[agent_runner] error: {e}", flush=True)
        time.sleep(60)
