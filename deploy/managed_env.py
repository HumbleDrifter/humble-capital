import argparse
import json
import os
import shlex
import time
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except Exception:  # pragma: no cover
    tomllib = None


@dataclass(frozen=True)
class ManagedField:
    key: str
    group: str
    label: str
    deployed: bool = True
    secret: bool = False
    required_when: str = ""


FIELD_REGISTRY = [
    ManagedField("COINBASE_API_KEY", "secrets", "Coinbase API Key", secret=True),
    ManagedField("COINBASE_API_SECRET", "secrets", "Coinbase API Secret", secret=True),
    ManagedField("WEBHOOK_SHARED_SECRET", "secrets", "Webhook Shared Secret", secret=True),
    ManagedField("WEBHOOK_SHARED_SECRETS", "secrets", "Webhook Shared Secret List", secret=True),
    ManagedField("INTERNAL_API_SECRET", "secrets", "Internal API Secret", secret=True),
    ManagedField("STATUS_SECRET", "secrets", "Status Secret", secret=True),
    ManagedField("APP_SESSION_SECRET", "secrets", "App Session Secret", secret=True),
    ManagedField("TELEGRAM_BOT_TOKEN", "secrets", "Telegram Bot Token", secret=True),
    ManagedField("TELEGRAM_CHAT_ID", "secrets", "Telegram Chat Id", secret=True),
    ManagedField("IBKR_ENABLED", "runtime", "IBKR Enabled"),
    ManagedField("IBKR_PAPER_TRADING", "runtime", "IBKR Paper Trading"),
    ManagedField("IBKR_HOST", "runtime", "IBKR Host", required_when="IBKR_ENABLED"),
    ManagedField("IBKR_PORT_PAPER", "runtime", "IBKR Paper Port", required_when="IBKR_ENABLED"),
    ManagedField("IBKR_PORT_LIVE", "runtime", "IBKR Live Port", required_when="IBKR_ENABLED"),
    ManagedField("IBKR_CLIENT_ID", "runtime", "IBKR Client Id", required_when="IBKR_ENABLED"),
    ManagedField("IBKR_ACCOUNT", "runtime", "IBKR Account"),
    ManagedField("IBKR_ALLOW_LIVE_OPTIONS", "runtime", "IBKR Allow Live Options"),
    ManagedField("IBKR_OPTIONS_EXCHANGE", "runtime", "IBKR Options Exchange"),
    ManagedField("IBKR_OPTIONS_CURRENCY", "runtime", "IBKR Options Currency"),
    ManagedField("OPTIONS_ENABLED", "runtime", "Options Enabled"),
    ManagedField("OPTIONS_REQUIRE_APPROVAL", "runtime", "Options Require Approval"),
    ManagedField("OPTIONS_PAPER_ONLY", "runtime", "Options Paper Only"),
    ManagedField("OPTIONS_ALLOWED_UNDERLYINGS", "runtime", "Options Allowed Underlyings", required_when="OPTIONS_ENABLED"),
    ManagedField("OPTIONS_MIN_DTE", "runtime", "Options Min DTE", required_when="OPTIONS_ENABLED"),
    ManagedField("OPTIONS_MAX_DTE", "runtime", "Options Max DTE", required_when="OPTIONS_ENABLED"),
    ManagedField("OPTIONS_MAX_CONTRACTS", "runtime", "Options Max Contracts", required_when="OPTIONS_ENABLED"),
    ManagedField("OPTIONS_MAX_PREMIUM_USD", "runtime", "Options Max Premium USD", required_when="OPTIONS_ENABLED"),
    ManagedField("OPTIONS_ALLOW_0DTE", "runtime", "Options Allow 0DTE"),
    ManagedField("REMOTE_HOST", "deployment_target", "Remote Host", deployed=False),
    ManagedField("REMOTE_PORT", "deployment_target", "Remote Port", deployed=False),
    ManagedField("REMOTE_USER", "deployment_target", "Remote User", deployed=False),
    ManagedField("REMOTE_ENV_PATH", "deployment_target", "Remote Env Path", deployed=False),
    ManagedField("REMOTE_SERVICE_NAME", "deployment_target", "Remote Service Name", deployed=False),
    ManagedField("REMOTE_WORKER_SERVICE_NAME", "deployment_target", "Remote Worker Service Name", deployed=False),
    ManagedField("SSH_KEY_PATH", "deployment_target", "SSH Key Path", deployed=False),
    ManagedField("FORCE_CLEAR_BLANKS", "deployment_target", "Force Clear Blanks", deployed=False),
    ManagedField("DEPLOY_SSH_PASSWORD", "deployment_secrets", "Deploy SSH Password", deployed=False, secret=True),
]

FIELD_MAP = {field.key: field for field in FIELD_REGISTRY}
DEPLOYED_FIELD_KEYS = {field.key for field in FIELD_REGISTRY if field.deployed}


def _stringify(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _is_blank(value):
    return str(value or "").strip() == ""


def _is_truthy(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _safe_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _csv_items(value):
    return [part.strip().upper() for part in str(value or "").split(",") if part.strip()]


def _mask_value(key, value):
    field = FIELD_MAP.get(key)
    if field and field.secret:
        return "***MASKED***" if not _is_blank(value) else ""
    return _stringify(value)


def parse_env_text(text):
    values = {}
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = raw_line.split("=", 1)
        values[str(key).strip()] = value.strip()
    return values


def build_env_text(values):
    env_values = values if isinstance(values, dict) else {}
    lines = [
        "# Generated by deploy/managed_env.py",
        "# Blank local values are skipped by default to avoid wiping working remote config.",
        "# Use --force-clear or FORCE_CLEAR_BLANKS=true only when you explicitly intend to clear remote values.",
        "",
    ]
    for group_name in ["secrets", "runtime"]:
        group_fields = [field for field in FIELD_REGISTRY if field.group == group_name and field.deployed]
        if not group_fields:
            continue
        lines.append(f"# [{group_name}]")
        for field in group_fields:
            if field.key in env_values:
                lines.append(f"{field.key}={_stringify(env_values.get(field.key, ''))}")
        lines.append("")

    remaining_keys = sorted(
        key for key in env_values.keys()
        if key not in {field.key for field in FIELD_REGISTRY if field.deployed}
    )
    if remaining_keys:
        lines.append("# [preserved_unmanaged]")
        for key in remaining_keys:
            lines.append(f"{key}={_stringify(env_values.get(key, ''))}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def load_managed_config(path):
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"managed config not found: {config_path}")
    if tomllib is None:
        raise RuntimeError("tomllib is required to load managed env config")

    data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    flat = {}
    for section_name, section_values in data.items():
        if not isinstance(section_values, dict):
            continue
        for key, value in section_values.items():
            flat[str(key).strip()] = _stringify(value)
    return {
        "raw": data,
        "flat": flat,
    }


def merge_managed_env(local_flat, remote_flat, force_clear=False):
    local_flat = dict(local_flat or {})
    remote_flat = dict(remote_flat or {})
    merged = dict(remote_flat)
    preview = {
        "added": [],
        "changed": [],
        "unchanged": [],
        "blank_skipped": [],
        "blank_cleared": [],
        "preserved_remote": [],
    }

    deployed_fields = [field for field in FIELD_REGISTRY if field.deployed]
    for field in deployed_fields:
        key = field.key
        local_value = _stringify(local_flat.get(key, ""))
        remote_value = _stringify(remote_flat.get(key, ""))

        if _is_blank(local_value):
            if force_clear:
                merged[key] = ""
                if not _is_blank(remote_value):
                    preview["blank_cleared"].append(key)
                else:
                    preview["blank_cleared"].append(key)
            else:
                if not _is_blank(remote_value):
                    merged[key] = remote_value
                    preview["blank_skipped"].append(key)
                    preview["preserved_remote"].append(key)
                else:
                    merged.pop(key, None)
                    preview["blank_skipped"].append(key)
            continue

        merged[key] = local_value
        if key not in remote_flat or _is_blank(remote_value):
            preview["added"].append(key)
        elif remote_value != local_value:
            preview["changed"].append(key)
        else:
            preview["unchanged"].append(key)

    return {
        "merged": merged,
        "preview": preview,
    }


def validate_managed_env(local_flat, merged_env, *, deploying=False):
    local_flat = dict(local_flat or {})
    merged_env = dict(merged_env or {})
    errors = []

    if _is_truthy(merged_env.get("IBKR_ENABLED")):
        if _is_blank(merged_env.get("IBKR_HOST")):
            errors.append("IBKR_ENABLED=true requires IBKR_HOST")
        if _is_blank(merged_env.get("IBKR_CLIENT_ID")):
            errors.append("IBKR_ENABLED=true requires IBKR_CLIENT_ID")
        if _is_truthy(merged_env.get("IBKR_PAPER_TRADING")):
            if _is_blank(merged_env.get("IBKR_PORT_PAPER")):
                errors.append("IBKR_ENABLED=true with IBKR_PAPER_TRADING=true requires IBKR_PORT_PAPER")
        elif _is_blank(merged_env.get("IBKR_PORT_LIVE")):
            errors.append("IBKR_ENABLED=true with IBKR_PAPER_TRADING=false requires IBKR_PORT_LIVE")
        if _is_blank(merged_env.get("IBKR_PORT_PAPER")) and _is_blank(merged_env.get("IBKR_PORT_LIVE")):
            errors.append("IBKR_ENABLED=true requires at least one IBKR paper/live port")
        if not _is_blank(merged_env.get("IBKR_PORT_PAPER")) and _safe_int(merged_env.get("IBKR_PORT_PAPER"), 0) <= 0:
            errors.append("IBKR_PORT_PAPER must be > 0 when set")
        if not _is_blank(merged_env.get("IBKR_PORT_LIVE")) and _safe_int(merged_env.get("IBKR_PORT_LIVE"), 0) <= 0:
            errors.append("IBKR_PORT_LIVE must be > 0 when set")
        if _safe_int(merged_env.get("IBKR_CLIENT_ID"), 0) <= 0:
            errors.append("IBKR_CLIENT_ID must be > 0 when IBKR is enabled")

    if _is_truthy(merged_env.get("OPTIONS_ENABLED")):
        if not _is_truthy(merged_env.get("IBKR_ENABLED")):
            errors.append("OPTIONS_ENABLED=true requires IBKR_ENABLED=true")
        allowed = str(merged_env.get("OPTIONS_ALLOWED_UNDERLYINGS") or "").strip()
        min_dte = _safe_int(merged_env.get("OPTIONS_MIN_DTE"), -1)
        max_dte = _safe_int(merged_env.get("OPTIONS_MAX_DTE"), -1)
        max_contracts = _safe_int(merged_env.get("OPTIONS_MAX_CONTRACTS"), 0)
        max_premium = _safe_float(merged_env.get("OPTIONS_MAX_PREMIUM_USD"), 0.0)

        if _is_blank(allowed) or not _csv_items(allowed):
            errors.append("OPTIONS_ENABLED=true requires OPTIONS_ALLOWED_UNDERLYINGS")
        if min_dte < 0:
            errors.append("OPTIONS_ENABLED=true requires OPTIONS_MIN_DTE >= 0")
        if max_dte < 0:
            errors.append("OPTIONS_ENABLED=true requires OPTIONS_MAX_DTE >= 0")
        if max_dte >= 0 and min_dte >= 0 and max_dte < min_dte:
            errors.append("OPTIONS_MAX_DTE must be >= OPTIONS_MIN_DTE")
        if max_contracts <= 0:
            errors.append("OPTIONS_ENABLED=true requires OPTIONS_MAX_CONTRACTS > 0")
        if max_premium <= 0:
            errors.append("OPTIONS_ENABLED=true requires OPTIONS_MAX_PREMIUM_USD > 0")
        if not _is_truthy(merged_env.get("OPTIONS_PAPER_ONLY")) and not _is_truthy(merged_env.get("IBKR_ALLOW_LIVE_OPTIONS")):
            errors.append("OPTIONS_PAPER_ONLY=false requires IBKR_ALLOW_LIVE_OPTIONS=true")

    if deploying:
        if _is_blank(local_flat.get("REMOTE_HOST")):
            errors.append("deployment requires REMOTE_HOST")
        if _is_blank(local_flat.get("REMOTE_USER")):
            errors.append("deployment requires REMOTE_USER")
        if _is_blank(local_flat.get("REMOTE_ENV_PATH")):
            errors.append("deployment requires REMOTE_ENV_PATH")

    return {
        "ok": not errors,
        "errors": errors,
    }


def build_preview(local_flat, remote_flat, merged_env, preview):
    local_flat = dict(local_flat or {})
    remote_flat = dict(remote_flat or {})
    merged_env = dict(merged_env or {})
    preview = dict(preview or {})

    def _rows(keys):
        return [
            {
                "key": key,
                "group": FIELD_MAP[key].group if key in FIELD_MAP else "unmanaged",
                "local": _mask_value(key, local_flat.get(key, "")),
                "remote": _mask_value(key, remote_flat.get(key, "")),
                "merged": _mask_value(key, merged_env.get(key, "")),
            }
            for key in keys
        ]

    return {
        "summary": {
            "added": len(preview.get("added", [])),
            "changed": len(preview.get("changed", [])),
            "unchanged": len(preview.get("unchanged", [])),
            "blank_skipped": len(preview.get("blank_skipped", [])),
            "blank_cleared": len(preview.get("blank_cleared", [])),
            "preserved_remote": len(preview.get("preserved_remote", [])),
        },
        "added": _rows(preview.get("added", [])),
        "changed": _rows(preview.get("changed", [])),
        "unchanged": _rows(preview.get("unchanged", [])),
        "blank_skipped": _rows(preview.get("blank_skipped", [])),
        "blank_cleared": _rows(preview.get("blank_cleared", [])),
        "preserved_remote": _rows(preview.get("preserved_remote", [])),
    }


def verify_remote_env_keys(remote_text, expected_keys):
    remote_values = parse_env_text(remote_text)
    keys = [key for key in expected_keys if key]
    present = sorted([key for key in keys if key in remote_values])
    missing = sorted([key for key in keys if key not in remote_values])
    return {
        "ok": not missing,
        "present": present,
        "missing": missing,
    }


def _ssh_client(local_flat):
    try:
        import paramiko
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"paramiko is required for deploy workflow: {exc}") from exc

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    connect_kwargs = {
        "hostname": str(local_flat.get("REMOTE_HOST") or "").strip(),
        "port": _safe_int(local_flat.get("REMOTE_PORT"), 22),
        "username": str(local_flat.get("REMOTE_USER") or "").strip(),
        "timeout": 20,
    }
    password = str(local_flat.get("DEPLOY_SSH_PASSWORD") or "").strip()
    key_path = str(local_flat.get("SSH_KEY_PATH") or "").strip()
    if key_path:
        connect_kwargs["key_filename"] = key_path
    if password:
        connect_kwargs["password"] = password
    client.connect(**connect_kwargs)
    return client


def _read_remote_env(client, remote_env_path):
    sftp = client.open_sftp()
    try:
        try:
            with sftp.open(remote_env_path, "r") as handle:
                return handle.read().decode("utf-8")
        except FileNotFoundError:
            return ""
        except OSError:
            return ""
    finally:
        sftp.close()


def _write_remote_env(client, remote_env_path, env_text):
    sftp = client.open_sftp()
    tmp_path = f"{remote_env_path}.tmp.{os.getpid()}.{int(time.time())}"
    try:
        with sftp.open(tmp_path, "w") as handle:
            handle.write(env_text)
            handle.flush()
        try:
            posix_rename = getattr(sftp, "posix_rename", None)
            if callable(posix_rename):
                posix_rename(tmp_path, remote_env_path)
            else:
                raise OSError("posix_rename unavailable")
        except Exception:
            try:
                sftp.remove(remote_env_path)
            except FileNotFoundError:
                pass
            except OSError as exc:
                message = str(exc or "").strip().lower()
                if "no such file" not in message and "not found" not in message:
                    raise
            sftp.rename(tmp_path, remote_env_path)
    except Exception:
        try:
            sftp.remove(tmp_path)
        except Exception:
            pass
        raise
    finally:
        sftp.close()


def _run_remote_command(client, command):
    stdin, stdout, stderr = client.exec_command(command)
    exit_code = stdout.channel.recv_exit_status()
    return {
        "command": command,
        "exit_code": exit_code,
        "stdout": stdout.read().decode("utf-8", errors="ignore").strip(),
        "stderr": stderr.read().decode("utf-8", errors="ignore").strip(),
        "ok": exit_code == 0,
    }


def deploy_managed_env(config_path, *, force_clear=False, restart_services=True):
    loaded = load_managed_config(config_path)
    local_flat = loaded["flat"]
    effective_force_clear = bool(force_clear or _is_truthy(local_flat.get("FORCE_CLEAR_BLANKS")))
    validation = validate_managed_env(local_flat, local_flat, deploying=True)
    if not validation["ok"]:
        return {"ok": False, "stage": "local_validation", "errors": validation["errors"]}

    client = _ssh_client(local_flat)
    remote_env_path = str(local_flat.get("REMOTE_ENV_PATH") or "/root/tradingbot/.env").strip()
    try:
        remote_text = _read_remote_env(client, remote_env_path)
        remote_flat = parse_env_text(remote_text)
        merged = merge_managed_env(local_flat, remote_flat, force_clear=effective_force_clear)
        validation = validate_managed_env(local_flat, merged["merged"], deploying=True)
        if not validation["ok"]:
            return {"ok": False, "stage": "merged_validation", "errors": validation["errors"]}

        env_text = build_env_text(merged["merged"])
        _write_remote_env(client, remote_env_path, env_text)

        restart_results = []
        active_checks = []
        if restart_services:
            service_names = [
                str(local_flat.get("REMOTE_SERVICE_NAME") or "").strip(),
                str(local_flat.get("REMOTE_WORKER_SERVICE_NAME") or "").strip(),
            ]
            for service_name in [name for name in service_names if name]:
                restart_results.append(
                    _run_remote_command(client, f"systemctl restart {shlex.quote(service_name)}")
                )
                active_checks.append(
                    _run_remote_command(client, f"systemctl is-active {shlex.quote(service_name)}")
                )

        verify_text = _read_remote_env(client, remote_env_path)
        expected_keys = sorted(key for key in merged["merged"].keys() if key in DEPLOYED_FIELD_KEYS)
        verification = verify_remote_env_keys(verify_text, expected_keys)
        preview = build_preview(local_flat, remote_flat, merged["merged"], merged["preview"])
        return {
            "ok": verification["ok"] and all(item["ok"] for item in restart_results) and all(item["ok"] for item in active_checks),
            "remote_env_path": remote_env_path,
            "force_clear": effective_force_clear,
            "preview": preview,
            "verification": verification,
            "restart_results": restart_results,
            "service_status_checks": active_checks,
        }
    finally:
        client.close()


def _cli_preview(args):
    loaded = load_managed_config(args.config)
    local_flat = loaded["flat"]
    effective_force_clear = bool(args.force_clear or _is_truthy(local_flat.get("FORCE_CLEAR_BLANKS")))
    remote_flat = {}
    if args.remote_env and Path(args.remote_env).exists():
        remote_flat = parse_env_text(Path(args.remote_env).read_text(encoding="utf-8"))
    merged = merge_managed_env(local_flat, remote_flat, force_clear=effective_force_clear)
    validation = validate_managed_env(local_flat, merged["merged"], deploying=False)
    preview = build_preview(local_flat, remote_flat, merged["merged"], merged["preview"])
    print(json.dumps({"validation": validation, "force_clear": effective_force_clear, "preview": preview}, indent=2))


def _cli_deploy(args):
    result = deploy_managed_env(args.config, force_clear=args.force_clear, restart_services=not args.skip_restart)
    print(json.dumps(result, indent=2))
    if not result.get("ok"):
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description="Managed env preview/deploy helper for tradingbot.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    preview_parser = subparsers.add_parser("preview", help="Preview merged env changes without deploying.")
    preview_parser.add_argument("--config", required=True, help="Path to managed TOML config.")
    preview_parser.add_argument("--remote-env", help="Optional current env file for preview diff.")
    preview_parser.add_argument("--force-clear", action="store_true", help="Explicitly clear blank local values.")
    preview_parser.set_defaults(func=_cli_preview)

    deploy_parser = subparsers.add_parser("deploy", help="Deploy merged env to remote server.")
    deploy_parser.add_argument("--config", required=True, help="Path to managed TOML config.")
    deploy_parser.add_argument("--force-clear", action="store_true", help="Explicitly clear blank local values.")
    deploy_parser.add_argument("--skip-restart", action="store_true", help="Skip remote service restarts.")
    deploy_parser.set_defaults(func=_cli_deploy)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
