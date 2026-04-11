import getpass
import os
import sys
from typing import Dict, List, Tuple


def _project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _env_path() -> str:
    return os.path.join(_project_root(), ".env")


def _read_env_lines(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        return handle.readlines()


def _read_env_map(path: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for raw_line in _read_env_lines(path):
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _update_env_file(path: str, updates: Dict[str, str]) -> None:
    lines = _read_env_lines(path)
    seen = set()
    out: List[str] = []

    for raw_line in lines:
        line = raw_line.rstrip("\n")
        if "=" not in line or line.lstrip().startswith("#"):
            out.append(raw_line if raw_line.endswith("\n") else f"{raw_line}\n")
            continue

        key, _value = line.split("=", 1)
        key = key.strip()
        if key in updates:
            out.append(f"{key}={updates[key]}\n")
            seen.add(key)
        else:
            out.append(raw_line if raw_line.endswith("\n") else f"{raw_line}\n")

    if out and out[-1].strip():
        out.append("\n")

    for key, value in updates.items():
        if key in seen:
            continue
        out.append(f"{key}={value}\n")

    with open(path, "w", encoding="utf-8") as handle:
        handle.writelines(out)


def _prompt_with_default(label: str, default: str = "", secret: bool = False) -> str:
    prompt = f"{label}"
    if default:
        prompt += f" [{default}]"
    prompt += ": "

    if secret:
        value = getpass.getpass(prompt)
    else:
        value = input(prompt)

    value = value.strip()
    return value or default


def _to_dict(value):
    return value.to_dict() if hasattr(value, "to_dict") else value


def _call_first(obj, method_names: List[str], *args, **kwargs):
    last_error = None
    for name in method_names:
        method = getattr(obj, name, None)
        if not callable(method):
            continue
        try:
            return method(*args, **kwargs)
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise AttributeError(f"No callable methods found: {method_names}")


def _extract_access_token(result) -> str:
    if isinstance(result, dict):
        return str(result.get("accessToken") or result.get("access_token") or "").strip()
    return ""


def _device_id_from_client(client) -> str:
    for attr in ("_did", "did", "device_id"):
        try:
            value = str(getattr(client, attr, "") or "").strip()
        except Exception:
            value = ""
        if value:
            return value
    return ""


def _unlock_trading(client, pin: str) -> Tuple[bool, str]:
    if not pin:
        return False, "no_pin_provided"

    for method_name in ("get_trade_token", "trade_token", "unlock_trade"):
        method = getattr(client, method_name, None)
        if not callable(method):
            continue
        try:
            method(pin)
            return True, method_name
        except Exception as exc:
            last_error = str(exc)
            continue
    return False, last_error if "last_error" in locals() else "trade_token_method_unavailable"


def _switch_to_paper(client) -> Tuple[bool, str]:
    for method_name in ("switch_to_paper", "set_paper_account", "paper_webull"):
        method = getattr(client, method_name, None)
        if not callable(method):
            continue
        try:
            result = method()
            return True, str(result or method_name)
        except Exception as exc:
            last_error = str(exc)
            continue
    return False, last_error if "last_error" in locals() else "paper_mode_method_unavailable"


def _fetch_account_probe(client) -> Tuple[dict, list]:
    account = {}
    positions = []

    try:
        account = _to_dict(_call_first(client, ["get_account", "get_account_info", "get_portfolio"]))
        if not isinstance(account, dict):
            account = {}
    except Exception:
        account = {}

    try:
        raw_positions = _call_first(client, ["get_positions", "get_position"])
        if isinstance(raw_positions, list):
            positions = [_to_dict(row) for row in raw_positions]
    except Exception:
        positions = []

    return account, positions


def main():
    print("=" * 60)
    print("  Humble Capital - Webull Setup Wizard")
    print("=" * 60)
    print()

    try:
        from webull import webull
        print("[1/7] Webull SDK detected")
    except ImportError:
        print("[1/7] Webull SDK not installed.")
        print("Install it with: pip3 install webull")
        sys.exit(1)

    env_path = _env_path()
    env_map = _read_env_map(env_path)

    email = _prompt_with_default("Webull email", env_map.get("WEBULL_EMAIL", ""))
    password = _prompt_with_default("Webull password", env_map.get("WEBULL_PASSWORD", ""), secret=True)
    pin = _prompt_with_default("Trading PIN (6 digits)", env_map.get("WEBULL_TRADING_PIN", ""), secret=True)

    if not email or not password or not pin:
        print("All fields are required.")
        sys.exit(1)

    existing_did = env_map.get("WEBULL_DEVICE_ID", "").strip()
    print()
    print("[2/7] Preparing Webull session...")

    wb = webull()
    if existing_did:
        print(f"Found existing device ID: {existing_did[:8]}...")
        for attr in ("_did", "did", "device_id"):
            try:
                setattr(wb, attr, existing_did)
                break
            except Exception:
                continue

    print()
    print("[3/7] Attempting login...")
    login_result = None
    try:
        login_result = wb.login(email, password)
    except Exception as exc:
        print(f"Initial login attempt: {exc}")

    if not _extract_access_token(login_result):
        print()
        print("MFA verification required. Requesting code...")
        try:
            wb.get_mfa(email)
            print("MFA code sent to your email/phone.")
        except Exception as exc:
            print(f"MFA request note: {exc}")
            print("Check your Webull app or inbox for the verification code.")

        mfa_code = input("Enter MFA code: ").strip()
        if not mfa_code:
            print("MFA code is required.")
            sys.exit(1)

        try:
            login_result = wb.login(email, password, mfa=mfa_code)
        except Exception as exc:
            print(f"Login failed: {exc}")
            sys.exit(1)

    if not _extract_access_token(login_result):
        print(f"Login failed. Response: {login_result}")
        sys.exit(1)

    print("Login successful.")
    device_id = _device_id_from_client(wb)
    if device_id:
        print(f"Captured device ID: {device_id[:8]}...")
    else:
        print("No device ID was captured from the session.")

    print()
    print("[4/7] Unlocking trading + paper mode...")
    unlocked, unlock_note = _unlock_trading(wb, pin)
    if unlocked:
        print(f"Trading token unlocked via {unlock_note}.")
    else:
        print(f"Trading token note: {unlock_note}")

    paper_enabled = True
    paper_ok, paper_note = _switch_to_paper(wb)
    if paper_ok:
        print("Paper trading mode enabled.")
    else:
        print(f"Paper trading note: {paper_note} (continuing)")

    print()
    print("[5/7] Testing account connection...")
    account_info, positions = _fetch_account_probe(wb)
    balance = float(
        account_info.get("netLiquidation")
        or account_info.get("netLiquidationValue")
        or account_info.get("balance")
        or account_info.get("totalValue")
        or 0.0
    )
    buying_power = float(
        account_info.get("buyingPower")
        or account_info.get("buying_power")
        or account_info.get("cashBalance")
        or account_info.get("cash")
        or 0.0
    )
    print(f"Account probe complete. Buying power: ${buying_power:,.2f} | Balance: ${balance:,.2f}")
    print(f"Positions returned: {len(positions)}")

    print()
    print("[6/7] Updating .env...")
    env_updates = {
        "WEBULL_ENABLED": "true",
        "WEBULL_EMAIL": email,
        "WEBULL_PASSWORD": password,
        "WEBULL_DEVICE_ID": device_id,
        "WEBULL_TRADING_PIN": pin,
        "WEBULL_PAPER_TRADING": "true" if paper_enabled else "false",
    }
    _update_env_file(env_path, env_updates)
    for key, value in env_updates.items():
        os.environ[key] = value
    print(f"Updated {env_path}")

    print()
    print("[7/7] Testing option chain fetcher with AAPL...")
    chain_ok = False
    chain_error = ""
    expirations = []
    contract_count = 0
    try:
        from options.chain_fetcher import OptionChainFetcher

        fetcher = OptionChainFetcher(broker="webull")
        chain = fetcher.get_chain("AAPL")
        chain_ok = bool(chain.get("ok"))
        if chain_ok:
            expirations = list(chain.get("expirations") or [])
            for exp in expirations[:1]:
                data = (chain.get("chains") or {}).get(exp) or {}
                contract_count += len(data.get("calls") or []) + len(data.get("puts") or [])
            print(
                f"Option chain fetched successfully. Expirations: {len(expirations)} | "
                f"Contracts in first chain: {contract_count}"
            )
        else:
            chain_error = str(chain.get("error") or "unknown_error")
            print(f"Option chain fetch failed: {chain_error}")
    except Exception as exc:
        chain_error = str(exc)
        print(f"Option chain fetch failed: {chain_error}")

    print()
    print("=" * 60)
    print("  Webull Setup Summary")
    print("=" * 60)
    print(f"Email:               {email}")
    print(f"Paper trading:       {'enabled' if paper_enabled else 'disabled'}")
    print(f"Device ID saved:     {'yes' if bool(device_id) else 'no'}")
    print(f"Trading PIN stored:  {'yes' if bool(pin) else 'no'}")
    print(f"Account connected:   {'yes' if bool(account_info or positions) else 'partial'}")
    print(f"Options chain test:  {'passed' if chain_ok else 'failed'}")
    if expirations:
        print(f"AAPL expirations:    {len(expirations)}")
    if chain_error:
        print(f"Options error:       {chain_error}")
    print(f".env updated:        {env_path}")
    print()
    print("Setup complete.")


if __name__ == "__main__":
    main()
