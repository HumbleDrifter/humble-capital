from dotenv import load_dotenv

from execution import get_client, _to_dict, get_best_bid_ask
from storage import reset_positions, upsert_position

load_dotenv("/root/tradingbot/.env")

STABLE_CURRENCIES = {"USD", "USDC", "USDT", "DAI"}
KNOWN_NON_TRADEABLE = {"EUR", "GBP"}


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return 0.0



def infer_product_id(currency):
    return f"{str(currency).upper()}-USD"



def product_exists(product_id):
    try:
        bid, ask = get_best_bid_ask(product_id)
        return float(bid) > 0 or float(ask) > 0
    except Exception:
        return False



def classify_locked_amount(account):
    available_balance = account.get("available_balance") or {}
    total_balance = account.get("balance") or {}

    available = safe_float(available_balance.get("value", available_balance.get("amount", 0)))
    total = safe_float(total_balance.get("value", total_balance.get("amount", 0)))

    hold = account.get("hold") or {}
    locked = account.get("locked") or {}
    staking_balance = account.get("staking_balance") or {}
    staked_balance = account.get("staked_balance") or {}

    hold_amt = safe_float(hold.get("value", hold.get("amount", 0)))
    locked_amt = safe_float(locked.get("value", locked.get("amount", 0)))
    staking_amt = safe_float(staking_balance.get("value", staking_balance.get("amount", 0)))
    staked_amt = safe_float(staked_balance.get("value", staked_balance.get("amount", 0)))

    explicit_locked = max(hold_amt, locked_amt, staking_amt, staked_amt)
    if explicit_locked > 0:
        liquid = max(0.0, total - explicit_locked)
        return total, liquid, explicit_locked

    inferred_locked = max(0.0, total - available)
    return total, available, inferred_locked



def fetch_accounts():
    client = get_client()
    accounts = []
    cursor = None

    while True:
        resp = _to_dict(client.get_accounts(cursor=cursor) if cursor else client.get_accounts())
        accounts.extend(resp.get("accounts", []))
        if not resp.get("has_next"):
            break
        cursor = resp.get("cursor")

    return accounts



def reconcile_positions(log_accounts=False):
    accounts = fetch_accounts()
    imported = []
    skipped = []

    reset_positions()

    for acct in accounts:
        currency = str(acct.get("currency") or "").upper().strip()
        if not currency:
            skipped.append({"reason": "blank_currency"})
            continue

        total_qty, liquid_qty, locked_qty = classify_locked_amount(acct)
        if total_qty <= 0:
            skipped.append({"currency": currency, "reason": "zero_total"})
            continue

        if currency in STABLE_CURRENCIES:
            # cash/stable balances are handled separately in portfolio.get_cash_breakdown()
            skipped.append({"currency": currency, "reason": "cash_equivalent"})
            continue

        if currency in KNOWN_NON_TRADEABLE:
            skipped.append({"currency": currency, "reason": "known_non_tradeable"})
            continue

        product_id = infer_product_id(currency)
        if not product_exists(product_id):
            skipped.append({"currency": currency, "product_id": product_id, "reason": "no_usd_market"})
            continue

        upsert_position(
            product_id=product_id,
            base_qty_total=total_qty,
            base_qty_liquid=liquid_qty,
            base_qty_locked=locked_qty,
        )
        imported.append({
            "currency": currency,
            "product_id": product_id,
            "base_qty_total": total_qty,
            "base_qty_liquid": liquid_qty,
            "base_qty_locked": locked_qty,
        })

    if log_accounts:
        print("Imported positions:")
        for item in imported:
            print(item)
        print("Skipped accounts:")
        for item in skipped:
            print(item)

    return {
        "ok": True,
        "imported_count": len(imported),
        "skipped_count": len(skipped),
        "imported": imported,
        "skipped": skipped,
    }


if __name__ == "__main__":
    result = reconcile_positions(log_accounts=True)
    print("Reconcile complete.")
    print(result)
