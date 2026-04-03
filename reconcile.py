from dotenv import load_dotenv
from execution import get_client
from storage import reset_positions, upsert_position

load_dotenv("/root/tradingbot/.env")

STABLE_CURRENCIES = {"USD", "USDC", "USDT", "DAI"}


def _to_dict(x):
    return x.to_dict() if hasattr(x, "to_dict") else x


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return 0.0


def infer_product_id(currency):
    return f"{str(currency).upper()}-USD"


def has_usd_market(client, product_id):
    try:
        resp = _to_dict(client.get_product(product_id=product_id))
        return bool(resp)
    except Exception:
        return False


def classify_locked_amount(account):
    """
    Best-effort locked/staked extraction.

    Different Coinbase payloads can expose this differently, so we try:
    - hold
    - locked
    - staking / staked balances
    - total - available fallback

    Coinbase Advanced Trade sometimes reports usable balances in
    available_balance even when balance is zero, so we normalize that.
    """
    available_balance = account.get("available_balance") or {}
    total_balance = account.get("balance") or {}

    available = safe_float(
        available_balance.get("value", available_balance.get("amount", 0))
    )
    total = safe_float(
        total_balance.get("value", total_balance.get("amount", 0))
    )

    # Important Coinbase fix:
    # if total is zero but available is positive, trust available.
    if total <= 0 and available > 0:
        total = available

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

    # fallback: if total > available, treat difference as locked
    inferred_locked = max(0.0, total - available)
    return total, available, inferred_locked


def reconcile_positions():
    client = get_client()

    accounts = []
    cursor = None

    while True:
        resp = _to_dict(client.get_accounts(cursor=cursor) if cursor else client.get_accounts())
        accounts.extend(resp.get("accounts", []))

        if not resp.get("has_next"):
            break

        cursor = resp.get("cursor")

    imported = []
    skipped = []

    reset_positions()

    for acct in accounts:
        currency = str(acct.get("currency") or "").upper().strip()
        if not currency:
            skipped.append({"currency": "", "reason": "missing_currency"})
            continue

        # Cash/stablecoins are handled separately in portfolio cash logic,
        # so do not store them as crypto positions.
        if currency in STABLE_CURRENCIES:
            skipped.append({"currency": currency, "reason": "cash_or_stablecoin"})
            continue

        total_qty, liquid_qty, locked_qty = classify_locked_amount(acct)

        if total_qty <= 0:
            skipped.append({"currency": currency, "reason": "zero_total"})
            continue

        product_id = infer_product_id(currency)

        # Only insert if Coinbase recognizes the USD market.
        if not has_usd_market(client, product_id):
            skipped.append({"currency": currency, "reason": "no_usd_market", "product_id": product_id})
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
    result = reconcile_positions()
    print("Reconcile complete.")
    print(result)
