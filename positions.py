from storage import get_position
from portfolio import get_portfolio_snapshot, classify_asset

EPSILON = 1e-12


def _get_sell_policy(product_id: str, snapshot: dict) -> dict:
    config = snapshot.get("config", {}) or {}
    asset_class = classify_asset(product_id, snapshot)

    overrides = config.get("sell_policy_overrides", {}) or {}
    if product_id in overrides and isinstance(overrides[product_id], dict):
        return overrides[product_id]

    defaults = config.get("sell_policy_defaults", {}) or {}
    return defaults.get(asset_class, defaults.get("unknown", {})) or {}


def _get_position_with_price(product_id: str):
    snapshot = get_portfolio_snapshot()
    pos = snapshot.get("positions", {}).get(product_id, {}) or {}
    storage_pos = get_position(product_id) or {}

    liquid_base = float(storage_pos.get("base_qty_liquid", pos.get("base_qty_liquid", 0.0)) or 0.0)
    total_base = float(storage_pos.get("base_qty_total", pos.get("base_qty_total", 0.0)) or 0.0)
    price_usd = float(pos.get("price_usd", 0.0) or 0.0)

    return snapshot, {
        "base_qty_liquid": liquid_base,
        "base_qty_total": total_base,
        "price_usd": price_usd,
    }


def compute_sell_base(product_id: str, sell_pct: float) -> float:
    snapshot, pos = _get_position_with_price(product_id)

    liquid = float(pos.get("base_qty_liquid", 0.0) or 0.0)
    price = float(pos.get("price_usd", 0.0) or 0.0)

    if liquid <= 0 or price <= 0:
        return 0.0

    sell_pct = max(0.0, min(1.0, float(sell_pct)))
    requested_sell_base = liquid * sell_pct
    if requested_sell_base <= 0:
        return 0.0

    config = snapshot.get("config", {}) or {}
    trade_min_value_usd = float(config.get("trade_min_value_usd", 10.0) or 10.0)

    policy = _get_sell_policy(product_id, snapshot)
    preserve_remainder = bool(policy.get("preserve_tradable_remainder", True))
    allow_full_exit_on_small_remainder = bool(policy.get("allow_full_exit_on_small_remainder", False))
    min_meaningful_sell_usd = float(policy.get("min_meaningful_sell_usd", 5.0) or 0.0)

    requested_sell_usd = requested_sell_base * price
    if requested_sell_usd < min_meaningful_sell_usd:
        return 0.0

    remaining_base = liquid - requested_sell_base
    remaining_usd = remaining_base * price

    if not preserve_remainder:
        if remaining_base <= EPSILON and allow_full_exit_on_small_remainder:
            return liquid
        return max(0.0, min(liquid, requested_sell_base))

    if remaining_base <= EPSILON:
        return liquid if allow_full_exit_on_small_remainder else max(0.0, min(liquid, requested_sell_base))

    if remaining_usd >= trade_min_value_usd:
        return max(0.0, min(liquid, requested_sell_base))

    target_remaining_base = trade_min_value_usd / price
    adjusted_sell_base = liquid - target_remaining_base
    adjusted_sell_usd = adjusted_sell_base * price

    if adjusted_sell_base > EPSILON and adjusted_sell_usd >= min_meaningful_sell_usd:
        return max(0.0, min(liquid, adjusted_sell_base))

    if allow_full_exit_on_small_remainder:
        return liquid

    return 0.0


def compute_full_liquid_base(product_id: str) -> float:
    snapshot, pos = _get_position_with_price(product_id)

    liquid = float(pos.get("base_qty_liquid", 0.0) or 0.0)
    if liquid <= 0:
        return 0.0

    policy = _get_sell_policy(product_id, snapshot)
    allow_full_exit_on_small_remainder = bool(policy.get("allow_full_exit_on_small_remainder", False))

    if allow_full_exit_on_small_remainder:
        return liquid

    return liquid
