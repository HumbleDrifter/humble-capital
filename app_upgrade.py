def process_alert(data):
    action, signal_type, product_id = normalize_signal(data)

    allowed = is_allowed_product(product_id)

    if signal_type in {
        "CORE_BUY_WINDOW",
        "SATELLITE_BUY",
        "SATELLITE_BUY_EARLY",
        "SATELLITE_BUY_HEAVY",
        "SNIPER_BUY",
    } and not allowed:
        msg = f"❌ Blocked {signal_type} for {product_id}: product not allowed"
        print(msg)
        send_telegram(msg)
        return {"ok": False, "reason": f"product_not_allowed={product_id}"}

    if signal_type in {
        "SATELLITE_BUY_EARLY",
        "SATELLITE_BUY",
        "SATELLITE_BUY_HEAVY",
        "SNIPER_BUY",
    }:
        result = dispatch_signal_action(product_id, "BUY", signal_type=signal_type)
        msg = f"📡 {signal_type} {product_id}: {result}"
        print(msg)
        send_telegram(msg)
        return result

    if signal_type == "CORE_BUY_WINDOW":
        result = run_core_rebalance_window(trigger_product_id=product_id)
        msg = f"🏦 CORE_BUY_WINDOW {product_id}: {result}"
        print(msg)
        send_telegram(msg)
        return result

    if signal_type == "TRIM":
        result = dispatch_signal_action(product_id, "TRIM", signal_type=signal_type)
        msg = f"✂️ TRIM {product_id}: {result}"
        print(msg)
        send_telegram(msg)
        return result

    if signal_type == "EXIT":
        result = dispatch_signal_action(product_id, "EXIT", signal_type=signal_type)
        msg = f"🚪 EXIT {product_id}: {result}"
        print(msg)
        send_telegram(msg)
        return result

    return {"ok": False, "reason": f"unsupported_signal_type={signal_type}"}
