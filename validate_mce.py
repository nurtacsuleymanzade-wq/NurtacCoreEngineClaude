import json, math, sys

ok = True

# bias_context.jsonl
try:
    lines = [json.loads(l) for l in open("data/bias_context.jsonl") if l.strip()]
    last = lines[-1]
    lb = last["long_bias"]; sb = last["short_bias"]; gap = last["bias_gap"]
    errs = []
    if lb < 0 or math.isnan(lb): errs.append(f"long_bias bad: {lb}")
    if sb < 0 or math.isnan(sb): errs.append(f"short_bias bad: {sb}")
    if last["dominant_bias"] not in ("long","short","neutral"): errs.append("bad dominant_bias")
    if abs(gap - round(abs(lb-sb),4)) > 1e-3: errs.append(f"bias_gap mismatch {gap} vs {round(abs(lb-sb),4)}")
    fr = last["components"]["funding"]["funding_rate"]
    if not (-1.0 <= fr <= 1.0): errs.append(f"funding out of range: {fr}")
    gls = last["components"]["long_short_ratio"]["global_ls_ratio"]
    if not (0.0 <= gls <= 1.0): errs.append(f"global_ls out of range: {gls}")
    tbr = last["components"]["taker_volume"]["taker_buy_ratio"]
    if not (0.0 <= tbr <= 1.0): errs.append(f"taker_buy_ratio out of range: {tbr}")
    print(f"bias_context: {len(lines)} records | dominant={last['dominant_bias']} long={lb} short={sb} gap={gap}")
    print(f"  OI={last['market_context']['oi_value']} FR={fr:.8f} gLS={gls:.4f} ttLS={last['market_context']['top_trader_ls_ratio']:.4f} taker={tbr:.4f}")
    if errs: print("  ERRORS:", errs); ok = False
    else: print("  Validation OK")
except Exception as e:
    print(f"bias_context ERROR: {e}"); ok = False

# market_context.jsonl
try:
    lines = [json.loads(l) for l in open("data/market_context.jsonl") if l.strip()]
    last = lines[-1]
    print(f"market_context: {len(lines)} records | OI={last['oi_value']} price={last['current_price']}")
except Exception as e:
    print(f"market_context ERROR: {e}"); ok = False

# liquidation_heatmap.jsonl
try:
    lines = [json.loads(l) for l in open("data/liquidation_heatmap.jsonl") if l.strip()]
    last = lines[-1]
    print(f"liquidation_heatmap: {len(lines)} records | bias={last['heatmap_bias']} max_pain={last['max_pain_price']} levels={len(last['price_levels'])}")
except Exception as e:
    print(f"liquidation_heatmap ERROR: {e}"); ok = False

# liquidation_events.jsonl (may be empty)
try:
    lines = [l for l in open("data/liquidation_events.jsonl") if l.strip()]
    print(f"liquidation_events: {len(lines)} records")
except FileNotFoundError:
    print("liquidation_events: empty (no file)")

sys.exit(0 if ok else 1)
