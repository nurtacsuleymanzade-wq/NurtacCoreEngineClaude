import json, sys, math

# ── Evidence stream validation
ev_lines = []
with open("data/evidence_stream.jsonl") as f:
    for l in f:
        if l.strip():
            ev_lines.append(json.loads(l.strip()))

errors = []
ls_vals = []
ss_vals = []
sides = {}
for r in ev_lines:
    ls = r.get("long_score", -1)
    ss = r.get("short_score", -1)
    gap = r.get("score_gap", -1)
    ds = r.get("dominant_side", "?")
    ls_vals.append(ls)
    ss_vals.append(ss)
    sides[ds] = sides.get(ds, 0) + 1

    if ls < 0 or math.isnan(ls) or math.isinf(ls):
        errors.append(f"[1] bad long_score: {ls}")
    if ss < 0 or math.isnan(ss) or math.isinf(ss):
        errors.append(f"[1] bad short_score: {ss}")
    if ds not in ("long", "short", "neutral"):
        errors.append(f"[2] bad dominant_side: {ds}")
    expected = round(abs(ls - ss), 4)
    if abs(gap - expected) > 1e-4:
        errors.append(f"[3] score_gap mismatch: {gap} vs {expected}")

print(f"Evidence lines: {len(ev_lines)}")
print(f"Long score: min={min(ls_vals):.2f} max={max(ls_vals):.2f} avg={sum(ls_vals)/len(ls_vals):.2f}")
print(f"Short score: min={min(ss_vals):.2f} max={max(ss_vals):.2f} avg={sum(ss_vals)/len(ss_vals):.2f}")
print(f"Dominant side dist: {sides}")
print(f"Validation errors: {len(errors)}")
for e in errors[:5]:
    print(" ", e)

# Show top 5 highest long scores
top_long = sorted(ev_lines, key=lambda x: x["long_score"], reverse=True)[:5]
print("\nTop 5 long scores:")
for r in top_long:
    c = r.get("evidence_components", {})
    print(f"  ts={r['window_start_ts']} long={r['long_score']:.2f} short={r['short_score']:.2f} "
          f"gate={c.get('gate', {}).get('grade')} "
          f"trend1s={c.get('smart_money_1s', {}).get('trend_uptrend')} "
          f"bos={c.get('smart_money_1s', {}).get('micro_bos_bullish')} "
          f"ob={r.get('dominant_side')}")

# Check setups
try:
    se_lines = [json.loads(l) for l in open("data/setups.jsonl") if l.strip()]
    print(f"\nSetups: {len(se_lines)}")
    for s in se_lines[:3]:
        print(f"  {s['setup_id']} entry={s['entry']['price']} direction={s['direction']}")
except FileNotFoundError:
    print("\nNo setups file")

sys.exit(0 if not errors else 1)
