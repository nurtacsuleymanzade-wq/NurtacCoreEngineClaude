import json, sys

TFS = [("1S","data/structure_1s.jsonl"),("1M","data/structure_1m.jsonl"),
       ("5M","data/structure_5m.jsonl"),("15M","data/structure_15m.jsonl")]

all_ok = True
for tf, fname in TFS:
    try:
        with open(fname) as f:
            lines = [l.strip() for l in f if l.strip()]
        last = json.loads(lines[-1])
        errs = []
        for ob in last["order_blocks"]:
            if ob["ob_high"] <= ob["ob_low"]:
                errs.append(f"OB invalid: {ob}")
        for fvg in last["fvg"]:
            if fvg["gap_high"] <= fvg["gap_low"]:
                errs.append(f"FVG invalid: {fvg}")
        if not (last["atr_used"] > 0):
            errs.append("atr_used <= 0")
        if last["trend"]["direction"] not in ("uptrend","downtrend","ranging","unknown"):
            errs.append(f"bad trend direction: {last['trend']['direction']}")
        if last["window_start_ts"] >= last["window_end_ts"]:
            errs.append("ts order invalid")
        print(f"{tf}: {len(lines)} lines | trend={last['trend']['direction']}/{last['trend']['strength']} | fractals={last['swing']['confirmed_fractals_count']} | obs={len(last['order_blocks'])} | fvgs={len(last['fvg'])} | atr={last['atr_used']:.4f}")
        if errs:
            print(f"  ERRORS: {errs}")
            all_ok = False
        else:
            print(f"  Validation OK")
    except FileNotFoundError:
        print(f"{tf}: file not found")

sys.exit(0 if all_ok else 1)
