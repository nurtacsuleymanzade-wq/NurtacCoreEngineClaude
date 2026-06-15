import asyncio, time, sys
sys.path.insert(0, ".")
from market_context_engine import MktState, _compute_bias

state = MktState()
state.global_ls_ratio = 0.5912
state.top_trader_ls_ratio = 0.6032
state.taker_buy_ratio = 0.5099
state.funding_rate = -0.00003265
state.oi_curr = 101710.0
state.oi_prev = None

now_ts = int(time.time() * 1000)
rec = _compute_bias(state, now_ts)
if rec:
    lb = rec["long_bias"]
    sb = rec["short_bias"]
    gap = rec["bias_gap"]
    dom = rec["dominant_bias"]
    print(f"long={lb} short={sb} gap={gap} dominant={dom}")
    ls = rec["components"]["long_short_ratio"]
    print(f"  global_state={ls['global_state']} ls_long={ls['long_contribution']} ls_short={ls['short_contribution']}")
    fr = rec["components"]["funding"]
    print(f"  funding_state={fr['funding_state']}")
else:
    print("VALIDATION FAILED")
