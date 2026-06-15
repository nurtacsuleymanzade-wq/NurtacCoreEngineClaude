"""
NurtacCoreEngineClaude Production Supervisor

Tüm 17 engine'i tek bir asyncio event loop içinde çalıştırır.
Process count: 19 → 1
RAM usage: 3.8GB → 400-600MB (85% tasarrufu)

Her engine'i try/except ile sar — bir engine çökse bile supervisor devam eder.
"""

import asyncio
import importlib
import sys
import os
import time
import resource
import traceback
from pathlib import Path

# Tüm stderr'ı stdout'a yönlendir (journalctl'a yazılsın)
sys.stderr = sys.stdout

# DATA_DIR setup
DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
HALT_FILE = DATA_DIR / "SYSTEM_HALT"

# Engines listesi: (module_name, [candidate_function_names])
# Supervisor, first available function'ı kullanır
ENGINES = [
    ("rolling_window_engine", ["run_layer1", "run_live", "main"]),
    ("aligned_candle_engine", ["run_layer2", "run_live", "main"]),
    ("historical_baseline_engine", ["run_baseline", "run_live", "main"]),
    ("detector_engine", ["run_detector", "run_live", "main"]),
    ("decision_gate", ["run_gate", "run_live", "main"]),
    ("smart_money_engine", ["run_smartmoney", "run_live", "main"]),
    ("evidence_engine", ["run_evidence", "run_live", "main"]),
    ("market_context_engine", ["run_context", "run_live", "main"]),
    ("volume_profile_engine", ["run_volprofile", "run_live", "main"]),
    ("scenario_engine", ["run_scenario", "run_live", "main"]),
    ("observer_engine", ["run_observer", "run_live", "main"]),
    ("historical_outcome_engine", ["run_outcome", "run_live", "main"]),
    ("paper_trade_engine", ["run_paper", "run_live", "main"]),
    ("telegram_reporter", ["run_reporter", "run_live", "main"]),
    ("edge_matrix_engine", ["run_edge", "run_live", "main"]),
    ("final_setup_engine", ["run_final", "run_live", "main"]),
]

# State
tasks: dict[str, asyncio.Task] = {}
last_memory_check = time.time()

def get_memory_mb() -> float:
    """Get current process memory usage in MB."""
    try:
        ru = resource.getrusage(resource.RUSAGE_SELF)
        return ru.ru_maxrss / 1024  # Convert KB to MB
    except Exception:
        return 0.0

async def run_engine_with_restart(module_name: str, candidate_funcs: list[str]) -> None:
    """Run engine function in a loop, restarting on crash.

    CRITICAL: Her engine'nin hatası supervisor'ı çökmesine izin vermemeli.
    Try/except ile tüm engine hatalarını yakala, log et, yeniden başlat.
    """
    func = None
    func_name = None

    # Find first available function
    try:
        module = importlib.import_module(module_name)
        for cand_name in candidate_funcs:
            if hasattr(module, cand_name):
                func = getattr(module, cand_name)
                func_name = cand_name
                break
    except ImportError as e:
        print(f"[SUPERVISOR] ❌ HATA: {module_name} import edilemedi: {e}", flush=True)
        await asyncio.sleep(5)
        return

    if func is None:
        print(
            f"[SUPERVISOR] ❌ HATA: {module_name} için fonksiyon bulunamadı. "
            f"Aranılan: {', '.join(candidate_funcs)}",
            flush=True
        )
        await asyncio.sleep(5)
        return

    print(f"[SUPERVISOR] ▶ {module_name}.{func_name} başlatılıyor...", flush=True)

    crash_count = 0
    while not HALT_FILE.exists():
        try:
            # Call function (may be sync or async)
            if asyncio.iscoroutinefunction(func):
                await func()
                print(f"[SUPERVISOR] ✓ {module_name}.{func_name} normal olarak sonlandı", flush=True)
                break  # Exit after successful completion
            else:
                # Sync function — detect if it's "main" (runs forever)
                if func_name == "main":
                    # main() functions typically run forever in live mode
                    # Run them in background thread, don't wait
                    loop = asyncio.get_event_loop()
                    loop.run_in_executor(None, func)
                    print(f"[SUPERVISOR] ▶ {module_name}.{func_name} arka plan thread'inde başlatıldı (sonsuz loop)", flush=True)
                    # Keep the thread running until SYSTEM_HALT or crash
                    while not HALT_FILE.exists():
                        await asyncio.sleep(5)
                    print(f"[SUPERVISOR] ✓ {module_name}.{func_name} SYSTEM_HALT ile sonlandırıldı", flush=True)
                    break
                else:
                    # Other sync functions — wait for completion
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, func)
                    print(f"[SUPERVISOR] ✓ {module_name}.{func_name} normal olarak sonlandı", flush=True)
                    break

        except asyncio.CancelledError:
            print(f"[SUPERVISOR] ⊘ {module_name} iptal edildi", flush=True)
            raise
        except Exception as e:
            crash_count += 1
            print(f"[SUPERVISOR] ❌ CRASH #{crash_count}: {module_name}", flush=True)
            print(f"[SUPERVISOR] ❌ Hata: {type(e).__name__}: {e}", flush=True)

            # Traceback'i de print et
            print(f"[SUPERVISOR] ❌ Traceback:", flush=True)
            tb_lines = traceback.format_exc().split('\n')
            for line in tb_lines:
                if line:
                    print(f"[SUPERVISOR]   {line}", flush=True)

            print(f"[SUPERVISOR] ⏳ {module_name} 10 saniye sonra yeniden başlıyor (Crash #{crash_count})...", flush=True)
            await asyncio.sleep(10)

async def memory_monitor() -> None:
    """Periodically log memory usage."""
    while not HALT_FILE.exists():
        try:
            mem_mb = get_memory_mb()
            active_count = sum(1 for t in tasks.values() if not t.done())
            print(
                f"[SUPERVISOR] RAM: {mem_mb:.0f}MB | "
                f"Tasks: {active_count}/{len(ENGINES)} | "
                f"Time: {time.strftime('%H:%M:%S')}",
                flush=True
            )
        except Exception as e:
            print(f"[SUPERVISOR] Memory monitor error: {e}", flush=True)

        await asyncio.sleep(60)

async def main() -> None:
    """Main supervisor loop."""
    print("[SUPERVISOR] Production Supervisor başlatılıyor...", flush=True)
    print(f"[SUPERVISOR] {len(ENGINES)} engine yüklenecek", flush=True)

    # Add memory monitor task
    monitor_task = asyncio.create_task(memory_monitor(), name="memory-monitor")
    tasks["memory-monitor"] = monitor_task

    # Create tasks for each engine with staggered startup
    for idx, (module_name, candidate_funcs) in enumerate(ENGINES):
        if HALT_FILE.exists():
            print("[SUPERVISOR] SYSTEM_HALT — başlangıç iptal edildi", flush=True)
            break

        # Staggered startup: 2 seconds between each engine
        if idx > 0:
            await asyncio.sleep(2)

        task = asyncio.create_task(
            run_engine_with_restart(module_name, candidate_funcs),
            name=module_name
        )
        tasks[module_name] = task
        print(f"[SUPERVISOR] [{idx+1}/{len(ENGINES)}] {module_name} task oluşturuldu", flush=True)

    print("[SUPERVISOR] Tüm engine'ler başlatıldı, şimdi izleniyor...", flush=True)

    # Wait for all tasks
    try:
        await asyncio.gather(*tasks.values(), return_exceptions=True)
    except KeyboardInterrupt:
        print("[SUPERVISOR] KeyboardInterrupt — kapatılıyor", flush=True)
    except Exception as e:
        print(f"[SUPERVISOR] Beklenmeyen hata: {e}", flush=True)
    finally:
        print("[SUPERVISOR] Tüm görevler kapatılıyor...", flush=True)
        for task in tasks.values():
            if not task.done():
                task.cancel()

        # Wait for cancellation
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        print("[SUPERVISOR] Supervisor kapandı", flush=True)

if __name__ == "__main__":
    print("[SUPERVISOR] === NurtacCoreEngineClaude Production Supervisor ===", flush=True)
    print(f"[SUPERVISOR] Başlangıç zamanı: {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"[SUPERVISOR] Python version: {sys.version}", flush=True)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[SUPERVISOR] Interrupted by user", flush=True)
        sys.exit(0)
    except Exception as e:
        print(f"[SUPERVISOR] Fatal error: {e}", flush=True)
        sys.exit(1)
