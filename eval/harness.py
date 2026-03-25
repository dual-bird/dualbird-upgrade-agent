"""Long-running eval harness: fetch scripts, run eval, repeat.

Continuously grows the test corpus and re-evaluates as the support matrix
or analysis logic changes. Designed to run in the background.

Usage:
    python eval/harness.py [--interval 3600] [--max-scripts 200]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def run_step(label: str, cmd: list[str]) -> bool:
    """Run a subprocess and print status."""
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"\n[{ts}] {label}")
    print(f"  $ {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print(f"  FAILED (exit {result.returncode})")
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--interval",
        type=int,
        default=3600,
        help="Seconds between eval cycles (default: 3600 = 1 hour)",
    )
    parser.add_argument(
        "--max-scripts",
        type=int,
        default=200,
        help="Target number of PySpark scripts to fetch",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (no loop)",
    )
    args = parser.parse_args()

    python = sys.executable
    eval_dir = Path(__file__).parent

    cycle = 0
    while True:
        cycle += 1
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"\n{'='*70}")
        print(f"EVAL CYCLE #{cycle} — {ts}")
        print(f"{'='*70}")

        # Step 1: Fetch new scripts
        run_step(
            "Fetching PySpark scripts from GitHub...",
            [python, str(eval_dir / "fetch_pyspark_scripts.py"), "--count", str(args.max_scripts)],
        )

        # Step 2: Run evaluation
        run_step(
            "Running evaluation on all fixtures...",
            [python, str(eval_dir / "run_eval.py")],
        )

        # Step 3: Run unit tests
        run_step(
            "Running unit tests...",
            [python, "-m", "pytest", "tests/", "-v", "--tb=short"],
        )

        if args.once:
            break

        print(f"\nSleeping {args.interval}s until next cycle...")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
