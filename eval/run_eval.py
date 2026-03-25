"""Evaluate the DualBird Upgrade Agent MCP tools against real PySpark scripts.

Runs analyze_pyspark_code on every fixture script and produces a summary
report. Designed to be run repeatedly as the support matrix or analysis
logic evolves — results are timestamped and compared against previous runs.

Usage:
    python eval/run_eval.py [--fixtures eval/fixtures] [--output eval/results]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

# Add src to path for direct execution
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dualbird_upgrade_agent.server import (
    analyze_pyspark_code,
)


def evaluate_script(path: Path) -> dict:
    """Run analysis on a single PySpark script and return metrics."""
    start = time.monotonic()
    try:
        raw = analyze_pyspark_code(file_path=str(path))
        result = json.loads(raw)
        elapsed = time.monotonic() - start

        if "error" in result:
            return {
                "file": path.name,
                "status": "error",
                "error": result["error"],
                "elapsed_ms": round(elapsed * 1000),
            }

        return {
            "file": path.name,
            "status": "ok",
            "total_operators": result["total_operators"],
            "accelerable_count": result["accelerable_count"],
            "blocked_count": result["blocked_count"],
            "has_udfs": result["has_udfs"],
            "has_pandas_udfs": result.get("has_pandas_udfs", False),
            "confidence": result["confidence"],
            "coverage_pct": (
                round(result["accelerable_count"] / result["total_operators"] * 100, 1)
                if result["total_operators"] > 0
                else 0
            ),
            "categories": sorted(
                {
                    op["category"]
                    for op in result.get("operators", [])
                    if op.get("accelerable")
                }
            ),
            "blocked_reasons": [
                d["reason"]
                for d in result.get("blocked_details", [])
                if d.get("reason")
            ],
            "elapsed_ms": round(elapsed * 1000),
        }
    except Exception as e:
        elapsed = time.monotonic() - start
        return {
            "file": path.name,
            "status": "exception",
            "error": str(e),
            "elapsed_ms": round(elapsed * 1000),
        }


def compute_summary(results: list[dict]) -> dict:
    """Compute aggregate metrics from individual script results."""
    ok_results = [r for r in results if r["status"] == "ok"]
    error_count = sum(1 for r in results if r["status"] != "ok")

    if not ok_results:
        return {
            "total_scripts": len(results),
            "successful": 0,
            "errors": error_count,
        }

    coverages = [r["coverage_pct"] for r in ok_results]
    udf_count = sum(1 for r in ok_results if r["has_udfs"])

    # Aggregate category frequency
    category_freq: dict[str, int] = {}
    for r in ok_results:
        for cat in r.get("categories", []):
            category_freq[cat] = category_freq.get(cat, 0) + 1

    # Aggregate blocked reasons
    reason_freq: dict[str, int] = {}
    for r in ok_results:
        for reason in r.get("blocked_reasons", []):
            reason_freq[reason] = reason_freq.get(reason, 0) + 1

    return {
        "total_scripts": len(results),
        "successful": len(ok_results),
        "errors": error_count,
        "coverage": {
            "mean_pct": round(sum(coverages) / len(coverages), 1),
            "median_pct": round(sorted(coverages)[len(coverages) // 2], 1),
            "min_pct": min(coverages),
            "max_pct": max(coverages),
            "fully_accelerable": sum(1 for c in coverages if c == 100),
            "partially_accelerable": sum(1 for c in coverages if 0 < c < 100),
            "not_accelerable": sum(1 for c in coverages if c == 0),
        },
        "udfs": {
            "scripts_with_udfs": udf_count,
            "pct_with_udfs": round(udf_count / len(ok_results) * 100, 1),
        },
        "category_frequency": dict(sorted(category_freq.items(), key=lambda x: -x[1])),
        "top_blocked_reasons": dict(
            sorted(reason_freq.items(), key=lambda x: -x[1])[:15]
        ),
        "performance": {
            "mean_ms": round(
                sum(r["elapsed_ms"] for r in ok_results) / len(ok_results)
            ),
            "max_ms": max(r["elapsed_ms"] for r in ok_results),
        },
    }


def compare_with_previous(results_dir: Path, current_summary: dict) -> dict | None:
    """Compare current run with the most recent previous run."""
    previous_runs = sorted(results_dir.glob("eval_*.json"))
    if len(previous_runs) < 2:
        return None

    # Load the second-most-recent (the most recent is current)
    prev = json.loads(previous_runs[-2].read_text())
    prev_summary = prev.get("summary", {})

    if not prev_summary.get("coverage"):
        return None

    return {
        "previous_run": previous_runs[-2].name,
        "coverage_mean_delta": round(
            current_summary["coverage"]["mean_pct"]
            - prev_summary["coverage"]["mean_pct"],
            1,
        ),
        "scripts_delta": (
            current_summary["total_scripts"] - prev_summary["total_scripts"]
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fixtures",
        type=Path,
        default=Path(__file__).parent / "fixtures",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent / "results",
    )
    args = parser.parse_args()

    scripts = sorted(args.fixtures.glob("*.py"))
    if not scripts:
        print(f"No .py files found in {args.fixtures}")
        print("Run: python eval/fetch_pyspark_scripts.py --count 20")
        sys.exit(1)

    print(f"Evaluating {len(scripts)} PySpark scripts...")
    print()

    results = []
    for i, script in enumerate(scripts, 1):
        r = evaluate_script(script)
        status_icon = "+" if r["status"] == "ok" else "!"
        coverage = (
            f"{r.get('coverage_pct', 0)}%"
            if r["status"] == "ok"
            else r.get("error", "?")
        )
        print(
            f"  [{status_icon}] {i:3d}/{len(scripts)} {script.name[:60]:<60s} {coverage}"
        )
        results.append(r)

    summary = compute_summary(results)

    # Print summary
    print("\n" + "=" * 70)
    print("EVALUATION SUMMARY")
    print("=" * 70)
    print(f"Scripts analyzed:     {summary['successful']}/{summary['total_scripts']}")
    if summary.get("coverage"):
        cov = summary["coverage"]
        print(f"Mean coverage:        {cov['mean_pct']}%")
        print(f"Median coverage:      {cov['median_pct']}%")
        print(f"Range:                {cov['min_pct']}% - {cov['max_pct']}%")
        print(f"Fully accelerable:    {cov['fully_accelerable']}")
        print(f"Partially:            {cov['partially_accelerable']}")
        print(f"None:                 {cov['not_accelerable']}")

        if summary.get("udfs"):
            print(
                f"\nScripts with UDFs:    {summary['udfs']['scripts_with_udfs']} ({summary['udfs']['pct_with_udfs']}%)"
            )

        if summary.get("category_frequency"):
            print("\nCategory frequency:")
            for cat, count in summary["category_frequency"].items():
                print(f"  {cat:<15s} {count}")

        if summary.get("top_blocked_reasons"):
            print("\nTop blocked reasons:")
            for reason, count in list(summary["top_blocked_reasons"].items())[:10]:
                print(f"  ({count}x) {reason[:70]}")

    # Save results
    args.output.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    output_file = args.output / f"eval_{timestamp}.json"

    report = {
        "timestamp": datetime.now(UTC).isoformat(),
        "scripts_dir": str(args.fixtures),
        "summary": summary,
        "results": results,
    }

    # Compare with previous
    comparison = compare_with_previous(args.output, summary)
    if comparison:
        report["comparison"] = comparison
        print(f"\nDelta vs {comparison['previous_run']}:")
        print(f"  Coverage: {comparison['coverage_mean_delta']:+.1f}%")
        print(f"  Scripts:  {comparison['scripts_delta']:+d}")

    output_file.write_text(json.dumps(report, indent=2))
    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
