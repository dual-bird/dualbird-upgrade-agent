"""
Generate JSON report from acceleration analysis.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dualbird_upgrade_agent.analyzer.estimator import ApplicationEstimate
from dualbird_upgrade_agent.parser.dag_parser import PipelineDefinition


def _ms_to_min(ms: int) -> float:
    return round(ms / 60_000, 2)


def generate_report(
    estimates: list[ApplicationEstimate],
    cluster_info: dict | None = None,
    pipeline_name: str | None = None,
    orchestrator: str | None = None,
    pipeline_def: PipelineDefinition | None = None,
) -> dict:
    """
    Generate the canonical JSON report.

    Highlights what DualBird can and cannot accelerate.
    Does NOT include fabricated speedup numbers.
    """
    total_duration_ms = sum(e.duration_ms for e in estimates)

    # Stage verdict aggregation
    total_verdicts: dict[str, int] = {"full": 0, "partial": 0, "none": 0}
    for est in estimates:
        for v, count in est.stage_verdicts.items():
            total_verdicts[v] = total_verdicts.get(v, 0) + count

    total_stages = sum(total_verdicts.values())
    coverage = (total_verdicts["full"] + total_verdicts["partial"]) / max(
        total_stages, 1
    )

    # Build tasks
    tasks = []
    for app_est in estimates:
        stages = []
        for sql_est in app_est.sql_executions:
            for se in sql_est.stages:
                stages.append(
                    {
                        "stage_id": se.stage_id,
                        "duration_ms": se.duration_ms,
                        "executor_time_ms": se.executor_time_ms,
                        "verdict": se.verdict,
                        "inferred_profile": se.inferred_profile,
                        "accelerable_categories": se.accelerable_categories,
                        "blocked_categories": se.blocked_categories,
                        "operators": se.operators,
                        "metrics": se.metrics,
                        "fallback_reasons": se.fallback_reasons,
                        "category_breakdown": se.category_breakdown,
                    }
                )

        tasks.append(
            {
                "name": app_est.app_name or app_est.app_id,
                "type": "spark",
                "duration_min": _ms_to_min(app_est.duration_ms),
                "spark_app_id": app_est.app_id,
                "stages": stages,
                "stage_verdicts": app_est.stage_verdicts,
            }
        )

    # Merge operator summaries
    cat_totals: dict[str, dict[str, int]] = {}
    for app_est in estimates:
        for entry in app_est.operator_summary:
            cat = entry["category"]
            if cat not in cat_totals:
                cat_totals[cat] = {"total_time_ms": 0, "accelerable_time_ms": 0}
            cat_totals[cat]["total_time_ms"] += entry["total_time_ms"]
            cat_totals[cat]["accelerable_time_ms"] += entry.get(
                "accelerable_time_ms", 0
            )

    operator_summary = sorted(
        [
            {
                "category": cat,
                "total_time_ms": t["total_time_ms"],
                "accelerable_time_ms": t["accelerable_time_ms"],
                "accelerable": t["accelerable_time_ms"] > 0,
            }
            for cat, t in cat_totals.items()
        ],
        key=lambda x: x["total_time_ms"],
        reverse=True,
    )

    # Merge fallbacks
    all_fallbacks = []
    for app_est in estimates:
        for fb in app_est.all_fallbacks:
            all_fallbacks.append(
                {
                    "stage_id": fb.get("stage_id"),
                    "task": fb.get("task", app_est.app_name),
                    "reason": fb.get("reason", ""),
                    "operator": fb.get("operator", ""),
                }
            )

    # Recommended config
    recommended_config = _generate_config(estimates, cat_totals)

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "version": "0.1.0",
        "pipeline": {
            "name": pipeline_name
            or (estimates[0].app_name if estimates else "Unknown"),
            "orchestrator": orchestrator,
            "current_duration_min": _ms_to_min(total_duration_ms),
            "cluster": cluster_info or {},
        },
        "coverage": {
            "total_stages": total_stages,
            "full": total_verdicts["full"],
            "partial": total_verdicts["partial"],
            "none": total_verdicts["none"],
            "coverage_pct": round(coverage, 4),
        },
        "tasks": tasks,
        "operator_summary": operator_summary,
        "fallbacks": all_fallbacks,
        "recommended_config": recommended_config,
    }


def _generate_config(
    estimates: list[ApplicationEstimate],
    cat_totals: dict[str, dict[str, int]],
) -> dict[str, Any]:
    config: dict[str, Any] = {
        "spark.plugins": "com.dualbird.spark.FpgaPlugin",
        "spark.dualbird.enabled": True,
    }

    op_config_map = {
        "sort": "spark.dualbird.sql.sort.enabled",
        "join": "spark.dualbird.sql.sortMergeJoin.enabled",
        "aggregate": "spark.dualbird.sql.groupBy.enabled",
        "window": "spark.dualbird.sql.window.enabled",
        "filter": "spark.dualbird.sql.filter.enabled",
        "project": "spark.dualbird.sql.project.enabled",
        "shuffle": "spark.dualbird.sql.shuffle.enabled",
        "scan": "spark.dualbird.sql.parquet.read.enabled",
        "write": "spark.dualbird.sql.parquet.write.enabled",
    }

    for cat, config_key in op_config_map.items():
        config[config_key] = cat in cat_totals

    total_shuffle = sum(
        se.metrics.get("shuffle_write_bytes", 0)
        for app in estimates
        for sql in app.sql_executions
        for se in sql.stages
    )
    if total_shuffle > 10 * 1024**3:
        config["spark.dualbird.shuffle.io.write.parallelism"] = 48
        config["spark.dualbird.shuffle.compute.write.parallelism"] = 24
    elif total_shuffle > 1 * 1024**3:
        config["spark.dualbird.shuffle.io.write.parallelism"] = 24
        config["spark.dualbird.shuffle.compute.write.parallelism"] = 12

    config["spark.dualbird.tracing.level"] = "info"

    return config


def write_report(report: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)


def report_to_json(report: dict) -> str:
    return json.dumps(report, indent=2, default=str)
