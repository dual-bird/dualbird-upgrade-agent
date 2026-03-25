"""
Core acceleration analysis engine.

Analyzes Spark workloads and highlights what DualBird can and cannot accelerate.
Does NOT fabricate speedup numbers — real performance data comes from running
the plugin (Phase 3 / data-synthesizer validation).

## What this produces

Per stage:
  - Which operator categories are present (inferred from I/O profile)
  - Which of those operators the plugin supports (from support matrix)
  - What's blocking full acceleration (fallback reasons)
  - Data volume context (shuffle bytes, input bytes, etc.)

Per application:
  - Coverage: what fraction of stages contain accelerable operators
  - Operator breakdown: time-weighted distribution of operator categories
  - All fallback reasons aggregated
  - Recommended DualBird configuration
"""

from __future__ import annotations

from dataclasses import dataclass, field

from dualbird_upgrade_agent.parser.event_log import (
    SparkApplication,
    StageInfo,
)
from dualbird_upgrade_agent.parser.plan_parser import (
    PlanAnalysis,
    analyze_plan,
    flatten_operators,
)
from dualbird_upgrade_agent.matrix.support import DEFAULT_ACCELERATION_FACTORS


@dataclass
class StageEstimate:
    stage_id: int
    duration_ms: int              # wall-clock
    executor_time_ms: int         # total CPU time across tasks
    verdict: str                  # "full" | "partial" | "none"
    inferred_profile: list[str]   # what ops this stage likely runs
    operators: list[dict] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)
    fallback_reasons: list[dict] = field(default_factory=list)
    category_breakdown: dict[str, int] = field(default_factory=dict)
    accelerable_categories: list[str] = field(default_factory=list)
    blocked_categories: list[str] = field(default_factory=list)


@dataclass
class SQLExecutionEstimate:
    execution_id: int
    description: str
    duration_ms: int
    stages: list[StageEstimate] = field(default_factory=list)
    plan_analysis: PlanAnalysis | None = None
    full_stages: int = 0
    partial_stages: int = 0
    none_stages: int = 0


@dataclass
class ApplicationEstimate:
    app_id: str
    app_name: str
    duration_ms: int
    sql_executions: list[SQLExecutionEstimate] = field(default_factory=list)
    operator_summary: list[dict] = field(default_factory=list)
    all_fallbacks: list[dict] = field(default_factory=list)
    stage_verdicts: dict[str, int] = field(default_factory=dict)  # verdict -> count


# ── Stage profiling ──────────────────────────────────────────────────────────


def _stage_wall_clock(stage: StageInfo) -> int:
    if stage.submission_time and stage.completion_time:
        return stage.completion_time - stage.submission_time
    return stage.metrics.executor_run_time_ms


def _infer_stage_categories(stage: StageInfo) -> list[str]:
    """
    Infer which operator categories likely run in this stage
    based on its I/O profile (scan bytes, shuffle bytes, output bytes).
    """
    m = stage.metrics
    cats: list[str] = []

    has_input = m.input_bytes > 0
    has_shuffle_read = m.shuffle_read_bytes > 0
    has_shuffle_write = m.shuffle_write_bytes > 0
    has_output = m.output_bytes > 0

    if has_input:
        cats.extend(["scan", "filter", "project"])
    if has_shuffle_write:
        cats.append("shuffle")
    if has_shuffle_read:
        cats.append("shuffle")
    if has_shuffle_read and not has_input:
        cats.extend(["sort", "aggregate"])
        if has_shuffle_write:
            cats.append("join")
    if has_output:
        cats.append("write")

    return cats or ["aggregate"]


def _attribute_time(stage: StageInfo, stage_cats: list[str]) -> dict[str, int]:
    """
    Distribute stage executor_run_time across inferred operator categories.
    Uses data volume as a rough proxy.
    """
    total_time = stage.metrics.executor_run_time_ms
    if total_time == 0:
        return {}

    m = stage.metrics
    total_data = max(
        m.input_bytes + m.shuffle_read_bytes + m.shuffle_write_bytes + m.output_bytes,
        1,
    )

    weights: dict[str, float] = {}
    for cat in stage_cats:
        if cat == "scan":
            weights[cat] = weights.get(cat, 0) + m.input_bytes / total_data
        elif cat == "shuffle":
            weights[cat] = weights.get(cat, 0) + (
                (m.shuffle_read_bytes + m.shuffle_write_bytes) / total_data * 0.5
            )
        elif cat == "write":
            weights[cat] = weights.get(cat, 0) + m.output_bytes / total_data
        elif cat in ("sort", "join"):
            weights[cat] = weights.get(cat, 0) + 0.25
        elif cat == "aggregate":
            weights[cat] = weights.get(cat, 0) + 0.20
        elif cat in ("filter", "project"):
            weights[cat] = weights.get(cat, 0) + 0.05
        elif cat == "window":
            weights[cat] = weights.get(cat, 0) + 0.15

    total_weight = sum(weights.values()) or 1.0
    return {
        cat: int(total_time * w / total_weight)
        for cat, w in weights.items()
    }


# ── Stage analysis ───────────────────────────────────────────────────────────


def analyze_stage(
    stage: StageInfo,
    plan_analysis: PlanAnalysis | None,
) -> StageEstimate:
    """
    Analyze a stage: what runs here, what's accelerable, what's blocked.
    """
    wall_clock = _stage_wall_clock(stage)
    exec_time = stage.metrics.executor_run_time_ms
    stage_cats = _infer_stage_categories(stage)
    cat_times = _attribute_time(stage, stage_cats)

    accel_cats: set[str] = set()
    blocked_cats: dict[str, list[str]] = {}
    fallback_reasons: list[dict] = []
    op_list: list[dict] = []

    if plan_analysis and plan_analysis.operators:
        all_ops = []
        for root_op in plan_analysis.operators:
            all_ops.extend(flatten_operators(root_op))

        for op in all_ops:
            cat = op.category
            if cat in ("passthrough", "codegen", "exchange", "broadcast"):
                continue
            if op.accelerable:
                accel_cats.add(cat)
            else:
                if cat not in blocked_cats:
                    blocked_cats[cat] = []
                for r in op.fallback_reasons:
                    blocked_cats[cat].append(r)
                    fallback_reasons.append({"operator": op.node_name, "reason": r})

            op_list.append({
                "name": op.simple_string[:120],
                "node_name": op.node_name,
                "accelerable": op.accelerable,
                "category": cat,
                "expressions_supported": op.expressions_supported,
                "types_supported": op.types_supported,
                "fallback_reason": op.fallback_reasons[0] if op.fallback_reasons else None,
            })

    # Determine verdict based on which inferred categories are accelerable
    cats_in_stage = set(stage_cats)
    accel_in_stage = cats_in_stage & accel_cats
    blocked_in_stage = cats_in_stage & set(blocked_cats.keys())

    if not plan_analysis:
        verdict = "none"
    elif blocked_in_stage:
        verdict = "partial" if accel_in_stage else "none"
    elif accel_in_stage:
        verdict = "full"
    else:
        # No operators matched the inferred categories — conservative
        verdict = "partial" if accel_cats else "none"

    return StageEstimate(
        stage_id=stage.stage_id,
        duration_ms=wall_clock,
        executor_time_ms=exec_time,
        verdict=verdict,
        inferred_profile=stage_cats,
        operators=op_list,
        metrics=_metrics_dict(stage),
        fallback_reasons=fallback_reasons,
        category_breakdown=cat_times,
        accelerable_categories=sorted(accel_in_stage),
        blocked_categories=sorted(blocked_in_stage),
    )


def _metrics_dict(stage: StageInfo) -> dict:
    m = stage.metrics
    return {
        "executor_run_time_ms": m.executor_run_time_ms,
        "shuffle_write_bytes": m.shuffle_write_bytes,
        "shuffle_read_bytes": m.shuffle_read_bytes,
        "input_bytes": m.input_bytes,
        "output_bytes": m.output_bytes,
    }


# ── Application-level ────────────────────────────────────────────────────────


def estimate_application(
    app: SparkApplication,
) -> ApplicationEstimate:
    """Analyze a Spark application and highlight acceleration opportunities."""
    sql_estimates: list[SQLExecutionEstimate] = []
    all_fallbacks: list[dict] = []
    category_time_totals: dict[str, dict[str, int]] = {}
    verdict_counts: dict[str, int] = {"full": 0, "partial": 0, "none": 0}

    # Map stages to SQL executions via jobs
    stage_to_sql: dict[int, int] = {}
    for job in app.jobs.values():
        if job.sql_execution_id is not None:
            for sid in job.stage_ids:
                stage_to_sql[sid] = job.sql_execution_id

    for sql_exec in app.sql_executions.values():
        plan_analysis = None
        if sql_exec.plan_root:
            plan_analysis = analyze_plan(sql_exec.plan_root)

        sql_stages = [
            app.stages[sid]
            for sid, sql_id in stage_to_sql.items()
            if sql_id == sql_exec.execution_id and sid in app.stages
        ]

        stage_estimates = []
        full = partial = none = 0

        for stage in sql_stages:
            se = analyze_stage(stage, plan_analysis)
            stage_estimates.append(se)

            verdict_counts[se.verdict] = verdict_counts.get(se.verdict, 0) + 1
            if se.verdict == "full":
                full += 1
            elif se.verdict == "partial":
                partial += 1
            else:
                none += 1

            all_fallbacks.extend(
                {**fb, "stage_id": se.stage_id, "task": sql_exec.description}
                for fb in se.fallback_reasons
            )

            for cat, time_ms in se.category_breakdown.items():
                if cat not in category_time_totals:
                    category_time_totals[cat] = {"total": 0, "accelerable": 0}
                category_time_totals[cat]["total"] += time_ms
                if cat in se.accelerable_categories:
                    category_time_totals[cat]["accelerable"] += time_ms

        sql_duration = sql_exec.duration_ms or sum(se.duration_ms for se in stage_estimates)

        sql_estimates.append(SQLExecutionEstimate(
            execution_id=sql_exec.execution_id,
            description=sql_exec.description,
            duration_ms=sql_duration,
            stages=stage_estimates,
            plan_analysis=plan_analysis,
            full_stages=full,
            partial_stages=partial,
            none_stages=none,
        ))

    total_duration = app.duration_ms or sum(se.duration_ms for se in sql_estimates)

    op_summary = sorted(
        [
            {
                "category": cat,
                "total_time_ms": t["total"],
                "accelerable_time_ms": t["accelerable"],
                "accelerable": t["accelerable"] > 0,
            }
            for cat, t in category_time_totals.items()
        ],
        key=lambda x: x["total_time_ms"],
        reverse=True,
    )

    # Deduplicate fallbacks
    seen: set[tuple] = set()
    unique_fallbacks = []
    for fb in all_fallbacks:
        key = (fb.get("stage_id"), fb.get("operator"), fb.get("reason"))
        if key not in seen:
            seen.add(key)
            unique_fallbacks.append(fb)

    return ApplicationEstimate(
        app_id=app.app_id,
        app_name=app.app_name,
        duration_ms=total_duration,
        sql_executions=sql_estimates,
        operator_summary=op_summary,
        all_fallbacks=unique_fallbacks,
        stage_verdicts=verdict_counts,
    )
