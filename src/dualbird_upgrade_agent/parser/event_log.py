"""
Parse Spark event log files (JSON lines format).

Spark event logs are produced when spark.eventLog.enabled=true.
Each line is a JSON object with an "Event" field identifying the event type.
"""

from __future__ import annotations

import gzip
import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class TaskMetrics:
    executor_run_time_ms: int = 0
    executor_cpu_time_ns: int = 0
    jvm_gc_time_ms: int = 0
    peak_execution_memory: int = 0
    input_bytes: int = 0
    input_records: int = 0
    output_bytes: int = 0
    output_records: int = 0
    shuffle_read_bytes: int = 0
    shuffle_read_records: int = 0
    shuffle_write_bytes: int = 0
    shuffle_write_records: int = 0


@dataclass
class StageInfo:
    stage_id: int
    attempt_id: int = 0
    name: str = ""
    num_tasks: int = 0
    submission_time: int | None = None
    completion_time: int | None = None
    failure_reason: str | None = None
    rdd_ids: list[int] = field(default_factory=list)
    parent_ids: list[int] = field(default_factory=list)
    # Aggregated task metrics
    metrics: TaskMetrics = field(default_factory=TaskMetrics)
    task_count: int = 0


@dataclass
class SparkPlanNode:
    """A node in the Spark physical plan tree (from sparkPlanInfo)."""

    node_name: str
    simple_string: str
    children: list[SparkPlanNode] = field(default_factory=list)
    metrics: list[dict] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


@dataclass
class SQLExecution:
    execution_id: int
    description: str = ""
    physical_plan_description: str = ""
    plan_root: SparkPlanNode | None = None
    start_time: int = 0
    end_time: int = 0
    duration_ms: int = 0


@dataclass
class JobInfo:
    job_id: int
    stage_ids: list[int] = field(default_factory=list)
    submission_time: int = 0
    completion_time: int = 0
    sql_execution_id: int | None = None


@dataclass
class SparkApplication:
    app_id: str = ""
    app_name: str = ""
    start_time: int = 0
    end_time: int = 0
    duration_ms: int = 0
    # Executors
    executor_count: int = 0
    total_cores: int = 0
    executor_memory: str = ""
    # Parsed data
    stages: dict[int, StageInfo] = field(default_factory=dict)
    jobs: dict[int, JobInfo] = field(default_factory=dict)
    sql_executions: dict[int, SQLExecution] = field(default_factory=dict)
    # Spark properties
    properties: dict[str, str] = field(default_factory=dict)


def _parse_plan_node(raw: dict) -> SparkPlanNode:
    """Recursively parse a sparkPlanInfo JSON object into SparkPlanNode."""
    return SparkPlanNode(
        node_name=raw.get("nodeName", ""),
        simple_string=raw.get("simpleString", ""),
        children=[_parse_plan_node(c) for c in raw.get("children", [])],
        metrics=raw.get("metrics", []),
        metadata=raw.get("metadata", {}),
    )


def _extract_task_metrics(raw_metrics: dict) -> TaskMetrics:
    """Extract TaskMetrics from a Spark event's Task Metrics JSON."""
    if not raw_metrics:
        return TaskMetrics()

    input_m = raw_metrics.get("Input Metrics", {})
    output_m = raw_metrics.get("Output Metrics", {})
    shuffle_r = raw_metrics.get("Shuffle Read Metrics", {})
    shuffle_w = raw_metrics.get("Shuffle Write Metrics", {})

    return TaskMetrics(
        executor_run_time_ms=raw_metrics.get("Executor Run Time", 0),
        executor_cpu_time_ns=raw_metrics.get("Executor CPU Time", 0),
        jvm_gc_time_ms=raw_metrics.get("JVM GC Time", 0),
        peak_execution_memory=raw_metrics.get("Peak Execution Memory", 0),
        input_bytes=input_m.get("Bytes Read", 0),
        input_records=input_m.get("Records Read", 0),
        output_bytes=output_m.get("Bytes Written", 0),
        output_records=output_m.get("Records Written", 0),
        shuffle_read_bytes=(
            shuffle_r.get("Remote Bytes Read", 0) + shuffle_r.get("Local Bytes Read", 0)
        ),
        shuffle_read_records=shuffle_r.get("Total Records Read", 0),
        shuffle_write_bytes=shuffle_w.get("Shuffle Bytes Written", 0),
        shuffle_write_records=shuffle_w.get("Shuffle Records Written", 0),
    )


def _accumulate_metrics(target: TaskMetrics, source: TaskMetrics) -> None:
    """Add source metrics into target (in-place)."""
    target.executor_run_time_ms += source.executor_run_time_ms
    target.executor_cpu_time_ns += source.executor_cpu_time_ns
    target.jvm_gc_time_ms += source.jvm_gc_time_ms
    target.peak_execution_memory = max(
        target.peak_execution_memory, source.peak_execution_memory
    )
    target.input_bytes += source.input_bytes
    target.input_records += source.input_records
    target.output_bytes += source.output_bytes
    target.output_records += source.output_records
    target.shuffle_read_bytes += source.shuffle_read_bytes
    target.shuffle_read_records += source.shuffle_read_records
    target.shuffle_write_bytes += source.shuffle_write_bytes
    target.shuffle_write_records += source.shuffle_write_records


def _open_log(path: Path):
    """Open a log file, handling gzip if needed."""
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def parse_event_log(path: Path) -> SparkApplication:
    """
    Parse a Spark event log file and return a SparkApplication.

    Handles both plain JSON-lines and gzip-compressed logs.
    """
    app = SparkApplication()

    with _open_log(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            _process_event(app, event)

    # Compute derived fields
    if app.start_time and app.end_time:
        app.duration_ms = app.end_time - app.start_time

    for sql in app.sql_executions.values():
        if sql.start_time and sql.end_time:
            sql.duration_ms = sql.end_time - sql.start_time

    return app


def _process_event(app: SparkApplication, event: dict) -> None:
    """Route a single event to the appropriate handler."""
    evt_type = event.get("Event", "")

    if evt_type == "SparkListenerApplicationStart":
        app.app_id = event.get("App ID", "")
        app.app_name = event.get("App Name", "")
        app.start_time = event.get("Timestamp", 0)

    elif evt_type == "SparkListenerApplicationEnd":
        app.end_time = event.get("Timestamp", 0)

    elif evt_type == "SparkListenerEnvironmentUpdate":
        props = event.get("Spark Properties", {})
        if isinstance(props, dict):
            app.properties = props
        elif isinstance(props, list):
            # Some Spark versions emit [["key","value"], ...]
            app.properties = {p[0]: p[1] for p in props if len(p) >= 2}
        app.executor_memory = app.properties.get("spark.executor.memory", "")
        cores_str = app.properties.get("spark.executor.cores", "0")
        try:
            app.total_cores = int(cores_str)
        except ValueError:
            pass

    elif evt_type == "SparkListenerExecutorAdded":
        app.executor_count += 1

    elif evt_type == "SparkListenerJobStart":
        job_id = event.get("Job ID", -1)
        stage_ids = [s.get("Stage ID", -1) for s in event.get("Stage Infos", [])]
        props = event.get("Properties", {})
        sql_exec_id = None
        if isinstance(props, dict):
            raw = props.get("spark.sql.execution.id")
            if raw is not None:
                sql_exec_id = int(raw)

        app.jobs[job_id] = JobInfo(
            job_id=job_id,
            stage_ids=stage_ids,
            submission_time=event.get("Submission Time", 0),
            sql_execution_id=sql_exec_id,
        )

        # Also register stages from Stage Infos
        for si in event.get("Stage Infos", []):
            sid = si.get("Stage ID", -1)
            if sid not in app.stages:
                app.stages[sid] = StageInfo(
                    stage_id=sid,
                    attempt_id=si.get("Stage Attempt ID", 0),
                    name=si.get("Stage Name", ""),
                    num_tasks=si.get("Number of Tasks", 0),
                    parent_ids=si.get("Parent IDs", []),
                    rdd_ids=[r.get("RDD ID", -1) for r in si.get("RDD Info", [])],
                )

    elif evt_type == "SparkListenerJobEnd":
        job_id = event.get("Job ID", -1)
        if job_id in app.jobs:
            app.jobs[job_id].completion_time = event.get("Completion Time", 0)

    elif evt_type == "SparkListenerStageSubmitted":
        si = event.get("Stage Info", {})
        sid = si.get("Stage ID", -1)
        if sid not in app.stages:
            app.stages[sid] = StageInfo(stage_id=sid)
        stage = app.stages[sid]
        stage.submission_time = si.get("Submission Time")
        stage.name = si.get("Stage Name", stage.name)
        stage.num_tasks = si.get("Number of Tasks", stage.num_tasks)
        stage.parent_ids = si.get("Parent IDs", stage.parent_ids)

    elif evt_type == "SparkListenerStageCompleted":
        si = event.get("Stage Info", {})
        sid = si.get("Stage ID", -1)
        if sid not in app.stages:
            app.stages[sid] = StageInfo(stage_id=sid)
        stage = app.stages[sid]
        stage.completion_time = si.get("Completion Time")
        stage.failure_reason = si.get("Failure Reason")

    elif evt_type == "SparkListenerTaskEnd":
        sid = event.get("Stage ID", -1)
        if sid not in app.stages:
            app.stages[sid] = StageInfo(stage_id=sid)
        stage = app.stages[sid]
        stage.task_count += 1

        task_info = event.get("Task Info", {})
        if task_info.get("Failed", False):
            return  # skip failed tasks

        raw_metrics = event.get("Task Metrics", {})
        task_metrics = _extract_task_metrics(raw_metrics)
        _accumulate_metrics(stage.metrics, task_metrics)

    elif evt_type == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
        exec_id = event.get("executionId", -1)
        plan_info = event.get("sparkPlanInfo")
        plan_root = _parse_plan_node(plan_info) if plan_info else None

        app.sql_executions[exec_id] = SQLExecution(
            execution_id=exec_id,
            description=event.get("description", ""),
            physical_plan_description=event.get("physicalPlanDescription", ""),
            plan_root=plan_root,
            start_time=event.get("time", 0),
        )

    elif evt_type == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
        exec_id = event.get("executionId", -1)
        if exec_id in app.sql_executions:
            app.sql_executions[exec_id].end_time = event.get("time", 0)


def parse_event_logs(paths: list[Path]) -> list[SparkApplication]:
    """Parse multiple event log files."""
    apps = []
    for p in paths:
        if p.is_dir():
            # Spark sometimes writes event logs as directories with part files
            for child in sorted(p.iterdir()):
                if child.is_file() and not child.name.startswith("."):
                    apps.append(parse_event_log(child))
        else:
            apps.append(parse_event_log(p))
    return apps
