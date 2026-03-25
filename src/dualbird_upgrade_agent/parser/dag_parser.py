"""
Parse Airflow DAG files (.py) and Databricks workflow definitions (.json).

Extracts pipeline structure: task names, types, dependencies, and
which tasks are Spark jobs (so we can link them to event logs).

Uses Python AST for Airflow DAGs — no execution needed.
"""

from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class PipelineTask:
    name: str
    type: str  # "spark" | "python" | "bash" | "notebook" | "other"
    operator: str  # e.g. "SparkSubmitOperator", "PythonOperator"
    dependencies: list[str] = field(default_factory=list)
    spark_app_name: str | None = None
    config: dict = field(default_factory=dict)


@dataclass
class PipelineDefinition:
    name: str
    orchestrator: str  # "airflow" | "databricks"
    tasks: list[PipelineTask] = field(default_factory=list)


# Airflow operators that indicate Spark jobs
SPARK_OPERATORS = {
    "SparkSubmitOperator",
    "SparkSqlOperator",
    "DataprocSubmitJobOperator",
    "DataprocSubmitSparkJobOperator",
    "DataprocCreateBatchOperator",
    "EmrAddStepsOperator",
    "EmrCreateJobFlowOperator",
    "EmrServerlessStartJobRunOperator",
    "DatabricksRunNowOperator",
    "DatabricksSubmitRunOperator",
    "LivyOperator",
    "SparkKubernetesOperator",
    "GluJobOperator",
}

PYTHON_OPERATORS = {
    "PythonOperator",
    "PythonVirtualenvOperator",
    "BranchPythonOperator",
    "ShortCircuitOperator",
}

BASH_OPERATORS = {
    "BashOperator",
    "SSHOperator",
}


def _classify_operator(operator_name: str) -> str:
    if operator_name in SPARK_OPERATORS:
        return "spark"
    if operator_name in PYTHON_OPERATORS:
        return "python"
    if operator_name in BASH_OPERATORS:
        return "bash"
    # Heuristic: if "spark" in the name, it's probably Spark
    if "spark" in operator_name.lower() or "emr" in operator_name.lower():
        return "spark"
    if "notebook" in operator_name.lower():
        return "notebook"
    return "other"


# ── Airflow DAG parsing (AST-based) ─────────────────────────────────────────


def parse_airflow_dag(path: Path) -> PipelineDefinition:
    """Parse an Airflow DAG Python file using AST. No execution."""
    source = path.read_text()
    tree = ast.parse(source, filename=str(path))

    dag_name = path.stem
    tasks: dict[str, PipelineTask] = {}
    # Map variable names to task_id for dependency resolution
    var_to_task: dict[str, str] = {}
    dependencies: list[tuple[str, str]] = []  # (upstream, downstream)

    # Pass 1: find DAG name and task definitions
    for node in ast.walk(tree):
        # Find DAG(...) call for the dag_id
        if isinstance(node, ast.Call):
            func_name = _get_call_name(node)
            if func_name == "DAG":
                for kw in node.keywords:
                    if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant):
                        dag_name = str(kw.value.value)
                if node.args and isinstance(node.args[0], ast.Constant):
                    dag_name = str(node.args[0].value)

    # Pass 2: find task assignments
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and isinstance(node.value, ast.Call):
                var_name = target.id
                call = node.value
                func_name = _get_call_name(call)

                if func_name and func_name[0].isupper():
                    task_id = _extract_kwarg(call, "task_id")
                    if not task_id:
                        task_id = var_name

                    task_type = _classify_operator(func_name)
                    spark_app = _extract_kwarg(call, "application") or _extract_kwarg(call, "name")

                    tasks[task_id] = PipelineTask(
                        name=task_id,
                        type=task_type,
                        operator=func_name,
                        spark_app_name=spark_app,
                    )
                    var_to_task[var_name] = task_id

    # Pass 3: find dependencies (>> and << operators, set_downstream, set_upstream)
    for node in ast.walk(tree):
        # task1 >> task2 or task1 >> [task2, task3]
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
            upstreams = _resolve_task_refs(node.left, var_to_task)
            downstreams = _resolve_task_refs(node.right, var_to_task)
            for u in upstreams:
                for d in downstreams:
                    dependencies.append((u, d))

        elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.LShift):
            downstreams = _resolve_task_refs(node.left, var_to_task)
            upstreams = _resolve_task_refs(node.right, var_to_task)
            for u in upstreams:
                for d in downstreams:
                    dependencies.append((u, d))

    # Apply dependencies
    for upstream, downstream in dependencies:
        if downstream in tasks:
            if upstream not in tasks[downstream].dependencies:
                tasks[downstream].dependencies.append(upstream)

    return PipelineDefinition(
        name=dag_name,
        orchestrator="airflow",
        tasks=list(tasks.values()),
    )


def _get_call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _extract_kwarg(call: ast.Call, key: str) -> str | None:
    for kw in call.keywords:
        if kw.arg == key and isinstance(kw.value, ast.Constant):
            return str(kw.value.value)
    return None


def _resolve_task_refs(node: ast.expr, var_map: dict[str, str]) -> list[str]:
    if isinstance(node, ast.Name) and node.id in var_map:
        return [var_map[node.id]]
    if isinstance(node, ast.List):
        refs = []
        for elt in node.elts:
            refs.extend(_resolve_task_refs(elt, var_map))
        return refs
    # Handle chained >> (a >> b >> c becomes BinOp(BinOp(a, >>, b), >>, c))
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
        return _resolve_task_refs(node.right, var_map)
    return []


# ── Databricks workflow parsing ──────────────────────────────────────────────


def parse_databricks_workflow(path: Path) -> PipelineDefinition:
    """Parse a Databricks workflow JSON export."""
    data = json.loads(path.read_text())

    # Handle both single job and list of jobs
    if isinstance(data, list):
        data = data[0] if data else {}

    job_name = data.get("settings", {}).get("name", path.stem)
    raw_tasks = data.get("settings", {}).get("tasks", [])

    tasks = []
    for t in raw_tasks:
        task_key = t.get("task_key", t.get("task_name", "unknown"))

        # Determine type
        if "spark_jar_task" in t or "spark_submit_task" in t:
            task_type = "spark"
            operator = "SparkJarTask" if "spark_jar_task" in t else "SparkSubmitTask"
        elif "spark_python_task" in t:
            task_type = "spark"
            operator = "SparkPythonTask"
        elif "notebook_task" in t:
            task_type = "notebook"
            operator = "NotebookTask"
        elif "python_wheel_task" in t:
            task_type = "python"
            operator = "PythonWheelTask"
        elif "pipeline_task" in t:
            task_type = "spark"
            operator = "DLTPipelineTask"
        elif "sql_task" in t:
            task_type = "spark"
            operator = "SqlTask"
        else:
            task_type = "other"
            operator = "UnknownTask"

        deps = [
            d.get("task_key", "")
            for d in t.get("depends_on", [])
            if d.get("task_key")
        ]

        tasks.append(PipelineTask(
            name=task_key,
            type=task_type,
            operator=operator,
            dependencies=deps,
        ))

    return PipelineDefinition(
        name=job_name,
        orchestrator="databricks",
        tasks=tasks,
    )


# ── Auto-detect and parse ───────────────────────────────────────────────────


def parse_dag(path: Path) -> PipelineDefinition | None:
    """Auto-detect file type and parse as pipeline definition."""
    if path.suffix == ".py":
        try:
            return parse_airflow_dag(path)
        except SyntaxError:
            return None
    if path.suffix == ".json":
        try:
            data = json.loads(path.read_text())
            # Databricks workflows have "settings.tasks"
            if isinstance(data, dict) and "settings" in data:
                return parse_databricks_workflow(path)
            if isinstance(data, list) and data and "settings" in data[0]:
                return parse_databricks_workflow(path)
        except (json.JSONDecodeError, KeyError):
            pass
    return None
