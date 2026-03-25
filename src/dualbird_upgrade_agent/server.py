"""MCP server exposing DualBird Estimator analysis as tools."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from dualbird_estimator.analyzer.estimator import estimate_application
from dualbird_estimator.matrix.support import (
    MAX_DECIMAL_PRECISION,
    PASSTHROUGH_OPERATORS,
    SUPPORTED_AGG_FUNCTIONS,
    SUPPORTED_EXPRESSIONS,
    SUPPORTED_JOIN_TYPES,
    SUPPORTED_OPERATORS,
    SUPPORTED_PRIMITIVE_TYPES,
    SUPPORTED_WINDOW_FUNCTIONS,
    UNSUPPORTED_EXPRESSIONS,
    check_data_type,
    check_operator,
)
from dualbird_estimator.parser.event_log import parse_event_logs
from dualbird_estimator.parser.pyspark_parser import (
    analysis_to_operators,
    analyze_pyspark_file,
)
from dualbird_estimator.report.generator import generate_report

mcp = FastMCP(
    "dualbird-upgrade-agent",
    instructions=(
        "You are a DualBird Spark Upgrade Agent. You help users enable DualBird "
        "FPGA acceleration on their existing Spark pipelines. This is a "
        "configuration-only change — no application code modifications are needed. "
        "Use the tools to assess workload compatibility, generate recommended "
        "Spark configurations, and validate acceleration coverage."
    ),
)


# ---------------------------------------------------------------------------
# Tool: analyze_pyspark_code
# ---------------------------------------------------------------------------


@mcp.tool()
def analyze_pyspark_code(
    source_code: str = "",
    file_path: str = "",
) -> str:
    """Analyze PySpark source code for DualBird FPGA acceleration compatibility.

    Performs static analysis on PySpark code to predict which Spark physical
    operators will be produced and whether DualBird can accelerate them.
    Identifies UDFs, unsupported aggregate functions, and other blockers.

    Provide either source_code (inline PySpark code) or file_path (path to a .py file).
    Returns JSON with predicted operators, coverage assessment, and recommended config.
    """
    if not source_code and not file_path:
        return json.dumps({"error": "Provide either source_code or file_path"})

    if source_code:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False
        ) as tmp:
            tmp.write(source_code)
            tmp.flush()
            path = Path(tmp.name)
    else:
        path = Path(file_path)
        if not path.exists():
            return json.dumps({"error": f"File not found: {file_path}"})

    analysis = analyze_pyspark_file(path)
    operators = analysis_to_operators(analysis)

    accelerable = [op for op in operators if op["accelerable"]]
    blocked = [op for op in operators if not op["accelerable"]]
    categories = list({op["category"] for op in accelerable})

    # Generate recommended config for discovered categories
    config = _build_config(categories)

    return json.dumps(
        {
            "file": str(path),
            "total_operators": len(operators),
            "accelerable_count": len(accelerable),
            "blocked_count": len(blocked),
            "has_udfs": analysis.has_udfs,
            "has_pandas_udfs": analysis.has_pandas_udfs,
            "confidence": analysis.confidence,
            "operators": operators,
            "blocked_details": [
                {"category": op["category"], "reason": op["fallback_reason"]}
                for op in blocked
            ],
            "recommended_config": config,
        },
        indent=2,
    )


# ---------------------------------------------------------------------------
# Tool: analyze_spark_event_log
# ---------------------------------------------------------------------------


@mcp.tool()
def analyze_spark_event_log(file_path: str) -> str:
    """Analyze Spark event log(s) for DualBird FPGA acceleration compatibility.

    Parses Spark event logs (.jsonl) to extract physical plan trees, stage
    metrics, and operator details. Produces a full acceleration coverage report
    with per-stage verdicts, fallback reasons, and recommended DualBird config.

    This gives the most accurate assessment since it uses actual execution data
    rather than static code analysis.

    Args:
        file_path: Path to a Spark event log file (.jsonl), or a directory
                   containing multiple event log files.
    """
    path = Path(file_path)
    if not path.exists():
        return json.dumps({"error": f"Path not found: {file_path}"})

    if path.is_dir():
        log_files = sorted(path.glob("*.jsonl")) + sorted(path.glob("*.json"))
        if not log_files:
            return json.dumps({"error": f"No .jsonl/.json files found in {file_path}"})
        paths = log_files
    else:
        paths = [path]

    apps = parse_event_logs(paths)
    if not apps:
        return json.dumps({"error": "No Spark applications found in the event log(s)"})

    estimates = [estimate_application(app) for app in apps]
    report = generate_report(estimates)

    return json.dumps(report, indent=2, default=str)


# ---------------------------------------------------------------------------
# Tool: check_operator_support
# ---------------------------------------------------------------------------


@mcp.tool()
def check_operator_support(operator_name: str) -> str:
    """Check whether a specific Spark physical operator is supported by DualBird FPGA.

    Returns support status, operator category, and any fallback reasons.
    Use this to quickly verify if a particular operator (e.g., SortExec,
    HashAggregateExec, WindowExec) can be accelerated.

    Args:
        operator_name: Spark physical operator name (e.g., "SortExec", "HashAggregateExec").
    """
    verdict = check_operator(operator_name)
    result = {
        "operator": verdict.operator_name,
        "supported": verdict.supported,
        "category": verdict.category,
        "fallback_reasons": verdict.fallback_reasons,
    }

    if verdict.supported and verdict.category != "passthrough":
        config_key = SUPPORTED_OPERATORS.get(verdict.operator_name, {}).get("config")
        if config_key:
            result["config_key"] = f"spark.dualbird.{config_key}"

    return json.dumps(result, indent=2)


# ---------------------------------------------------------------------------
# Tool: check_expression_support
# ---------------------------------------------------------------------------


@mcp.tool()
def check_expression_support(expression_name: str) -> str:
    """Check whether a Spark Catalyst expression is supported by DualBird FPGA.

    Even when an operator is supported, the FPGA may not handle all expressions
    within it. Use this to check specific expression types like UDFs, string
    functions, date functions, etc.

    Args:
        expression_name: Catalyst expression type (e.g., "RegExpReplace", "ScalaUDF", "Add").
    """
    name = expression_name.strip()
    supported = name in SUPPORTED_EXPRESSIONS
    known_unsupported = name in UNSUPPORTED_EXPRESSIONS

    result = {
        "expression": name,
        "supported": supported,
        "known_unsupported": known_unsupported,
    }

    if not supported:
        if known_unsupported:
            result["note"] = (
                f"{name} is explicitly unsupported by the FPGA expression converter. "
                "Operators containing this expression will fall back to Spark CPU."
            )
        else:
            result["note"] = (
                f"{name} is not in the supported expressions set. "
                "It will likely cause fallback to Spark CPU execution."
            )

    return json.dumps(result, indent=2)


# ---------------------------------------------------------------------------
# Tool: get_recommended_config
# ---------------------------------------------------------------------------


@mcp.tool()
def get_recommended_config(
    categories: list[str] | None = None,
    platform: str = "generic",
) -> str:
    """Generate recommended DualBird Spark configuration for FPGA acceleration.

    Produces the spark.dualbird.* configuration keys needed to enable
    acceleration for the given operator categories. If no categories are
    provided, enables all supported acceleration features.

    Args:
        categories: Operator categories to enable (e.g., ["sort", "join", "aggregate"]).
                    If empty/null, enables all categories.
        platform: Target platform for formatted output. One of:
                  "generic" (key=value), "emr" (EMR configuration JSON),
                  "databricks" (cluster spark config), "spark-submit" (--conf flags).
    """
    all_categories = {
        "sort", "join", "aggregate", "window", "filter",
        "project", "shuffle", "scan", "write",
    }
    enabled = set(categories) if categories else all_categories

    config = _build_config(list(enabled))

    result: dict = {"config": config}

    # Platform-specific formatting
    if platform == "emr":
        result["emr_configuration"] = [
            {
                "Classification": "spark-defaults",
                "Properties": {k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in config.items()},
            }
        ]
        result["note"] = (
            "Add this to your EMR cluster configuration JSON, or pass via "
            "--configurations flag to aws emr create-cluster."
        )
    elif platform == "databricks":
        result["databricks_config"] = {
            k: str(v).lower() if isinstance(v, bool) else str(v)
            for k, v in config.items()
        }
        result["note"] = (
            "Add these to your Databricks cluster Spark Config "
            "(Compute > Edit > Advanced Options > Spark Config)."
        )
    elif platform == "spark-submit":
        result["spark_submit_args"] = " \\\n  ".join(
            f"--conf {k}={str(v).lower() if isinstance(v, bool) else v}"
            for k, v in config.items()
        )
        result["note"] = "Add these --conf flags to your spark-submit command."
    else:
        result["spark_defaults_conf"] = "\n".join(
            f"{k} {str(v).lower() if isinstance(v, bool) else v}"
            for k, v in config.items()
        )
        result["note"] = (
            "Add these to spark-defaults.conf, or pass as --conf flags to spark-submit."
        )

    return json.dumps(result, indent=2)


# ---------------------------------------------------------------------------
# Tool: get_support_matrix
# ---------------------------------------------------------------------------


@mcp.tool()
def get_support_matrix() -> str:
    """Return the full DualBird FPGA support matrix.

    Lists all supported Spark physical operators, aggregate functions, window
    functions, join types, data types, expressions, and known unsupported
    expressions. Use this as a reference when assessing workload compatibility.
    """
    return json.dumps(
        {
            "supported_operators": {
                name: info for name, info in SUPPORTED_OPERATORS.items()
            },
            "passthrough_operators": sorted(PASSTHROUGH_OPERATORS),
            "supported_agg_functions": sorted(SUPPORTED_AGG_FUNCTIONS),
            "supported_window_functions": sorted(SUPPORTED_WINDOW_FUNCTIONS),
            "supported_join_types": sorted(SUPPORTED_JOIN_TYPES),
            "supported_primitive_types": sorted(SUPPORTED_PRIMITIVE_TYPES),
            "max_decimal_precision": MAX_DECIMAL_PRECISION,
            "supported_expressions": sorted(SUPPORTED_EXPRESSIONS),
            "unsupported_expressions": sorted(UNSUPPORTED_EXPRESSIONS),
        },
        indent=2,
    )


# ---------------------------------------------------------------------------
# Tool: check_data_type_support
# ---------------------------------------------------------------------------


@mcp.tool()
def check_data_type_support(data_type: str) -> str:
    """Check whether a Spark data type is supported by DualBird FPGA.

    Validates primitive types, Decimal precision, and composite types
    (Array, Map, Struct). Nested composite types are not supported.

    Args:
        data_type: Spark SQL data type string (e.g., "string", "decimal(38,10)",
                   "array<int>", "struct<name:string,age:int>").
    """
    supported, reason = check_data_type(data_type)
    result = {
        "data_type": data_type,
        "supported": supported,
    }
    if reason:
        result["reason"] = reason
    return json.dumps(result, indent=2)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_OP_CONFIG_MAP = {
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


def _build_config(categories: list[str]) -> dict:
    config: dict = {
        "spark.plugins": "com.dualbird.spark.FpgaPlugin",
        "spark.dualbird.enabled": True,
    }
    for cat in sorted(set(categories)):
        key = _OP_CONFIG_MAP.get(cat)
        if key:
            config[key] = True
    config["spark.dualbird.tracing.level"] = "info"
    return config
