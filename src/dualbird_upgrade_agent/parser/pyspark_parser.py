"""
Static analysis of PySpark code to predict operator categories.

Scans Python files for PySpark API calls and SQL strings to predict
what Spark physical plan operators the job will produce. Results are
marked as "predicted" — only event logs give "observed" ground truth.

This maps directly to DualBird plugin capabilities:
  .groupBy/.agg      → aggregate  (HashAggregateExec)
  .join               → join       (SortMergeJoinExec)
  .orderBy/.sort      → sort       (SortExec)
  .filter/.where      → filter     (FilterExec)
  .select/.withColumn → project    (ProjectExec)
  .read.parquet       → scan       (FileSourceScanExec)
  .write.parquet      → write      (WriteFilesExec)
  .repartition        → shuffle    (ShuffleExchangeExec)
  .over(Window...)    → window     (WindowExec)
  udf/pandas_udf      → FALLBACK
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class PredictedOperator:
    category: str          # sort, shuffle, join, aggregate, etc.
    source: str            # the code snippet that triggered this
    line: int              # line number in the source file
    accelerable: bool      # whether DualBird supports this
    fallback_reason: str | None = None


@dataclass
class PySparkAnalysis:
    file_path: str
    predicted_ops: list[PredictedOperator] = field(default_factory=list)
    has_udfs: bool = False
    has_pandas_udfs: bool = False
    sql_strings: list[str] = field(default_factory=list)
    confidence: str = "low"  # "low" | "medium" — never "high", that's event logs


# PySpark method → (category, accelerable)
PYSPARK_METHOD_MAP: dict[str, tuple[str, bool]] = {
    # Aggregation
    "groupBy": ("aggregate", True),
    "groupby": ("aggregate", True),
    "agg": ("aggregate", True),
    "count": ("aggregate", True),
    "sum": ("aggregate", True),
    "avg": ("aggregate", True),
    "mean": ("aggregate", True),
    "min": ("aggregate", True),
    "max": ("aggregate", True),
    "rollup": ("aggregate", True),
    "cube": ("aggregate", True),
    # Sort
    "orderBy": ("sort", True),
    "sort": ("sort", True),
    "sortWithinPartitions": ("sort", True),
    # Join
    "join": ("join", True),
    "crossJoin": ("join", True),
    # Filter
    "filter": ("filter", True),
    "where": ("filter", True),
    # Project
    "select": ("project", True),
    "withColumn": ("project", True),
    "withColumnRenamed": ("project", True),
    "drop": ("project", True),
    "alias": ("project", True),
    # Shuffle
    "repartition": ("shuffle", True),
    "repartitionByRange": ("shuffle", True),
    "coalesce": ("shuffle", True),
    # Window
    "over": ("window", True),
    # I/O
    "parquet": ("scan", True),  # could be read or write
    "load": ("scan", True),
    "save": ("write", True),
    "saveAsTable": ("write", True),
    "insertInto": ("write", True),
    # Fallback triggers
    "udf": ("udf", False),
    "pandas_udf": ("udf", False),
    "mapInPandas": ("udf", False),
    "mapInArrow": ("udf", False),
    "applyInPandas": ("udf", False),
    "toPandas": ("udf", False),
    "foreach": ("udf", False),
    "foreachBatch": ("udf", False),
    "mapPartitions": ("udf", False),
    # Unsupported agg functions
    "approx_count_distinct": ("aggregate", False),
    "approxCountDistinct": ("aggregate", False),
    "collect_list": ("aggregate", False),
    "collect_set": ("aggregate", False),
    "percentile": ("aggregate", False),
    "percentile_approx": ("aggregate", False),
    "stddev": ("aggregate", False),
    "stddev_pop": ("aggregate", False),
    "stddev_samp": ("aggregate", False),
    "variance": ("aggregate", False),
    "var_pop": ("aggregate", False),
    "var_samp": ("aggregate", False),
    "corr": ("aggregate", False),
    "covar_pop": ("aggregate", False),
    "covar_samp": ("aggregate", False),
}

# SQL patterns → (category, accelerable)
SQL_PATTERNS: list[tuple[str, str, bool]] = [
    (r"\bGROUP\s+BY\b", "aggregate", True),
    (r"\bORDER\s+BY\b", "sort", True),
    (r"\bSORT\s+BY\b", "sort", True),
    (r"\bJOIN\b", "join", True),
    (r"\bLEFT\s+(OUTER\s+)?JOIN\b", "join", True),
    (r"\bRIGHT\s+(OUTER\s+)?JOIN\b", "join", True),
    (r"\bFULL\s+(OUTER\s+)?JOIN\b", "join", True),
    (r"\bCROSS\s+JOIN\b", "join", True),
    (r"\bWHERE\b", "filter", True),
    (r"\bHAVING\b", "filter", True),
    (r"\bWINDOW\b", "window", True),
    (r"\bOVER\s*\(", "window", True),
    (r"\bROW_NUMBER\s*\(", "window", True),
    (r"\bRANK\s*\(", "window", True),
    (r"\bDENSE_RANK\s*\(", "window", True),
    (r"\bSUM\s*\(", "aggregate", True),
    (r"\bCOUNT\s*\(", "aggregate", True),
    (r"\bAVG\s*\(", "aggregate", True),
    (r"\bMIN\s*\(", "aggregate", True),
    (r"\bMAX\s*\(", "aggregate", True),
    (r"\bAPPROX_COUNT_DISTINCT\s*\(", "aggregate", False),
    (r"\bCOLLECT_LIST\s*\(", "aggregate", False),
    (r"\bCOLLECT_SET\s*\(", "aggregate", False),
    (r"\bPERCENTILE\s*\(", "aggregate", False),
    (r"\bSTDDEV\s*\(", "aggregate", False),
    (r"\bVARIANCE\s*\(", "aggregate", False),
    (r"\bREGEXP_REPLACE\s*\(", "project", False),
    (r"\bREGEXP_EXTRACT\s*\(", "project", False),
    (r"\bFROM\s+(\w+\.)*\w+", "scan", True),
    (r"\bINSERT\s+(INTO|OVERWRITE)\b", "write", True),
    (r"\bCREATE\s+TABLE\b.*\bAS\s+SELECT\b", "write", True),
]


def analyze_pyspark_file(path: Path) -> PySparkAnalysis:
    """Analyze a PySpark Python file for predicted operator usage."""
    source = path.read_text()
    analysis = PySparkAnalysis(file_path=str(path))

    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return analysis

    # Walk AST for method calls
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            method_name = _get_method_name(node)
            if method_name and method_name in PYSPARK_METHOD_MAP:
                cat, accel = PYSPARK_METHOD_MAP[method_name]
                line = getattr(node, "lineno", 0)
                snippet = _get_line(source, line)

                if method_name in ("udf", "pandas_udf"):
                    analysis.has_udfs = True
                    if method_name == "pandas_udf":
                        analysis.has_pandas_udfs = True

                analysis.predicted_ops.append(PredictedOperator(
                    category=cat,
                    source=snippet,
                    line=line,
                    accelerable=accel,
                    fallback_reason=f"{'UDF' if cat == 'udf' else 'Unsupported function'}: {method_name}" if not accel else None,
                ))

        # Find SQL strings (spark.sql("..."))
        if isinstance(node, ast.Call):
            func_name = _get_method_name(node)
            if func_name == "sql" and node.args:
                arg = node.args[0]
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    analysis.sql_strings.append(arg.value)

    # Analyze SQL strings
    for sql in analysis.sql_strings:
        for pattern, cat, accel in SQL_PATTERNS:
            if re.search(pattern, sql, re.IGNORECASE):
                analysis.predicted_ops.append(PredictedOperator(
                    category=cat,
                    source=sql[:80],
                    line=0,
                    accelerable=accel,
                    fallback_reason=None if accel else f"Unsupported SQL function in: {sql[:40]}",
                ))

    # Also scan raw source for common patterns (catches f-strings, variables, etc.)
    _scan_raw_patterns(source, analysis)

    # Deduplicate by category
    seen: set[tuple[str, bool]] = set()
    deduped = []
    for op in analysis.predicted_ops:
        key = (op.category, op.accelerable)
        if key not in seen:
            seen.add(key)
            deduped.append(op)
    analysis.predicted_ops = deduped

    # Set confidence
    if analysis.predicted_ops:
        analysis.confidence = "medium" if len(analysis.predicted_ops) >= 3 else "low"

    return analysis


def _get_method_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    if isinstance(node.func, ast.Name):
        return node.func.id
    return None


def _get_line(source: str, lineno: int) -> str:
    lines = source.splitlines()
    if 0 < lineno <= len(lines):
        return lines[lineno - 1].strip()[:100]
    return ""


def _scan_raw_patterns(source: str, analysis: PySparkAnalysis) -> None:
    """Catch patterns that AST might miss (dynamic SQL, f-strings, etc.)."""
    lines = source.splitlines()
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue

        # UDF registration
        if re.search(r"register\s*\(\s*['\"].*['\"].*udf", stripped, re.IGNORECASE):
            if not analysis.has_udfs:
                analysis.has_udfs = True
                analysis.predicted_ops.append(PredictedOperator(
                    category="udf",
                    source=stripped[:100],
                    line=i,
                    accelerable=False,
                    fallback_reason="UDF registration detected",
                ))

        # Read/write parquet
        if ".read.parquet" in stripped or "format('parquet')" in stripped or 'format("parquet")' in stripped:
            analysis.predicted_ops.append(PredictedOperator(
                category="scan", source=stripped[:100], line=i, accelerable=True,
            ))
        if ".write.parquet" in stripped or ".write.mode(" in stripped:
            analysis.predicted_ops.append(PredictedOperator(
                category="write", source=stripped[:100], line=i, accelerable=True,
            ))


def analysis_to_operators(analysis: PySparkAnalysis) -> list[dict]:
    """Convert PySpark analysis to operator dicts matching the report format."""
    ops = []
    for pred in analysis.predicted_ops:
        node_name = {
            "aggregate": "HashAggregateExec",
            "sort": "SortExec",
            "join": "SortMergeJoinExec",
            "filter": "FilterExec",
            "project": "ProjectExec",
            "shuffle": "ShuffleExchangeExec",
            "window": "WindowExec",
            "scan": "FileSourceScanExec",
            "write": "WriteFilesExec",
            "udf": "PythonUDF",
        }.get(pred.category, pred.category)

        ops.append({
            "name": f"[predicted] {pred.source}",
            "node_name": node_name,
            "accelerable": pred.accelerable,
            "category": pred.category,
            "expressions_supported": pred.accelerable,
            "types_supported": True,
            "fallback_reason": pred.fallback_reason,
            "predicted": True,
            "source_line": pred.line,
        })
    return ops
