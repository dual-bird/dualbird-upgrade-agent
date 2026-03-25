"""
DualBird FPGA support matrix — derived directly from spark-plugin source code.

This module encodes exactly which Spark operators, expressions, data types,
aggregate functions, and join types the DualBird FPGA plugin can accelerate.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Supported Spark physical operators -> DualBird FPGA replacements
# ---------------------------------------------------------------------------

SUPPORTED_OPERATORS: dict[str, dict] = {
    "FileSourceScanExec": {"category": "scan", "config": "sql.parquet.read.enabled"},
    "BatchScanExec": {"category": "scan", "config": "sql.parquet.read.enabled"},
    "HashAggregateExec": {"category": "aggregate", "config": "sql.groupBy.enabled"},
    "SortAggregateExec": {"category": "aggregate", "config": "sql.groupBy.enabled"},
    "SortExec": {"category": "sort", "config": "sql.sort.enabled"},
    "WindowExec": {"category": "window", "config": "sql.window.enabled"},
    "SortMergeJoinExec": {"category": "join", "config": "sql.sortMergeJoin.enabled"},
    "ShuffledHashJoinExec": {"category": "join", "config": "sql.sortMergeJoin.enabled"},
    "FilterExec": {"category": "filter", "config": "sql.filter.enabled"},
    "ProjectExec": {"category": "project", "config": "sql.project.enabled"},
    "ShuffleExchangeExec": {"category": "shuffle", "config": "sql.shuffle.enabled"},
    "DataWritingCommandExec": {
        "category": "write",
        "config": "sql.parquet.write.enabled",
    },
    "WriteFilesExec": {"category": "write", "config": "sql.parquet.write.enabled"},
    "V2ExistingTableWriteExec": {
        "category": "write",
        "config": "sql.parquet.write.enabled",
    },
    "InsertIntoHadoopFsRelationCommand": {
        "category": "write",
        "config": "sql.parquet.write.enabled",
    },
    "AppendDataExec": {"category": "write", "config": "sql.parquet.write.enabled"},
    "OverwritePartitionsDynamicExec": {
        "category": "write",
        "config": "sql.parquet.write.enabled",
    },
}

# Operators transparent to the FPGA plugin — they don't affect acceleration
# decisions. These are wrappers, adapters, or AQE infrastructure.
#
# NOTE: BroadcastHashJoinExec and BroadcastNestedLoopJoinExec are NOT
# passthrough — they are genuine unaccelerated join operators. The FPGA plugin
# only accelerates SortMergeJoinExec and ShuffledHashJoinExec.
PASSTHROUGH_OPERATORS: set[str] = {
    "WholeStageCodegen",
    "InputAdapter",
    "Exchange",
    "BroadcastExchange",
    "SubqueryExec",
    "ReusedExchangeExec",
    "ReusedSubqueryExec",
    "AdaptiveSparkPlanExec",
    "CustomShuffleReaderExec",
    "AQEShuffleReadExec",
    "ColumnarToRowExec",
    "RowToColumnarExec",
    "CollectLimitExec",
    "TakeOrderedAndProjectExec",
    "CoalesceExec",
    "UnionExec",
    "LocalTableScanExec",
    "RangeExec",
    "InMemoryTableScanExec",
    "ResultQueryStageExec",
    "ShuffleQueryStageExec",
    "BroadcastQueryStageExec",
    "TableCacheQueryStageExec",
}

SUPPORTED_AGG_FUNCTIONS: set[str] = {
    "sum",
    "count",
    "count_if",
    "avg",
    "average",
    "min",
    "max",
    "first",
    "last",
    "any_value",
    "bool_and",
    "bool_or",
    "bit_and",
    "bit_or",
    "bit_xor",
}

SUPPORTED_WINDOW_FUNCTIONS: set[str] = {
    "row_number",
    "rank",
    "dense_rank",
    "percent_rank",
    "cume_dist",
    "ntile",
} | SUPPORTED_AGG_FUNCTIONS

# Window functions explicitly NOT supported. The plugin's FpgaWindowExec has a
# TODO for AggregateWindowFunction support — these fall back to Spark.
UNSUPPORTED_WINDOW_FUNCTIONS: set[str] = {
    "lag",
    "lead",
    "nth_value",
}

SUPPORTED_JOIN_TYPES: set[str] = {
    "Inner",
    "LeftSemi",
    "LeftAnti",
    "LeftOuter",
    "RightOuter",
    "FullOuter",
}

SUPPORTED_PRIMITIVE_TYPES: set[str] = {
    "boolean",
    "bool",
    "byte",
    "tinyint",
    "short",
    "smallint",
    "int",
    "integer",
    "long",
    "bigint",
    "float",
    "double",
    "string",
    "date",
    "timestamp",
    "timestamp_ntz",
}

MAX_DECIMAL_PRECISION = 18

# Expressions the FPGA converter explicitly handles (from SparkToFpgaExpressionConverter).
# Anything NOT in this set triggers fallback to Spark CPU execution.
SUPPORTED_EXPRESSIONS: set[str] = {
    # Binary arithmetic
    "Add",
    "Subtract",
    "Multiply",
    "Divide",
    # Bitwise
    "ShiftLeft",
    "ShiftRight",
    "ShiftRightUnsigned",
    "BitwiseXor",
    # Comparison
    "LessThan",
    "LessThanOrEqual",
    "EqualTo",
    "EqualNullSafe",
    "GreaterThan",
    "GreaterThanOrEqual",
    # Logical
    "And",
    "Or",
    "Not",
    # Unary
    "UnaryMinus",
    "IsNull",
    "IsNotNull",
    "IsNaN",
    "NormalizeNaNAndZero",
    # Misc
    "Coalesce",
    "Cast",
    "CaseWhen",
    "DateFormatClass",
    "SortOrder",
    "AttributeReference",
    "Literal",
    "Alias",
}

# Common expressions known to trigger fallback. Not exhaustive — any expression
# not in SUPPORTED_EXPRESSIONS will fall back, but these are the ones users hit
# most often.
UNSUPPORTED_EXPRESSIONS: set[str] = {
    # UDFs
    "ScalaUDF",
    "PythonUDF",
    "HiveGenericUDF",
    "HiveSimpleUDF",
    "JavaUDF",
    # JSON
    "JsonTuple",
    "GetJsonObject",
    # String functions
    "RegExpReplace",
    "RegExpExtract",
    "StringSplit",
    "StringTrim",
    "StringTrimLeft",
    "StringTrimRight",
    "Substring",
    "Concat",
    "ConcatWs",
    "Upper",
    "Lower",
    "Length",
    "Like",
    "RLike",
    # Date/time functions (DateFormatClass IS supported, but these are not)
    "CurrentDate",
    "CurrentTimestamp",
    "FromUnixTime",
    "UnixTimestamp",
    "ToUnixTimestamp",
    "DateAdd",
    "DateSub",
    "DateDiff",
    "Year",
    "Month",
    "DayOfMonth",
    "Hour",
    "Minute",
    "Second",
    # Math functions
    "Floor",
    "Ceil",
    "Round",
    "Abs",
    "Sqrt",
    "Power",
    "Log",
    "Exp",
    "Rand",
    "Randn",
    "Greatest",
    "Least",
    # Complex type constructors / accessors
    "CreateNamedStruct",
    "CreateArray",
    "CreateMap",
    "GetStructField",
    "GetArrayItem",
    "GetMapValue",
    "ArrayContains",
    "ArrayDistinct",
    "ArraySort",
    "MapKeys",
    "MapValues",
    # Generators
    "Explode",
    "PosExplode",
    "Generator",
    # Unsupported aggregate functions
    "CollectList",
    "CollectSet",
    "ApproxCountDistinct",
    "HyperLogLogPlusPlus",
    "Percentile",
    "ApproxPercentile",
    "StddevPop",
    "StddevSamp",
    "VariancePop",
    "VarianceSamp",
    "Corr",
    "CovPopulation",
    "CovSample",
}


# ---------------------------------------------------------------------------
# Acceleration factor estimates (conservative defaults)
# ---------------------------------------------------------------------------


@dataclass
class AccelerationProfile:
    min_factor: float
    typical_factor: float
    max_factor: float
    confidence: str  # "high" | "medium" | "low"


DEFAULT_ACCELERATION_FACTORS: dict[str, AccelerationProfile] = {
    "sort": AccelerationProfile(2.0, 4.0, 10.0, "high"),
    "shuffle": AccelerationProfile(1.5, 3.0, 6.0, "high"),
    "join": AccelerationProfile(1.5, 3.0, 8.0, "medium"),
    "aggregate": AccelerationProfile(1.5, 3.0, 6.0, "high"),
    "window": AccelerationProfile(1.5, 2.5, 5.0, "medium"),
    "scan": AccelerationProfile(1.5, 2.5, 4.0, "high"),
    "write": AccelerationProfile(1.5, 2.5, 4.0, "medium"),
    "filter": AccelerationProfile(1.2, 1.8, 3.0, "high"),
    "project": AccelerationProfile(1.1, 1.5, 2.5, "medium"),
}


# ---------------------------------------------------------------------------
# Support checking
# ---------------------------------------------------------------------------


@dataclass
class SupportVerdict:
    supported: bool
    category: str
    operator_name: str
    fallback_reasons: list[str] = field(default_factory=list)
    expressions_supported: bool = True
    types_supported: bool = True


def normalize_operator_name(raw_name: str) -> str:
    name = raw_name.strip()
    name = re.sub(r"^[*()0-9\s]+", "", name)
    name = re.split(r"\s*\(", name, maxsplit=1)[0].strip()
    return name


def check_operator(node_name: str) -> SupportVerdict:
    clean = normalize_operator_name(node_name)
    if clean in SUPPORTED_OPERATORS:
        info = SUPPORTED_OPERATORS[clean]
        return SupportVerdict(
            supported=True, category=info["category"], operator_name=clean
        )
    if clean in PASSTHROUGH_OPERATORS:
        return SupportVerdict(
            supported=True, category="passthrough", operator_name=clean
        )
    return SupportVerdict(
        supported=False,
        category="other",
        operator_name=clean,
        fallback_reasons=[f"Unsupported operator: {clean}"],
    )


def check_agg_function(func_name: str) -> bool:
    return func_name.lower().strip() in SUPPORTED_AGG_FUNCTIONS


def check_window_function(func_name: str) -> bool:
    return func_name.lower().strip() in SUPPORTED_WINDOW_FUNCTIONS


def check_join_type(join_type: str) -> bool:
    return join_type.strip() in SUPPORTED_JOIN_TYPES


def check_data_type(type_str: str) -> tuple[bool, str | None]:
    t = type_str.lower().strip()
    if t in SUPPORTED_PRIMITIVE_TYPES:
        return True, None
    if t.startswith("decimal"):
        m = re.search(r"decimal\((\d+)", t)
        if m:
            precision = int(m.group(1))
            if precision <= MAX_DECIMAL_PRECISION:
                return True, None
            return False, f"Decimal precision {precision} > {MAX_DECIMAL_PRECISION}"
        return True, None
    if t.startswith("array<"):
        inner = t[6:].rstrip(">")
        if any(inner.startswith(c) for c in ("array<", "map<", "struct<")):
            return False, "Nested composite types not supported"
        return check_data_type(inner)
    if t.startswith("map<"):
        inner = t[4:].rstrip(">")
        parts = inner.split(",", 1)
        if len(parts) == 2:
            ok_k, r_k = check_data_type(parts[0].strip())
            ok_v, r_v = check_data_type(parts[1].strip())
            if not ok_k:
                return False, f"Map key: {r_k}"
            if not ok_v:
                return False, f"Map value: {r_v}"
        return True, None
    if t.startswith("struct<"):
        return True, None
    if "interval" in t or "calendar" in t:
        return False, "CalendarIntervalType not supported"
    return False, f"Unknown type: {type_str}"
