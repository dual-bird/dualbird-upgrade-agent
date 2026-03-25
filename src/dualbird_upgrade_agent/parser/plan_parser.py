"""
Parse and analyze Spark physical plan trees.

Extracts operator categories, expression details, aggregate functions,
join types, and data types from SparkPlanInfo nodes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from dualbird_upgrade_agent.parser.event_log import SparkPlanNode
from dualbird_upgrade_agent.matrix.support import (
    SupportVerdict,
    check_operator,
    check_agg_function,
    check_window_function,
    check_join_type,
    check_data_type,
    UNSUPPORTED_EXPRESSIONS,
)


@dataclass
class AnalyzedOperator:
    """A plan node with its DualBird support verdict."""
    node_name: str
    simple_string: str
    category: str
    accelerable: bool
    expressions_supported: bool = True
    types_supported: bool = True
    fallback_reasons: list[str] = field(default_factory=list)
    children: list[AnalyzedOperator] = field(default_factory=list)


@dataclass
class PlanAnalysis:
    """Full analysis of a SQL execution's physical plan."""
    operators: list[AnalyzedOperator] = field(default_factory=list)
    total_operators: int = 0
    accelerable_operators: int = 0
    passthrough_operators: int = 0
    unsupported_operators: int = 0
    category_counts: dict[str, int] = field(default_factory=dict)
    all_fallback_reasons: list[dict] = field(default_factory=list)


def _extract_agg_functions(simple_string: str) -> list[str]:
    """
    Extract aggregate function names from a HashAggregateExec simpleString.

    Example: "HashAggregate(keys=[ch#10], functions=[sum(amt#15), count(1)])"
    -> ["sum", "count"]
    """
    match = re.search(r"functions=\[(.+?)\]", simple_string)
    if not match:
        return []
    funcs_str = match.group(1)
    # Extract function names before the first paren
    raw = re.findall(r"(\w+)\s*\(", funcs_str)
    # Strip Spark's partial_/merge_ prefixes (internal aggregate modes)
    return [re.sub(r"^(partial_|merge_)", "", f) for f in raw]


def _extract_window_functions(simple_string: str) -> list[str]:
    """Extract window function names from WindowExec simpleString."""
    # WindowExec [row_number() windowspecdefinition(...)]
    return re.findall(r"(\w+)\s*\(", simple_string)


def _extract_join_info(simple_string: str) -> tuple[str | None, bool]:
    """
    Extract join type and whether there's a non-trivial condition.

    Returns (join_type, has_condition).
    """
    # "SortMergeJoin [col1#5], [col2#10], Inner"
    # "SortMergeJoin [col1#5], [col2#10], Inner, (a#1 > b#2)"
    join_type = None
    for jt in ("Inner", "LeftSemi", "LeftAnti", "LeftOuter",
                "RightOuter", "FullOuter", "Cross"):
        if jt in simple_string:
            join_type = jt
            break

    # Check for non-trivial condition (anything after the join type)
    has_condition = False
    if join_type:
        idx = simple_string.find(join_type)
        after = simple_string[idx + len(join_type):].strip().strip(",").strip()
        # If there's something after the join type that looks like a condition
        if after and after.startswith("("):
            has_condition = True

    return join_type, has_condition


def _extract_scan_format(simple_string: str) -> str | None:
    """Extract file format from scan operator."""
    for fmt in ("parquet", "orc", "json", "csv", "avro", "text"):
        if fmt in simple_string.lower():
            return fmt
    return None


def _check_expressions_in_string(simple_string: str) -> tuple[bool, list[str]]:
    """
    Check if expressions referenced in a simpleString are supported.
    Returns (all_supported, list_of_reasons).
    """
    reasons = []
    for expr_name in UNSUPPORTED_EXPRESSIONS:
        # Check for function-call style: funcName(
        pattern = rf"\b{re.escape(expr_name)}\s*\("
        if re.search(pattern, simple_string, re.IGNORECASE):
            reasons.append(f"Unsupported expression: {expr_name}")
    return len(reasons) == 0, reasons


def _extract_data_types(simple_string: str) -> list[str]:
    """Extract data type references from operator strings."""
    types = []
    # Match patterns like "decimal(38,10)" or column refs with types
    decimal_matches = re.findall(r"decimal\(\d+,\s*\d+\)", simple_string, re.IGNORECASE)
    types.extend(decimal_matches)
    return types


def analyze_node(node: SparkPlanNode) -> AnalyzedOperator:
    """Analyze a single plan node and its children recursively."""
    verdict = check_operator(node.node_name)
    fallback_reasons = list(verdict.fallback_reasons)
    expressions_ok = True
    types_ok = True

    # Deep checks based on operator category
    if verdict.category == "aggregate":
        agg_funcs = _extract_agg_functions(node.simple_string)
        for func in agg_funcs:
            if not check_agg_function(func):
                fallback_reasons.append(f"Unsupported aggregate function: {func}")
                expressions_ok = False

    elif verdict.category == "window":
        win_funcs = _extract_window_functions(node.simple_string)
        for func in win_funcs:
            if not check_window_function(func):
                fallback_reasons.append(f"Unsupported window function: {func}")
                expressions_ok = False

    elif verdict.category == "join":
        join_type, has_condition = _extract_join_info(node.simple_string)
        if join_type and not check_join_type(join_type):
            fallback_reasons.append(f"Unsupported join type: {join_type}")
            expressions_ok = False
        if has_condition:
            fallback_reasons.append("Non-trivial join condition")
            expressions_ok = False

    elif verdict.category == "scan":
        fmt = _extract_scan_format(node.simple_string)
        if fmt and fmt != "parquet":
            fallback_reasons.append(f"Non-Parquet format: {fmt}")
            expressions_ok = False

    elif verdict.category in ("filter", "project"):
        expr_ok, expr_reasons = _check_expressions_in_string(node.simple_string)
        if not expr_ok:
            fallback_reasons.extend(expr_reasons)
            expressions_ok = False

    # Check data types in any operator
    data_types = _extract_data_types(node.simple_string)
    for dt in data_types:
        dt_ok, dt_reason = check_data_type(dt)
        if not dt_ok:
            fallback_reasons.append(dt_reason)
            types_ok = False

    # UDF detection in any operator
    for udf_marker in ("PythonUDF", "ScalaUDF", "HiveGenericUDF", "HiveSimpleUDF", "JavaUDF"):
        if udf_marker in node.simple_string or udf_marker in node.node_name:
            fallback_reasons.append(f"{udf_marker} detected — cannot accelerate")
            expressions_ok = False

    # Final verdict: supported only if operator + expressions + types all pass
    is_accelerable = (
        verdict.supported
        and verdict.category != "passthrough"
        and expressions_ok
        and types_ok
        and len(fallback_reasons) == 0
    )

    # Recurse into children
    analyzed_children = [analyze_node(c) for c in node.children]

    return AnalyzedOperator(
        node_name=verdict.operator_name,
        simple_string=node.simple_string,
        category=verdict.category if verdict.category != "passthrough" else _infer_passthrough_category(node),
        accelerable=is_accelerable,
        expressions_supported=expressions_ok,
        types_supported=types_ok,
        fallback_reasons=fallback_reasons,
        children=analyzed_children,
    )


def _infer_passthrough_category(node: SparkPlanNode) -> str:
    """For passthrough operators, infer category from context."""
    name = node.node_name.lower()
    if "codegen" in name:
        return "codegen"
    if "exchange" in name or "shuffle" in name:
        return "exchange"
    if "broadcast" in name:
        return "broadcast"
    return "passthrough"


def analyze_plan(root: SparkPlanNode) -> PlanAnalysis:
    """Analyze a complete physical plan tree."""
    analysis = PlanAnalysis()
    root_op = analyze_node(root)
    analysis.operators = [root_op]

    # Flatten and count
    def walk(op: AnalyzedOperator):
        analysis.total_operators += 1
        cat = op.category
        analysis.category_counts[cat] = analysis.category_counts.get(cat, 0) + 1

        if op.accelerable:
            analysis.accelerable_operators += 1
        elif op.category in ("passthrough", "codegen", "exchange", "broadcast"):
            analysis.passthrough_operators += 1
        else:
            analysis.unsupported_operators += 1
            for reason in op.fallback_reasons:
                analysis.all_fallback_reasons.append({
                    "operator": op.node_name,
                    "reason": reason,
                })

        for child in op.children:
            walk(child)

    walk(root_op)
    return analysis


def flatten_operators(op: AnalyzedOperator) -> list[AnalyzedOperator]:
    """Flatten operator tree into a list (pre-order traversal)."""
    result = [op]
    for child in op.children:
        result.extend(flatten_operators(child))
    return result
