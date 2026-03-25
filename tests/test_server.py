"""Tests for the DualBird Upgrade Agent MCP server tools."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

from dualbird_upgrade_agent.server import (
    analyze_pyspark_code,
    check_data_type_support,
    check_expression_support,
    check_operator_support,
    get_recommended_config,
    get_support_matrix,
)

# ---------------------------------------------------------------------------
# check_operator_support
# ---------------------------------------------------------------------------


class TestCheckOperatorSupport:
    def test_supported_operator(self):
        result = json.loads(check_operator_support("SortExec"))
        assert result["supported"] is True
        assert result["category"] == "sort"
        assert result["config_key"] == "spark.dualbird.sql.sort.enabled"

    def test_unsupported_operator(self):
        result = json.loads(check_operator_support("SomeCustomExec"))
        assert result["supported"] is False
        assert result["category"] == "other"

    def test_passthrough_operator(self):
        result = json.loads(check_operator_support("WholeStageCodegen"))
        assert result["supported"] is True
        assert result["category"] == "passthrough"

    def test_aggregate_operator(self):
        result = json.loads(check_operator_support("HashAggregateExec"))
        assert result["supported"] is True
        assert result["category"] == "aggregate"
        assert result["config_key"] == "spark.dualbird.sql.groupBy.enabled"

    def test_join_operator(self):
        result = json.loads(check_operator_support("SortMergeJoinExec"))
        assert result["supported"] is True
        assert result["category"] == "join"

    def test_scan_operator(self):
        result = json.loads(check_operator_support("FileSourceScanExec"))
        assert result["supported"] is True
        assert result["category"] == "scan"


# ---------------------------------------------------------------------------
# check_expression_support
# ---------------------------------------------------------------------------


class TestCheckExpressionSupport:
    def test_supported_expression(self):
        result = json.loads(check_expression_support("Add"))
        assert result["supported"] is True
        assert result["known_unsupported"] is False

    def test_unsupported_expression(self):
        result = json.loads(check_expression_support("ScalaUDF"))
        assert result["supported"] is False
        assert result["known_unsupported"] is True
        assert "unsupported" in result["note"].lower()

    def test_unknown_expression(self):
        result = json.loads(check_expression_support("SomeRandomExpr"))
        assert result["supported"] is False
        assert result["known_unsupported"] is False

    def test_cast_supported(self):
        result = json.loads(check_expression_support("Cast"))
        assert result["supported"] is True

    def test_regexp_unsupported(self):
        result = json.loads(check_expression_support("RegExpReplace"))
        assert result["supported"] is False
        assert result["known_unsupported"] is True


# ---------------------------------------------------------------------------
# check_data_type_support
# ---------------------------------------------------------------------------


class TestCheckDataTypeSupport:
    def test_primitive_supported(self):
        result = json.loads(check_data_type_support("string"))
        assert result["supported"] is True

    def test_int_supported(self):
        result = json.loads(check_data_type_support("int"))
        assert result["supported"] is True

    def test_decimal_under_limit(self):
        result = json.loads(check_data_type_support("decimal(18,2)"))
        assert result["supported"] is True

    def test_decimal_over_limit(self):
        result = json.loads(check_data_type_support("decimal(38,10)"))
        assert result["supported"] is False
        assert "precision" in result["reason"].lower()

    def test_array_supported(self):
        result = json.loads(check_data_type_support("array<int>"))
        assert result["supported"] is True

    def test_nested_composite_unsupported(self):
        result = json.loads(check_data_type_support("array<array<int>>"))
        assert result["supported"] is False


# ---------------------------------------------------------------------------
# get_recommended_config
# ---------------------------------------------------------------------------


class TestGetRecommendedConfig:
    def test_specific_categories(self):
        result = json.loads(get_recommended_config(["sort", "join"]))
        config = result["config"]
        assert config["spark.plugins"] == "com.dualbird.spark.FpgaPlugin"
        assert config["spark.dualbird.enabled"] is True
        assert config["spark.dualbird.sql.sort.enabled"] is True
        assert config["spark.dualbird.sql.sortMergeJoin.enabled"] is True
        assert "spark.dualbird.sql.window.enabled" not in config

    def test_all_categories(self):
        result = json.loads(get_recommended_config())
        config = result["config"]
        assert config["spark.dualbird.sql.sort.enabled"] is True
        assert config["spark.dualbird.sql.groupBy.enabled"] is True
        assert config["spark.dualbird.sql.window.enabled"] is True
        assert config["spark.dualbird.sql.parquet.read.enabled"] is True

    def test_emr_format(self):
        result = json.loads(get_recommended_config(["sort"], "emr"))
        assert "emr_configuration" in result
        emr = result["emr_configuration"][0]
        assert emr["Classification"] == "spark-defaults"
        assert "spark.plugins" in emr["Properties"]

    def test_databricks_format(self):
        result = json.loads(get_recommended_config(["sort"], "databricks"))
        assert "databricks_config" in result

    def test_spark_submit_format(self):
        result = json.loads(get_recommended_config(["sort"], "spark-submit"))
        assert "spark_submit_args" in result
        assert "--conf" in result["spark_submit_args"]

    def test_generic_format(self):
        result = json.loads(get_recommended_config(["sort"], "generic"))
        assert "spark_defaults_conf" in result


# ---------------------------------------------------------------------------
# get_support_matrix
# ---------------------------------------------------------------------------


class TestGetSupportMatrix:
    def test_returns_all_sections(self):
        result = json.loads(get_support_matrix())
        assert "supported_operators" in result
        assert "passthrough_operators" in result
        assert "supported_agg_functions" in result
        assert "supported_window_functions" in result
        assert "supported_join_types" in result
        assert "supported_primitive_types" in result
        assert "max_decimal_precision" in result
        assert "supported_expressions" in result
        assert "unsupported_expressions" in result

    def test_operators_have_categories(self):
        result = json.loads(get_support_matrix())
        ops = result["supported_operators"]
        assert "SortExec" in ops
        assert ops["SortExec"]["category"] == "sort"

    def test_max_decimal_precision(self):
        result = json.loads(get_support_matrix())
        assert result["max_decimal_precision"] == 18


# ---------------------------------------------------------------------------
# analyze_pyspark_code
# ---------------------------------------------------------------------------


class TestAnalyzePysparkCode:
    def test_inline_code(self):
        code = textwrap.dedent("""\
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet("s3://bucket/data")
            result = df.filter(df.age > 25).groupBy("city").count()
            result.write.parquet("s3://bucket/output")
        """)
        result = json.loads(analyze_pyspark_code(source_code=code))
        assert result["total_operators"] > 0
        assert result["accelerable_count"] > 0
        assert "recommended_config" in result
        assert (
            result["recommended_config"]["spark.plugins"]
            == "com.dualbird.spark.FpgaPlugin"
        )

    def test_code_with_udf(self):
        code = textwrap.dedent("""\
            from pyspark.sql import SparkSession
            from pyspark.sql.functions import udf
            spark = SparkSession.builder.getOrCreate()
            my_udf = udf(lambda x: x.upper())
            df = spark.read.parquet("data.parquet")
            df.withColumn("upper_name", my_udf(df.name)).write.parquet("out")
        """)
        result = json.loads(analyze_pyspark_code(source_code=code))
        assert result["has_udfs"] is True
        assert result["blocked_count"] > 0

    def test_missing_file(self):
        result = json.loads(analyze_pyspark_code(file_path="/nonexistent/file.py"))
        assert "error" in result

    def test_no_input(self):
        result = json.loads(analyze_pyspark_code())
        assert "error" in result

    def test_file_path(self, tmp_path: Path):
        code = "from pyspark.sql import SparkSession\ndf = spark.read.parquet('data')\ndf.orderBy('col').write.parquet('out')\n"
        f = tmp_path / "job.py"
        f.write_text(code)
        result = json.loads(analyze_pyspark_code(file_path=str(f)))
        assert result["total_operators"] > 0
