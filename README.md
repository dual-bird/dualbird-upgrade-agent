# DualBird Upgrade Agent

AI agent that helps enable DualBird FPGA acceleration on Apache Spark pipelines. Enabling DualBird is **configuration-only** — no application code changes needed.

Provides an [MCP](https://modelcontextprotocol.io/) server wrapping the [DualBird Estimator](https://github.com/dual-bird/dualbird-estimator) analysis engine, plus ready-to-use agent definitions for **Kiro**, **Claude Code**, and **Cursor**.

Inspired by the [Apache Spark Upgrade Agent for Amazon EMR](https://aws.amazon.com/blogs/big-data/introducing-apache-spark-upgrade-agent-for-amazon-emr/).

## MCP Tools

| Tool | Description |
|------|-------------|
| `analyze_pyspark_code` | Static analysis of PySpark code for FPGA compatibility |
| `analyze_spark_event_log` | Full analysis of Spark event logs with per-stage verdicts |
| `check_operator_support` | Check if a Spark physical operator is FPGA-supported |
| `check_expression_support` | Check if a Catalyst expression is FPGA-supported |
| `check_data_type_support` | Check if a Spark data type is FPGA-supported |
| `get_recommended_config` | Generate `spark.dualbird.*` config for EMR, Databricks, or spark-submit |
| `get_support_matrix` | Return the full FPGA support matrix |

## Quick Start

### Install

```bash
# Clone and install locally
git clone https://github.com/dual-bird/dualbird-upgrade-agent.git
cd dualbird-upgrade-agent
uv sync

# Or when published to PyPI:
pip install dualbird-upgrade-agent
```

### Run the MCP Server

```bash
# stdio (default — for IDE integration)
dualbird-upgrade-agent

# Or via Python module
python -m dualbird_upgrade_agent
```

### Configure Your IDE

Copy the appropriate `mcp.json` to your project:

**Kiro:**
```bash
cp agent/kiro/mcp.json .kiro/mcp.json
cp agent/kiro/POWER.md .kiro/powers/dualbird-upgrade/POWER.md
```

**Claude Code:**
```bash
cp agent/claude-code/mcp.json .mcp.json
cp agent/claude-code/CLAUDE.md CLAUDE.md
```

**Cursor:**
```bash
cp agent/cursor/mcp.json .cursor/mcp.json
mkdir -p .cursor/rules
cp agent/cursor/rules/dualbird-upgrade.mdc .cursor/rules/
```

## Workflow

The agent guides users through three phases:

### 1. Assess
Analyze existing Spark workloads to determine FPGA acceleration coverage:
- Upload Spark event logs for the most accurate analysis
- Or analyze PySpark source code for a quick compatibility check

### 2. Configure
Generate the right `spark.dualbird.*` configuration for your platform:
- **EMR**: JSON configuration block
- **Databricks**: Cluster Spark Config
- **spark-submit**: `--conf` flags
- **Generic**: `spark-defaults.conf` entries

### 3. Validate
After applying configuration, re-run the pipeline and verify acceleration is active by comparing before/after event log analysis.

## Development

```bash
# Install with dev dependencies
uv sync

# Run tests
uv run pytest tests/ -v

# Lint
uv run ruff check src/ tests/
```

## How It Works

This agent wraps the [DualBird Estimator](https://github.com/dual-bird/dualbird-estimator) analysis engine as MCP tools. The estimator's support matrix is derived directly from the [DualBird Spark Plugin](https://github.com/dual-bird/spark-plugin) source code, encoding exactly which Spark operators, expressions, data types, and functions the FPGA can accelerate.

Key insight: unsupported operators don't prevent enablement. The DualBird plugin accelerates what it can and **transparently falls back to Spark CPU** for the rest. The agent helps users understand their acceleration coverage and generate the right configuration.
