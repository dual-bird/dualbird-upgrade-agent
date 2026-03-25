"""CLI entry point for dualbird-upgrade-agent."""

import sys


def main() -> None:
    """Run the MCP server (stdio transport by default)."""
    from dualbird_upgrade_agent.server import mcp

    transport = "stdio"
    if "--sse" in sys.argv:
        transport = "sse"
    mcp.run(transport=transport)
