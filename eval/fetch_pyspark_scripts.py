"""Fetch real PySpark scripts from public GitHub repos for evaluation.

Searches GitHub for PySpark scripts and downloads them to eval/fixtures/.
Run periodically to grow the evaluation corpus.

Usage:
    python eval/fetch_pyspark_scripts.py [--count 50]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"
MANIFEST_PATH = FIXTURES_DIR / "manifest.json"

# Search queries that find diverse PySpark patterns
SEARCH_QUERIES = [
    "SparkSession groupBy agg language:python",
    "pyspark join orderBy language:python",
    "spark.read.parquet filter language:python",
    "spark.sql window language:python",
    "pyspark udf withColumn language:python",
    "SparkSession write parquet language:python",
    "pyspark repartition groupBy language:python",
    "spark DataFrame collect_list language:python",
    "pyspark pandas_udf language:python",
    "spark.read.csv groupBy agg language:python",
]


def search_github(query: str, per_page: int = 10) -> list[dict]:
    """Search GitHub code via gh CLI."""
    cmd = [
        "gh",
        "api",
        "search/code",
        "-X",
        "GET",
        "-f",
        f"q={query}",
        "-f",
        f"per_page={per_page}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  Warning: search failed for '{query}': {result.stderr.strip()}")
        return []
    data = json.loads(result.stdout)
    return data.get("items", [])


def download_file(repo_full_name: str, path: str) -> str | None:
    """Download raw file content from GitHub."""
    cmd = [
        "gh",
        "api",
        f"repos/{repo_full_name}/contents/{path}",
        "-H",
        "Accept: application/vnd.github.raw+json",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    return result.stdout


def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {"files": {}}


def save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--count", type=int, default=50, help="Target number of scripts"
    )
    args = parser.parse_args()

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest()
    existing = set(manifest["files"].keys())

    print(f"Currently have {len(existing)} scripts, target: {args.count}")

    fetched = 0
    for query in SEARCH_QUERIES:
        if len(existing) + fetched >= args.count:
            break

        print(f"\nSearching: {query}")
        items = search_github(query, per_page=10)
        time.sleep(2)  # Rate limiting

        for item in items:
            if len(existing) + fetched >= args.count:
                break

            repo = item["repository"]["full_name"]
            path = item["path"]
            key = f"{repo}/{path}"

            if key in existing:
                continue
            if not path.endswith(".py"):
                continue

            print(f"  Downloading: {key}")
            content = download_file(repo, path)
            time.sleep(1)  # Rate limiting

            if not content:
                continue

            # Skip tiny files or files without pyspark indicators
            if len(content) < 100:
                continue
            lower = content.lower()
            if "spark" not in lower and "pyspark" not in lower:
                continue

            # Save with sanitized filename
            safe_name = key.replace("/", "__").replace(" ", "_")
            dest = FIXTURES_DIR / safe_name
            dest.write_text(content)

            manifest["files"][key] = {
                "local_path": safe_name,
                "repo": repo,
                "path": path,
                "size": len(content),
            }
            fetched += 1

    save_manifest(manifest)
    total = len(manifest["files"])
    print(f"\nDone. Total scripts: {total} (+{fetched} new)")


if __name__ == "__main__":
    main()
