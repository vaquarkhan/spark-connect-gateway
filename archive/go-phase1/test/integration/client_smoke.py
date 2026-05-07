"""End-to-end smoke test against a running gateway + Spark Connect server.

Usage:
    python3 client_smoke.py [gateway_url]

Default gateway_url: sc://localhost:15003
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession


def main(url: str) -> int:
    spark = SparkSession.builder.remote(url).getOrCreate()

    # Test 1: trivial range count
    n = spark.range(10).count()
    assert n == 10, f"range(10).count() = {n}, want 10"
    print(f"[OK] range(10).count() = {n}")

    # Test 2: small DataFrame with explicit schema
    df = spark.createDataFrame(
        [(1, "a", 1.5), (2, "b", 2.5), (3, "c", 3.5)],
        schema="id INT, label STRING, value DOUBLE",
    )
    rows = df.collect()
    assert len(rows) == 3, f"collected {len(rows)} rows, want 3"
    assert rows[0].id == 1 and rows[0].label == "a"
    print(f"[OK] createDataFrame returned {len(rows)} rows with correct schema")

    # Test 3: re-issue requests on the same session — stickiness should keep
    # them on the same backend (visible only via gateway logs).
    for i in range(5):
        spark.range(i + 1).count()
    print("[OK] 5 follow-up queries succeeded on the same session")

    spark.stop()
    return 0


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "sc://localhost:15003"
    sys.exit(main(url))
