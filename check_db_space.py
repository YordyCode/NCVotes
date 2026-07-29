#!/usr/bin/env python3
"""
Report Render Postgres space use for the voter tables.

Run this before a load to see whether a second copy of raw_voters fits.

Usage:
    python check_db_space.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import psycopg2

from src.etl.fast_load_voters import get_connection

QUERY = """
SELECT
    pg_database_size(current_database())                       AS database_bytes,
    COALESCE(pg_total_relation_size('raw.raw_voters'), 0)      AS table_bytes,
    COALESCE(pg_indexes_size('raw.raw_voters'), 0)             AS index_bytes,
    (SELECT reltuples::bigint FROM pg_class
      WHERE oid = 'raw.raw_voters'::regclass)                  AS approx_rows
"""

LEFTOVERS = """
SELECT table_name, pg_total_relation_size('raw.' || table_name) AS bytes
FROM information_schema.tables
WHERE table_schema = 'raw'
  AND table_name IN ('raw_voters_new', 'raw_voters_old')
"""


def gb(value):
    return f"{value / (1024 ** 3):.2f} GB"


def main():
    conn = get_connection()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(QUERY)
            database_bytes, table_bytes, index_bytes, approx_rows = cur.fetchone()

            print(f"Database total       : {gb(database_bytes)}")
            print(f"raw_voters total     : {gb(table_bytes)}")
            print(f"  of which indexes   : {gb(index_bytes)}")
            print(f"Approximate rows     : {approx_rows:,}")
            print()
            print(f"Swap mode peak need  : {gb(database_bytes + table_bytes)}")
            print(f"Truncate-first need  : {gb(database_bytes)}")

            cur.execute(LEFTOVERS)
            leftovers = cur.fetchall()
            if leftovers:
                print()
                print("Leftover tables from a failed run:")
                for name, size in leftovers:
                    print(f"  raw.{name}: {gb(size)}")
                print("Drop them to reclaim space.")
    finally:
        conn.close()

    print()
    print("Compare 'swap mode peak need' with the storage allocated to the")
    print("instance in the Render dashboard. Render bills storage at a fixed")
    print("rate per GB per month and lets you add storage in 5 GB steps with")
    print("no downtime.")


if __name__ == "__main__":
    main()
