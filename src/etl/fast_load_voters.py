"""
ETL: bulk load the NC statewide voter file into raw.raw_voters with COPY.

Replaces the pandas to_sql loader for automated runs. The source file is
streamed through a single COPY statement, so no intermediate table and no
DataFrame are needed. Runtime is minutes instead of hours over a remote
connection.

Two modes:
  swap (default) - load into raw.raw_voters_new, then rename. The site keeps
                   serving the old snapshot during the load. Needs storage for
                   two copies of the table at the same time.
  in-place       - TRUNCATE and COPY in one transaction. No extra storage, but
                   the table is locked for the whole load.

Usage:
    python -m src.etl.fast_load_voters
    python -m src.etl.fast_load_voters --in-place
    python -m src.etl.fast_load_voters --file data/raw/ncvoter_Statewide.txt
"""
import argparse
import csv
import io
import logging
import sys
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import psycopg2

from config.settings import RAW_DATA_DIR, get_db_url
from src.scraper.manifest import get_latest_file

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SCHEMA = "raw"
TABLE = "raw_voters"
STAGING = "raw_voters_new"
ENCODING = "latin1"
LOG_EVERY = 500_000

csv.field_size_limit(16 * 1024 * 1024)


# ---------------------------------------------------------------- connection

def get_connection():
    """Open a psycopg2 connection. Force SSL for remote hosts."""
    url = get_db_url()
    if "localhost" not in url and "127.0.0.1" not in url and "sslmode" not in url:
        url += "?sslmode=require" if "?" not in url else "&sslmode=require"
    conn = psycopg2.connect(url, connect_timeout=30)
    conn.autocommit = False
    return conn


def get_table_columns(conn, table):
    """Return the column names of a table in ordinal order."""
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (SCHEMA, table))
        return [row[0] for row in cur.fetchall()]


# ------------------------------------------------------------------ mapping

def normalize(name):
    return name.strip().strip('"').strip().lower()


def build_plan(target_columns, header):
    """
    Map target columns to source columns.

    Returns (plan, birth_year_index).
    plan is a list of (target_column, source_index, kind). Target columns that
    do not exist in the source file are left out of the COPY column list, so
    their database defaults apply. This absorbs NC SBE layout changes without
    a code change.
    """
    source_index = {}
    for position, name in enumerate(header):
        key = normalize(name)
        if key and key not in source_index:
            source_index[key] = position

    plan = []
    for column in target_columns:
        key = column.lower()
        if key == "age_group":
            plan.append((column, None, "age_group"))
        elif key in source_index:
            kind = "date" if key.endswith("_dt") else "text"
            plan.append((column, source_index[key], kind))

    missing = [c for c in target_columns
               if c.lower() != "age_group" and c.lower() not in source_index]
    if missing:
        logger.warning(f"Columns not present in source file: {', '.join(missing)}")

    extra = [k for k in source_index if k not in {c.lower() for c in target_columns}]
    if extra:
        logger.warning(f"Source columns not present in table: {', '.join(extra)}")

    return plan, source_index.get("birth_year")


# --------------------------------------------------------------- transforms

def escape(value):
    """Escape a value for COPY text format."""
    if value is None:
        return "\\N"
    value = value.strip()
    if value == "":
        return "\\N"
    if "\\" in value:
        value = value.replace("\\", "\\\\")
    if "\t" in value or "\n" in value or "\r" in value:
        value = value.replace("\t", " ").replace("\n", " ").replace("\r", " ")
    return value


def to_date(value):
    """Convert MM/DD/YYYY to ISO. Masked or invalid values become NULL."""
    if not value:
        return "\\N"
    value = value.strip()
    if len(value) != 10 or value.count("/") != 2:
        return "\\N"
    month, day, year = value.split("/")
    if not (month.isdigit() and day.isdigit() and year.isdigit()):
        return "\\N"
    try:
        return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
    except ValueError:
        return "\\N"


def age_group(birth_year, current_year):
    """Return the age band for a birth year."""
    if not birth_year or not birth_year.strip().isdigit():
        return "Unknown"
    age = current_year - int(birth_year.strip())
    if 18 <= age <= 25:
        return "18-25"
    if 26 <= age <= 35:
        return "26-35"
    if 36 <= age <= 50:
        return "36-50"
    if 51 <= age <= 65:
        return "51-65"
    if age > 65:
        return "65+"
    return "Unknown"


# ------------------------------------------------------------ stream adapter

class RowStream(io.RawIOBase):
    """File-like object that pulls transformed rows from a generator."""

    def __init__(self, lines):
        self._lines = lines
        self._buffer = b""
        self.rows = 0

    def readable(self):
        return True

    def readinto(self, target):
        want = len(target)
        while len(self._buffer) < want:
            try:
                self._buffer += next(self._lines)
                self.rows += 1
                if self.rows % LOG_EVERY == 0:
                    logger.info(f"Streamed {self.rows:,} rows")
            except StopIteration:
                break
        size = min(want, len(self._buffer))
        target[:size] = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return size


def row_generator(file_path, plan, birth_year_index):
    """Yield one COPY text line per source row, as bytes."""
    current_year = datetime.now().year
    width = max((i for _, i, _ in plan if i is not None), default=0) + 1
    bad_rows = 0

    with open(file_path, "r", encoding=ENCODING, newline="") as handle:
        reader = csv.reader(handle, delimiter="\t", quotechar='"')
        next(reader, None)  # header already read by the caller
        for row in reader:
            if len(row) < width:
                bad_rows += 1
                continue
            fields = []
            for _, index, kind in plan:
                if kind == "age_group":
                    source = row[birth_year_index] if birth_year_index is not None else ""
                    fields.append(age_group(source, current_year))
                elif kind == "date":
                    fields.append(to_date(row[index]))
                else:
                    fields.append(escape(row[index]))
            yield ("\t".join(fields) + "\n").encode("utf-8")

    if bad_rows:
        logger.warning(f"Skipped {bad_rows:,} short rows")


# ----------------------------------------------------------------- main load

def resolve_file(explicit=None):
    """Find the voter file to load."""
    if explicit:
        path = Path(explicit)
        return path if path.exists() else None

    entry = get_latest_file("registration_data")
    if entry:
        path = RAW_DATA_DIR / entry["filename"]
        if path.exists():
            return path

    matches = sorted(RAW_DATA_DIR.glob("ncvoter*.txt"),
                     key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def fast_load_voters(file_path=None, in_place=False):
    """Load the voter file into raw.raw_voters. Returns True on success."""
    path = resolve_file(file_path)
    if not path:
        logger.error("No voter file found. Run the scraper first.")
        return False

    size_gb = path.stat().st_size / (1024 ** 3)
    logger.info(f"Source file: {path} ({size_gb:.2f} GB)")

    conn = get_connection()
    try:
        target_columns = get_table_columns(conn, TABLE)
        if not target_columns:
            logger.error(f"Table {SCHEMA}.{TABLE} does not exist")
            return False

        with open(path, "r", encoding=ENCODING, newline="") as handle:
            header = next(csv.reader(handle, delimiter="\t", quotechar='"'))

        plan, birth_year_index = build_plan(target_columns, header)
        if not plan:
            logger.error("No columns matched between the file and the table")
            return False
        logger.info(f"Loading {len(plan)} columns")

        columns = ", ".join(f'"{name}"' for name, _, _ in plan)
        load_table = TABLE if in_place else STAGING

        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute("SET synchronous_commit = off")

            if in_place:
                logger.info(f"Truncating {SCHEMA}.{TABLE}")
                cur.execute(f"TRUNCATE TABLE {SCHEMA}.{TABLE}")
            else:
                logger.info(f"Creating {SCHEMA}.{STAGING}")
                cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{STAGING}")
                cur.execute(
                    f"CREATE TABLE {SCHEMA}.{STAGING} "
                    f"(LIKE {SCHEMA}.{TABLE} INCLUDING ALL)"
                )

            stream = RowStream(row_generator(path, plan, birth_year_index))
            buffered = io.BufferedReader(stream, buffer_size=1024 * 1024)

            logger.info("Starting COPY")
            started = datetime.now()
            cur.copy_expert(
                f"COPY {SCHEMA}.{load_table} ({columns}) "
                f"FROM STDIN WITH (FORMAT text)",
                buffered,
            )
            elapsed = (datetime.now() - started).total_seconds()
            logger.info(f"COPY finished: {stream.rows:,} rows in {elapsed / 60:.1f} min")

            if stream.rows < 1_000_000:
                raise RuntimeError(
                    f"Only {stream.rows:,} rows loaded. Aborting to protect the "
                    f"live table."
                )

            if not in_place:
                logger.info("Swapping tables")
                cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE}_old")
                cur.execute(f"ALTER TABLE {SCHEMA}.{TABLE} RENAME TO {TABLE}_old")
                cur.execute(f"ALTER TABLE {SCHEMA}.{STAGING} RENAME TO {TABLE}")

        conn.commit()
        logger.info("Load committed")

        conn.autocommit = True
        with conn.cursor() as cur:
            if not in_place:
                logger.info("Dropping previous snapshot")
                cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE}_old")
            logger.info("Running ANALYZE")
            cur.execute(f"ANALYZE {SCHEMA}.{TABLE}")

        try:
            from src.email.notifications import send_update_email
            send_update_email()
        except Exception as error:
            logger.warning(f"Email notification skipped: {error}")

        return True

    except Exception as error:
        conn.rollback()
        logger.error(f"Load failed: {error}", exc_info=True)
        return False
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Bulk load NC voter file")
    parser.add_argument("--file", help="Path to the voter .txt file")
    parser.add_argument("--in-place", action="store_true",
                        help="TRUNCATE and load in place instead of swapping")
    args = parser.parse_args()

    if not fast_load_voters(args.file, args.in_place):
        sys.exit(1)


if __name__ == "__main__":
    main()
