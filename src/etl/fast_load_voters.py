"""
ETL: bulk load the NC statewide voter file into raw.raw_voters with COPY.

Replaces the pandas to_sql loader for automated runs. The source file is
streamed through a single COPY statement, so no intermediate table and no
DataFrame are needed.

Three modes:
  swap (default)  - load into raw.raw_voters_new, then rename. The site keeps
                    the old snapshot until the swap. Peak storage is two
                    copies of the table.
  in-place        - TRUNCATE and COPY in one transaction. This also needs two
                    copies: Postgres keeps the old table file until the
                    transaction commits.
  truncate-first  - TRUNCATE, commit, then COPY. Peak storage is one copy, but
                    the site shows an empty table while the load runs.

Usage:
    python -m src.etl.fast_load_voters
    python -m src.etl.fast_load_voters --truncate-first
    python -m src.etl.fast_load_voters --only-columns config/load_columns.txt
    python -m src.etl.fast_load_voters --file data/raw/ncvoter_Statewide.txt
"""
import argparse
import csv
import io
import logging
import resource
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
PREVIOUS = "raw_voters_old"
ENCODING = "latin1"
LOG_EVERY = 500_000
MIN_EXPECTED_ROWS = 1_000_000

csv.field_size_limit(16 * 1024 * 1024)


# ---------------------------------------------------------------- connection

def get_connection():
    """
    Open a psycopg2 connection.

    TCP keepalives matter here. If the Render instance restarts or fills its
    disk during a long COPY, the connection can go dead without a packet from
    the server. Without keepalives the client waits on a socket that never
    answers, the runner timeout kills the process, and the log holds no error.
    With keepalives the driver raises OperationalError in about one minute.
    """
    url = get_db_url()
    if "localhost" not in url and "127.0.0.1" not in url and "sslmode" not in url:
        url += "?sslmode=require" if "?" not in url else "&sslmode=require"

    conn = psycopg2.connect(
        url,
        connect_timeout=30,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    conn.autocommit = False
    return conn


def get_table_columns(conn, table):
    """Return (column_name, data_type) pairs for a table, in ordinal order."""
    sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (SCHEMA, table))
        return [(row[0], row[1]) for row in cur.fetchall()]


# ----------------------------------------------------------------- preflight

def gb(value):
    return f"{value / (1024 ** 3):.2f} GB"


def drop_leftovers(conn):
    """Remove staging and previous tables left by a failed run."""
    with conn.cursor() as cur:
        for name in (STAGING, PREVIOUS):
            cur.execute("SELECT to_regclass(%s) IS NOT NULL",
                        (f"{SCHEMA}.{name}",))
            if not cur.fetchone()[0]:
                continue
            cur.execute("SELECT pg_total_relation_size(%s)",
                        (f"{SCHEMA}.{name}",))
            size = cur.fetchone()[0] or 0
            logger.warning(f"Dropping leftover table {SCHEMA}.{name} ({gb(size)})")
            cur.execute(f"DROP TABLE {SCHEMA}.{name}")
    conn.commit()


def report_space(conn, mode):
    """Log current space use and the peak the chosen mode needs."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pg_database_size(current_database()),
                   COALESCE(pg_total_relation_size(%s), 0)
        """, (f"{SCHEMA}.{TABLE}",))
        database_bytes, table_bytes = cur.fetchone()

    logger.info(f"Database size now: {gb(database_bytes)}")
    logger.info(f"{TABLE} size now: {gb(table_bytes)}")
    if mode == "truncate-first":
        logger.info(f"Peak space this mode needs: {gb(database_bytes)}")
    else:
        logger.info(
            f"Peak space this mode needs: {gb(database_bytes + table_bytes)}. "
            f"Use --truncate-first if the instance has less headroom."
        )


def log_memory(label):
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    logger.info(f"{label}: peak process memory {peak_kb / 1024:.0f} MB")


# ------------------------------------------------------------------ mapping

def normalize(name):
    return name.strip().strip('"').strip().lower()


def read_allowlist(path):
    """Read a column allowlist. One column name per line, # starts a comment."""
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    names = {normalize(line) for line in lines
             if line.strip() and not line.strip().startswith("#")}
    logger.info(f"Column allowlist: {len(names)} columns from {path}")
    return names


DATE_TYPES = {"date", "timestamp without time zone", "timestamp with time zone"}


def build_plan(target_columns, header, allowlist=None):
    """
    Map target columns to source columns.

    Returns (plan, birth_year_index).
    plan is a list of (target_column, source_index, kind). Target columns that
    are absent from the source file stay out of the COPY column list, so their
    database defaults apply. This absorbs NC SBE layout changes without a code
    change.

    Date handling follows the type of the target column, not the column name.
    The source file holds MM/DD/YYYY strings. If the column is DATE, the value
    is converted to ISO. If the column is text, the original string is kept,
    because queries in src/database/ match registr_dt with a MM/DD/YYYY regular
    expression and would return nothing against ISO text.
    """
    source_index = {}
    for position, name in enumerate(header):
        key = normalize(name)
        if key and key not in source_index:
            source_index[key] = position

    plan = []
    for column, data_type in target_columns:
        key = column.lower()
        if allowlist is not None and key not in allowlist:
            continue
        if key == "age_group":
            plan.append((column, None, "age_group"))
        elif key in source_index:
            kind = "date" if data_type in DATE_TYPES else "text"
            plan.append((column, source_index[key], kind))
            if key.endswith("_dt"):
                logger.info(
                    f"Column {column} has type {data_type}: "
                    f"{'converting to ISO' if kind == 'date' else 'keeping source text'}"
                )

    selected = {name.lower() for name, _, _ in plan}

    missing = [name for name, _ in target_columns
               if name.lower() != "age_group"
               and name.lower() not in source_index]
    if missing:
        logger.warning(f"Columns not present in source file: {', '.join(missing)}")

    if allowlist is not None:
        skipped = [name for name, _ in target_columns
                   if name.lower() not in selected]
        logger.info(f"Columns skipped by allowlist: {len(skipped)}")

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
                    log_memory(f"Streamed {self.rows:,} rows")
            except StopIteration:
                break
        size = min(want, len(self._buffer))
        target[:size] = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return size


def row_generator(file_path, plan, birth_year_index):
    """Yield one COPY text line per source row, as bytes."""
    current_year = datetime.now().year
    indexes = [i for _, i, _ in plan if i is not None]
    if birth_year_index is not None:
        indexes.append(birth_year_index)
    width = max(indexes, default=0) + 1
    bad_rows = 0

    with open(file_path, "r", encoding=ENCODING, newline="") as handle:
        reader = csv.reader(handle, delimiter="\t", quotechar='"')
        next(reader, None)
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


def fast_load_voters(file_path=None, mode="swap", only_columns=None):
    """Load the voter file into raw.raw_voters. Returns True on success."""
    if mode not in ("swap", "in-place", "truncate-first"):
        logger.error(f"Unknown mode: {mode}")
        return False

    path = resolve_file(file_path)
    if not path:
        logger.error("No voter file found. Run the scraper first.")
        return False

    logger.info(f"Source file: {path} ({path.stat().st_size / (1024 ** 3):.2f} GB)")
    logger.info(f"Load mode: {mode}")

    conn = get_connection()
    try:
        target_columns = get_table_columns(conn, TABLE)
        if not target_columns:
            logger.error(f"Table {SCHEMA}.{TABLE} does not exist")
            return False

        drop_leftovers(conn)
        report_space(conn, mode)

        with open(path, "r", encoding=ENCODING, newline="") as handle:
            header = next(csv.reader(handle, delimiter="\t", quotechar='"'))

        allowlist = read_allowlist(only_columns) if only_columns else None
        plan, birth_year_index = build_plan(target_columns, header, allowlist)
        if not plan:
            logger.error("No columns matched between the file and the table")
            return False
        logger.info(f"Loading {len(plan)} columns")

        columns = ", ".join(f'"{name}"' for name, _, _ in plan)
        load_table = STAGING if mode == "swap" else TABLE

        # truncate-first commits the empty table before the COPY starts, so
        # Postgres releases the old table file and peak storage stays at one
        # copy.
        if mode == "truncate-first":
            with conn.cursor() as cur:
                logger.warning(
                    f"Truncating {SCHEMA}.{TABLE} and committing. The site "
                    f"shows no data until the load finishes."
                )
                cur.execute(f"TRUNCATE TABLE {SCHEMA}.{TABLE}")
            conn.commit()

        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute("SET idle_in_transaction_session_timeout = 0")
            cur.execute("SET synchronous_commit = off")

            if mode == "in-place":
                logger.info(f"Truncating {SCHEMA}.{TABLE}")
                cur.execute(f"TRUNCATE TABLE {SCHEMA}.{TABLE}")
            elif mode == "swap":
                logger.info(f"Creating {SCHEMA}.{STAGING}")
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

            if stream.rows < MIN_EXPECTED_ROWS:
                raise RuntimeError(
                    f"Only {stream.rows:,} rows loaded. Aborting to protect the "
                    f"live table."
                )

            if mode == "swap":
                logger.info("Swapping tables")
                cur.execute(f"ALTER TABLE {SCHEMA}.{TABLE} RENAME TO {PREVIOUS}")
                cur.execute(f"ALTER TABLE {SCHEMA}.{STAGING} RENAME TO {TABLE}")

        conn.commit()
        logger.info("Load committed")

        conn.autocommit = True
        with conn.cursor() as cur:
            if mode == "swap":
                logger.info("Dropping previous snapshot")
                cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{PREVIOUS}")
            logger.info("Running ANALYZE")
            cur.execute(f"ANALYZE {SCHEMA}.{TABLE}")
            cur.execute("SELECT pg_database_size(current_database())")
            logger.info(f"Database size after load: {gb(cur.fetchone()[0])}")

        try:
            from src.email.notifications import send_update_email
            send_update_email()
        except Exception as error:
            logger.warning(f"Email notification skipped: {error}")

        return True

    except psycopg2.OperationalError as error:
        logger.error(
            f"Connection to the database was lost: {error}. A Render instance "
            f"that fills its disk, restarts, or is killed for memory use fails "
            f"this way. Check the Render metrics and event log for the time of "
            f"this run."
        )
        return False
    except Exception as error:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.error(f"Load failed: {error}", exc_info=True)
        return False
    finally:
        log_memory("Final")
        try:
            conn.close()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="Bulk load NC voter file")
    parser.add_argument("--file", help="Path to the voter .txt file")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--in-place", action="store_true",
                       help="TRUNCATE and COPY in one transaction")
    group.add_argument("--truncate-first", action="store_true",
                       help="TRUNCATE, commit, then COPY. Lowest storage need")
    parser.add_argument("--only-columns",
                        help="Path to a file that lists the columns to load")
    args = parser.parse_args()

    mode = "swap"
    if args.in_place:
        mode = "in-place"
    elif args.truncate_first:
        mode = "truncate-first"

    if not fast_load_voters(args.file, mode, args.only_columns):
        sys.exit(1)


if __name__ == "__main__":
    main()
