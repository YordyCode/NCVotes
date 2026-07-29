#!/usr/bin/env python3
"""
Headless weekly update: scrape, load, regenerate outputs. No web server.

This is the entry point for the GitHub Actions schedule. Unlike
run_pipeline.py it never calls app.run(), and it uses the COPY-based loader
instead of pandas to_sql.

Usage:
    python weekly_update.py
    python weekly_update.py --skip-scrape
    python weekly_update.py --in-place
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.database.connection import test_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('weekly_update.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def step(number, total, name):
    logger.info("=" * 60)
    logger.info(f"[{number}/{total}] {name}")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Weekly data refresh")
    parser.add_argument("--skip-scrape", action="store_true",
                        help="Use the file already in data/raw")
    parser.add_argument("--skip-load", action="store_true",
                        help="Regenerate outputs only")
    parser.add_argument("--in-place", action="store_true",
                        help="Load with TRUNCATE instead of table swap")
    args = parser.parse_args()

    total = 5

    step(1, total, "Test database connection")
    if not test_connection():
        logger.error("Database connection failed")
        return 1

    if not args.skip_scrape:
        step(2, total, "Download voter registration file")
        from src.scraper import registration
        if not registration.scrape_registration():
            logger.error("Download failed")
            return 1
    else:
        logger.info("[2/5] Download skipped")

    if not args.skip_load:
        step(3, total, "Bulk load voter file")
        from src.etl.fast_load_voters import fast_load_voters
        if not fast_load_voters(in_place=args.in_place):
            logger.error("Load failed")
            return 1
    else:
        logger.info("[3/5] Load skipped")

    step(4, total, "Generate charts and key statistics")
    failures = []
    from src.visualization import demographics, trends
    try:
        demographics.generate_all_demographics_charts()
    except Exception as error:
        failures.append(f"demographics: {error}")
    try:
        trends.generate_all_trends()
    except Exception as error:
        failures.append(f"trends: {error}")

    step(5, total, "Generate interactive maps")
    try:
        from src.visualization.interactive_map import create_all_maps
        create_all_maps()
    except Exception as error:
        failures.append(f"maps: {error}")

    if failures:
        for item in failures:
            logger.error(f"Output generation problem: {item}")
        logger.error("Data is loaded, but some outputs did not build")
        return 1

    logger.info("Weekly update complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
