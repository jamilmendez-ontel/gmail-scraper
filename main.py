#!/usr/bin/env python3
"""
Gmail scraper — entry point.

Usage:
    python main.py                        # Incremental scrape + parse
    python main.py --reprocess            # Re-fetch from GMAIL_DAYS_BACK + reparse
    python main.py --parse-only           # Re-parse already-scraped emails (no Gmail)
    python main.py --query "in:sent"      # Override search query
    python main.py --max-results 1000     # Override max messages to fetch
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from io import StringIO

from config import setup_logging, GMAIL_QUERY, GMAIL_DAYS_BACK, GMAIL_MAX_RESULTS, REPORT_EMAIL_TO
from db import close_db
from extractor import run_scraper
from notifier import send_report
from parser import run_parser

setup_logging()

# Capture all log output in memory for the report email
_log_buffer = StringIO()
_log_capture = logging.StreamHandler(_log_buffer)
_log_capture.setFormatter(logging.Formatter(
    "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
))
logging.getLogger("scraper").addHandler(_log_capture)


def main():
    arg_parser = argparse.ArgumentParser(
        description="Incremental Gmail scraper + package parser → Supabase"
    )
    arg_parser.add_argument(
        "--reprocess",
        action="store_true",
        help=f"Ignore last seen date; re-fetch last {GMAIL_DAYS_BACK} days (GMAIL_DAYS_BACK)",
    )
    arg_parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Skip Gmail scrape; only (re-)parse emails already in stg_emails",
    )
    arg_parser.add_argument(
        "--reparse",
        action="store_true",
        help="Re-parse all emails even if already in stg_package_emails",
    )
    arg_parser.add_argument(
        "--query",
        type=str,
        default=None,
        metavar="QUERY",
        help=f"Gmail search query (default: {GMAIL_QUERY!r} from .env)",
    )
    arg_parser.add_argument(
        "--max-results",
        type=int,
        default=None,
        metavar="N",
        help=f"Max messages to fetch (default: {GMAIL_MAX_RESULTS} from .env)",
    )
    args = arg_parser.parse_args()

    started = datetime.now(timezone.utc)

    try:
        # Step 1: Scrape Gmail (unless --parse-only)
        scraped_ids = []
        if not args.parse_only:
            scraped_ids = run_scraper(
                reprocess=args.reprocess,
                query=args.query,
                max_results=args.max_results,
            )

        # Step 2: Parse COP email bodies
        parsed_ids = run_parser(reparse=args.reparse or args.reprocess)

        ended = datetime.now(timezone.utc)

        # Step 3: Send notification email
        if REPORT_EMAIL_TO:
            log_text = _log_buffer.getvalue()
            # Use union of scraped + parsed IDs for the Excel report
            all_ids = list(dict.fromkeys(scraped_ids + parsed_ids))
            send_report(log_text, all_ids, started=started, ended=ended)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nFATAL: {type(e).__name__}: {e}")
        sys.exit(1)
    finally:
        close_db()


if __name__ == "__main__":
    main()
