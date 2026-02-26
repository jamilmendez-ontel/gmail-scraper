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
import sys

from config import setup_logging, GMAIL_QUERY, GMAIL_DAYS_BACK, GMAIL_MAX_RESULTS
from db import close_db
from extractor import run_scraper
from parser import run_parser

setup_logging()


def main():
    arg_parser = argparse.ArgumentParser(
        description="Incremental Gmail scraper + COP parser → Supabase"
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
        help="Re-parse all emails even if already in stg_cop_emails",
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

    try:
        # Step 1: Scrape Gmail (unless --parse-only)
        if not args.parse_only:
            run_scraper(
                reprocess=args.reprocess,
                query=args.query,
                max_results=args.max_results,
            )

        # Step 2: Parse COP email bodies
        run_parser(reparse=args.reparse or args.reprocess)

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
