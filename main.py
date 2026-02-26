#!/usr/bin/env python3
"""
Gmail scraper — entry point.

Usage:
    python main.py                        # Incremental (since last seen email)
    python main.py --reprocess            # Re-fetch from GMAIL_DAYS_BACK
    python main.py --query "in:sent"      # Override search query
    python main.py --max-results 1000     # Override max messages to fetch
"""

import argparse
import sys

from config import setup_logging, GMAIL_QUERY, GMAIL_DAYS_BACK, GMAIL_MAX_RESULTS
from db import close_db
from extractor import run_scraper

setup_logging()


def main():
    parser = argparse.ArgumentParser(
        description="Incremental Gmail scraper → Supabase"
    )
    parser.add_argument(
        "--reprocess",
        action="store_true",
        help=f"Ignore last seen date; re-fetch last {GMAIL_DAYS_BACK} days (GMAIL_DAYS_BACK)",
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        metavar="QUERY",
        help=f"Gmail search query (default: {GMAIL_QUERY!r} from .env)",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=None,
        metavar="N",
        help=f"Max messages to fetch (default: {GMAIL_MAX_RESULTS} from .env)",
    )
    args = parser.parse_args()

    try:
        run_scraper(
            reprocess=args.reprocess,
            query=args.query,
            max_results=args.max_results,
        )
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
