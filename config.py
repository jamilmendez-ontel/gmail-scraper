#!/usr/bin/env python3
"""
Configuration: environment variables, logging setup, schema constants.
"""

import os
import sys
import logging
from dotenv import load_dotenv

load_dotenv()

# ── Schema constants ──────────────────────────────────────────────────────────
SCHEMA_RAW      = "data_raw"
SCHEMA_STAGING  = "data_staging"
SCHEMA_PIPELINE = "pipeline"

# ── Gmail settings ────────────────────────────────────────────────────────────
GMAIL_QUERY       = os.getenv("GMAIL_QUERY", "in:inbox")
GMAIL_DAYS_BACK   = int(os.getenv("GMAIL_DAYS_BACK", "30"))
GMAIL_MAX_RESULTS = int(os.getenv("GMAIL_MAX_RESULTS", "500"))

# ── Notification settings ────────────────────────────────────────────────────
REPORT_EMAIL_TO = os.getenv("REPORT_EMAIL_TO", "")

# Path to the pipeline's gmail_credentials dir (nanoninth account used for sending)
NOTIFY_CREDENTIALS_DIR = os.getenv(
    "NOTIFY_CREDENTIALS_DIR",
    os.path.join(os.path.dirname(__file__), "..", "local-pipeline", "swift_api_pipeline", "gmail_credentials"),
)


def setup_logging(level: int = logging.INFO) -> None:
    """Configure logging with consistent format."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S"
    ))
    root = logging.getLogger("scraper")
    root.handlers = []
    root.addHandler(handler)
    root.setLevel(level)


def get_logger(name: str) -> logging.Logger:
    """Get a scraper logger: scraper.<name>"""
    return logging.getLogger(f"scraper.{name}")
