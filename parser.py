#!/usr/bin/env python3
"""
COP email HTML parser.

Reads html_body from data_staging.stg_emails, finds the first
"CLOSE OUT PACKAGE" table in each email, extracts all label:value
pairs into JSONB, and writes to data_staging.stg_cop_emails.

Re-runnable: ON CONFLICT (message_id) DO UPDATE so improvements
to the parser can be applied by re-running without re-scraping.
"""

import re
from typing import Optional, Dict, List, Tuple

from bs4 import BeautifulSoup

from config import SCHEMA_STAGING, get_logger
from db import get_db, retry_db

logger = get_logger("parser")

# Section header text that should NOT be treated as field labels
_SECTION_HEADERS = {
    "SITE TIMELINES", "DOWNLOAD LINKS", "COP LINKS",
    "ADDITIONAL NOTES", "PENDING ITEMS", "CLOSE OUT PACKAGE",
}

# ── HTML parsing ──────────────────────────────────────────────────────────────

def _clean_text(text: str) -> str:
    """Collapse whitespace and strip."""
    return re.sub(r'\s+', ' ', text).strip()


def _extract_package_type(header_text: str) -> str:
    """
    Derive a short package type label from the header row text.

    Examples:
      "POST MODIFICATION INSPECTION CLOSE OUT PACKAGE" → "PMI"
      "CLOSE OUT PACKAGE REVIEW"                       → "REVIEW"
      "CLOSE OUT PACKAGE REVISION"                     → "REVISION"
    """
    t = header_text.upper()
    if "POST MODIFICATION INSPECTION" in t:
        return "PMI"
    if "REVIEW" in t:
        return "REVIEW"
    if "REVISION" in t:
        return "REVISION"
    # Fallback: whatever follows "CLOSE OUT PACKAGE"
    m = re.search(r'CLOSE OUT PACKAGE\s+(.*)', t)
    if m:
        return m.group(1).strip() or "UNKNOWN"
    return "UNKNOWN"


def parse_cop_email(html_body: str) -> Dict:
    """
    Parse the first COP data table from an email HTML body.

    Returns dict with keys:
        package_type  str   REVIEW / REVISION / PMI / UNKNOWN
        fields        dict  All label:value pairs from the table
        dropbox_url   str | None
        swift_url     str | None
        parse_error   str | None  Set if parsing failed
    """
    if not html_body:
        return {"parse_error": "empty body"}

    soup = BeautifulSoup(html_body, "html.parser")

    # Remove zero-font hidden spans (used for hash IDs in some layouts)
    for span in soup.find_all("span", style=lambda s: s and "font-size:0" in s):
        span.decompose()

    # ── Step 1: Find the COP header cell ─────────────────────────────────────
    header_cell = None
    for cell in soup.find_all(["th", "td"]):
        text = _clean_text(cell.get_text(" ", strip=True)).upper()
        if "CLOSE OUT PACKAGE" in text:
            header_cell = cell
            break

    if not header_cell:
        return {"parse_error": "no CLOSE OUT PACKAGE header found"}

    package_type = _extract_package_type(
        _clean_text(header_cell.get_text(" ", strip=True))
    )

    # ── Step 2: Walk up to find the containing table ──────────────────────────
    # We want the innermost table that contains the header cell.
    cop_table = None
    node = header_cell.parent
    while node:
        if node.name == "table":
            cop_table = node
            break
        node = node.parent

    if not cop_table:
        return {"package_type": package_type, "parse_error": "could not find COP table"}

    # ── Step 3: Extract all label:value pairs ─────────────────────────────────
    fields: Dict[str, str] = {}
    dropbox_url: Optional[str] = None
    swift_url: Optional[str] = None

    for row in cop_table.find_all("tr"):
        # Extract URLs (recursive — links can be deep)
        for a in row.find_all("a", href=True):
            href = a["href"]
            if "dropbox.com" in href and not dropbox_url:
                dropbox_url = href
            elif "swiftprojects.io" in href and not swift_url:
                swift_url = href

        # Use only DIRECT child cells to avoid pulling in nested table data
        cells = row.find_all(["th", "td"], recursive=False)
        if not cells:
            continue

        i = 0
        while i < len(cells):
            cell = cells[i]
            text = _clean_text(cell.get_text(" ", strip=True))

            # A cell is a label if it's a <th> OR its text ends with ":"
            is_label = cell.name == "th" or text.endswith(":")

            if is_label and i + 1 < len(cells):
                label = text.rstrip(":").strip()

                # Skip section headers and empty labels
                if not label or label.upper() in _SECTION_HEADERS:
                    i += 1
                    continue

                value_text = _clean_text(
                    cells[i + 1].get_text(" ", strip=True)
                )

                # Don't overwrite already-captured fields (first occurrence wins)
                if label not in fields:
                    fields[label] = value_text

                i += 2
            else:
                i += 1

    if not fields:
        return {
            "package_type": package_type,
            "parse_error": "table found but no label:value pairs extracted",
        }

    return {
        "package_type": package_type,
        "fields": fields,
        "dropbox_url": dropbox_url,
        "swift_url": swift_url,
        "parse_error": None,
    }


# ── DB helpers ────────────────────────────────────────────────────────────────

def _upsert_cop_batch(db, rows: List[Dict]) -> None:
    """Upsert a batch into stg_cop_emails."""
    tuples = [
        (
            r["message_id"],
            r.get("package_type"),
            r.get("fields"),
            r.get("dropbox_url"),
            r.get("swift_url"),
            r.get("parse_error"),
        )
        for r in rows
    ]
    retry_db(
        lambda: db.executemany(
            f"""
            INSERT INTO {SCHEMA_STAGING}.stg_cop_emails
                (message_id, package_type, fields, dropbox_url, swift_url, parse_error)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (message_id) DO UPDATE SET
                package_type = EXCLUDED.package_type,
                fields       = EXCLUDED.fields,
                dropbox_url  = EXCLUDED.dropbox_url,
                swift_url    = EXCLUDED.swift_url,
                parse_error  = EXCLUDED.parse_error,
                parsed_at    = NOW()
            """,
            tuples,
        ),
        description="upsert stg_cop_emails batch",
    )


# ── Main pipeline ─────────────────────────────────────────────────────────────

BATCH_SIZE = 50


def run_parser(reparse: bool = False) -> int:
    """
    Parse COP email bodies and populate stg_cop_emails.

    Args:
        reparse: If True, re-parse all emails (including already parsed).
                 If False (default), only parse emails not yet in stg_cop_emails.

    Returns:
        Number of emails parsed.
    """
    logger.info("=" * 60)
    logger.info("COP Email Parser")
    logger.info("=" * 60)

    db = get_db()

    # Fetch emails to parse
    if reparse:
        rows = db.fetch(
            f"SELECT message_id, subject, html_body FROM {SCHEMA_STAGING}.stg_emails"
        )
        logger.info(f"Reparse mode: processing all {len(rows):,} emails")
    else:
        rows = db.fetch(
            f"""
            SELECT e.message_id, e.subject, e.html_body
            FROM {SCHEMA_STAGING}.stg_emails e
            LEFT JOIN {SCHEMA_STAGING}.stg_cop_emails c USING (message_id)
            WHERE c.message_id IS NULL
            """
        )
        logger.info(f"Emails pending parse: {len(rows):,}")

    if not rows:
        logger.info("Nothing to parse.")
        return 0

    # Parse
    parsed_rows = []
    errors = 0
    for row in rows:
        result = parse_cop_email(row["html_body"])
        parsed_rows.append({
            "message_id": row["message_id"],
            **result,
        })
        if result.get("parse_error"):
            errors += 1
            logger.debug(
                f"  [{row['message_id']}] parse_error: {result['parse_error']} "
                f"— subject: {row['subject']}"
            )

    # Batch upsert
    logger.info(f"Upserting {len(parsed_rows):,} rows to stg_cop_emails...")
    for i in range(0, len(parsed_rows), BATCH_SIZE):
        batch = parsed_rows[i:i + BATCH_SIZE]
        _upsert_cop_batch(db, batch)
        logger.info(f"  Upserted {min(i + BATCH_SIZE, len(parsed_rows))}/{len(parsed_rows)}")

    success = len(parsed_rows) - errors
    logger.info(f"Parser complete — {success:,} parsed OK, {errors:,} parse errors")
    logger.info("=" * 60)

    return len(parsed_rows)
