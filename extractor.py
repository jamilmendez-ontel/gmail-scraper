#!/usr/bin/env python3
"""
Gmail scraper — incremental email extraction to Supabase.

Flow:
  1. Query MAX(received_at) from raw_emails → build after: filter
  2. Authenticate Gmail
  3. Search messages with query + date filter
  4. For each message: parse headers + HTML body → insert raw + staging
  5. ON CONFLICT (message_id) DO NOTHING → safe to re-run
"""

import email.utils
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

from config import (
    SCHEMA_RAW, SCHEMA_STAGING, SCHEMA_PIPELINE,
    GMAIL_QUERY, GMAIL_DAYS_BACK, GMAIL_MAX_RESULTS,
    get_logger,
)
from db import get_db, retry_db
from gmail_client import (
    authenticate,
    search_messages,
    get_full_message,
    extract_html_body,
    extract_plain_text,
)

logger = get_logger("extractor")

BATCH_SIZE = 50  # Emails per DB batch (bodies can be large)


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_last_received_at(db) -> Optional[datetime]:
    """Return the most recent received_at already in raw_emails, or None."""
    row = db.fetchrow(
        f"SELECT MAX(received_at) AS last_at FROM {SCHEMA_RAW}.raw_emails"
    )
    return row["last_at"] if row and row["last_at"] else None


def get_existing_message_ids(db) -> set:
    """Return all message_ids already loaded (for in-memory dedup)."""
    rows = db.fetch(
        f"SELECT message_id FROM {SCHEMA_RAW}.raw_emails"
    )
    return {r["message_id"] for r in rows}


def insert_raw_batch(db, rows: List[Dict]):
    """Batch insert into data_raw.raw_emails. Skips duplicates."""
    tuples = [
        (
            r["message_id"], r["thread_id"], r["sender"],
            r["recipients"], r["subject"], r["received_at"],
            r["html_body"], r["headers"], r["labels"],
        )
        for r in rows
    ]
    retry_db(
        lambda: db.executemany(
            f"""
            INSERT INTO {SCHEMA_RAW}.raw_emails
                (message_id, thread_id, sender, recipients, subject,
                 received_at, html_body, headers, labels)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (message_id) DO NOTHING
            """,
            tuples,
        ),
        description="insert raw_emails batch",
    )


def insert_stg_batch(db, rows: List[Dict]):
    """Batch insert into data_staging.stg_emails. Skips duplicates."""
    tuples = [
        (
            r["message_id"], r["thread_id"], r["sender_email"],
            r["sender_name"], r["recipients_to"], r["recipients_cc"],
            r["subject"], r["received_at"], r["html_body"],
        )
        for r in rows
    ]
    retry_db(
        lambda: db.executemany(
            f"""
            INSERT INTO {SCHEMA_STAGING}.stg_emails
                (message_id, thread_id, sender_email, sender_name,
                 recipients_to, recipients_cc, subject, received_at, html_body)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (message_id) DO NOTHING
            """,
            tuples,
        ),
        description="insert stg_emails batch",
    )


# ── Parsing ───────────────────────────────────────────────────────────────────

def _parse_sender(from_header: str):
    """Split 'Display Name <email@domain.com>' into (name, email)."""
    name, addr = email.utils.parseaddr(from_header)
    return (name.strip() or None), (addr.strip() or from_header.strip())


def _parse_address_list(header_value: str) -> List[str]:
    """Parse a To/Cc header into a list of email addresses."""
    if not header_value:
        return []
    addresses = email.utils.getaddresses([header_value])
    return [addr for _, addr in addresses if addr]


def _parse_message(raw_msg: dict) -> dict:
    """Parse a raw Gmail API message into flat dicts for raw + staging tables."""
    payload     = raw_msg.get("payload", {})
    headers_list = payload.get("headers", [])
    headers     = {h["name"]: h["value"] for h in headers_list}

    # Timestamp
    internal_date_ms = int(raw_msg.get("internalDate", 0))
    received_at = datetime.fromtimestamp(internal_date_ms / 1000, tz=timezone.utc)

    # Core fields
    from_header = headers.get("From", "")
    subject     = headers.get("Subject", "(no subject)")
    sender_name, sender_email = _parse_sender(from_header)

    recipients = {
        "to":  headers.get("To", ""),
        "cc":  headers.get("Cc", ""),
        "bcc": headers.get("Bcc", ""),
    }
    recipients_to = _parse_address_list(headers.get("To", ""))
    recipients_cc = _parse_address_list(headers.get("Cc", ""))

    # Body — prefer HTML, fall back to plain text
    html_body = extract_html_body(payload) or extract_plain_text(payload)

    labels = raw_msg.get("labelIds", [])

    return {
        # raw
        "message_id":    raw_msg["id"],
        "thread_id":     raw_msg.get("threadId"),
        "sender":        from_header,
        "recipients":    recipients,
        "subject":       subject,
        "received_at":   received_at,
        "html_body":     html_body,
        "headers":       headers,
        "labels":        labels,
        # staging extras
        "sender_name":   sender_name,
        "sender_email":  sender_email,
        "recipients_to": recipients_to,
        "recipients_cc": recipients_cc,
    }


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_scraper(
    reprocess: bool = False,
    query: str = None,
    max_results: int = None,
):
    """
    Incremental Gmail scraper.

    Args:
        reprocess: If True, ignore last seen date and re-fetch from GMAIL_DAYS_BACK.
        query: Override GMAIL_QUERY from .env.
        max_results: Override GMAIL_MAX_RESULTS from .env.
    """
    base_query   = query or GMAIL_QUERY
    max_results  = max_results or GMAIL_MAX_RESULTS

    logger.info(f"\n{'='*60}")
    logger.info(f"Gmail Scraper")
    logger.info(f"Base query: {base_query}")
    logger.info(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")
    logger.info(f"{'='*60}\n")

    db = get_db()

    # ── Step 1: Determine date filter ─────────────────────────────────────────
    last_received_at = None if reprocess else get_last_received_at(db)

    if last_received_at:
        # Use the day BEFORE last seen to avoid missing same-day emails
        after_date = (last_received_at - timedelta(days=1)).strftime("%Y/%m/%d")
        full_query = f"{base_query} after:{after_date}"
        logger.info(f"Incremental mode: fetching emails after {after_date}")
    else:
        after_date = (datetime.now(timezone.utc) - timedelta(days=GMAIL_DAYS_BACK)).strftime("%Y/%m/%d")
        full_query = f"{base_query} after:{after_date}"
        logger.info(f"First run / reprocess: fetching last {GMAIL_DAYS_BACK} days (after {after_date})")

    # ── Step 2: Load existing message IDs for dedup ───────────────────────────
    existing_ids = get_existing_message_ids(db)
    logger.info(f"Existing emails in DB: {len(existing_ids):,}")

    # ── Step 3: Authenticate + search ─────────────────────────────────────────
    logger.info("Authenticating with Gmail...")
    service = authenticate()
    logger.info("Authenticated successfully")

    logger.info(f"Searching: {full_query}")
    messages = search_messages(service, full_query, max_results=max_results)
    logger.info(f"Found {len(messages)} messages matching query")

    if not messages:
        logger.info("No new emails found. Nothing to do.")
        return

    # ── Step 4: Filter already-loaded, sort oldest-first ─────────────────────
    new_message_ids = [m["id"] for m in messages if m["id"] not in existing_ids]
    logger.info(f"New emails to load: {len(new_message_ids):,} "
                f"({len(messages) - len(new_message_ids):,} already in DB)")

    if not new_message_ids:
        logger.info("All matched emails already loaded.")
        return

    # ── Step 5: Fetch full messages + parse ───────────────────────────────────
    logger.info("Fetching full message details...")
    parsed = []
    for i, msg_id in enumerate(new_message_ids, 1):
        raw_msg = get_full_message(service, msg_id)
        parsed.append(_parse_message(raw_msg))
        if i % 50 == 0:
            logger.info(f"  Fetched {i}/{len(new_message_ids)}")

    # Sort chronologically (oldest first) before loading
    parsed.sort(key=lambda m: m["received_at"])

    # ── Step 6: Batch insert raw + staging ────────────────────────────────────
    logger.info(f"Loading {len(parsed):,} emails to Supabase...")
    total_loaded = 0

    for i in range(0, len(parsed), BATCH_SIZE):
        batch = parsed[i:i + BATCH_SIZE]
        insert_raw_batch(db, batch)
        insert_stg_batch(db, batch)
        total_loaded += len(batch)
        logger.info(f"  Loaded {total_loaded}/{len(parsed)}")

    # ── Step 7: Summary ───────────────────────────────────────────────────────
    if parsed:
        earliest = parsed[0]["received_at"].strftime("%Y-%m-%d %H:%M UTC")
        latest   = parsed[-1]["received_at"].strftime("%Y-%m-%d %H:%M UTC")
    else:
        earliest = latest = "—"

    logger.info(f"\n{'='*60}")
    logger.info(f"Scraper complete")
    logger.info(f"  Emails loaded:  {total_loaded:,}")
    logger.info(f"  Already in DB:  {len(messages) - len(new_message_ids):,}")
    logger.info(f"  Date range:     {earliest} to {latest}")
    logger.info(f"{'='*60}\n")
