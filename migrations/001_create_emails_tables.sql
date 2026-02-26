-- Gmail Scraper — initial schema
-- Creates raw_emails (data_raw) and stg_emails (data_staging)
-- Safe to re-run: all DDL uses IF NOT EXISTS

-- ─────────────────────────────────────────────────────────────────────────────
-- Schema guards (schemas should already exist in Supabase, but just in case)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS data_raw;
CREATE SCHEMA IF NOT EXISTS data_staging;


-- ─────────────────────────────────────────────────────────────────────────────
-- data_raw.raw_emails
-- Stores every email exactly as received from Gmail API.
-- html_body  : full HTML (or plain text fallback) — can be large
-- headers    : all headers as JSONB  {"From": "...", "Subject": "...", ...}
-- labels     : Gmail label IDs array e.g. ["INBOX", "CATEGORY_PROMOTIONS"]
-- recipients : raw To/Cc/Bcc strings as JSONB  {"to": "...", "cc": "...", "bcc": "..."}
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_raw.raw_emails (
    message_id   TEXT        NOT NULL,
    thread_id    TEXT,
    sender       TEXT,                          -- raw From header value
    recipients   JSONB,                         -- {"to": "...", "cc": "...", "bcc": "..."}
    subject      TEXT,
    received_at  TIMESTAMPTZ NOT NULL,
    html_body    TEXT,
    headers      JSONB,
    labels       JSONB,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT raw_emails_pkey PRIMARY KEY (message_id)
);

CREATE INDEX IF NOT EXISTS raw_emails_received_at_idx
    ON data_raw.raw_emails (received_at DESC);

CREATE INDEX IF NOT EXISTS raw_emails_thread_id_idx
    ON data_raw.raw_emails (thread_id);


-- ─────────────────────────────────────────────────────────────────────────────
-- data_staging.stg_emails
-- Parsed / normalised version of raw_emails.
-- sender_email    : extracted email address from From header
-- sender_name     : display name from From header (nullable)
-- recipients_to   : array of email addresses from To header
-- recipients_cc   : array of email addresses from Cc header
-- html_body       : same body as raw (kept here to avoid cross-schema joins)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_staging.stg_emails (
    message_id      TEXT        NOT NULL,
    thread_id       TEXT,
    sender_email    TEXT,
    sender_name     TEXT,
    recipients_to   TEXT[],
    recipients_cc   TEXT[],
    subject         TEXT,
    received_at     TIMESTAMPTZ NOT NULL,
    html_body       TEXT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT stg_emails_pkey PRIMARY KEY (message_id)
);

CREATE INDEX IF NOT EXISTS stg_emails_received_at_idx
    ON data_staging.stg_emails (received_at DESC);

CREATE INDEX IF NOT EXISTS stg_emails_sender_email_idx
    ON data_staging.stg_emails (sender_email);

CREATE INDEX IF NOT EXISTS stg_emails_thread_id_idx
    ON data_staging.stg_emails (thread_id);
