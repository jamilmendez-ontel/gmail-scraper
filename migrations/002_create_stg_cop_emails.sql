-- Gmail Scraper — COP email parsed data
-- Populated by parser.py from stg_emails.html_body
-- Safe to re-run: IF NOT EXISTS

CREATE TABLE IF NOT EXISTS data_staging.stg_cop_emails (
    message_id   TEXT        NOT NULL,

    -- Derived from the COP table header row
    -- Values: REVIEW, REVISION, PMI, UNKNOWN
    package_type TEXT,

    -- All label:value pairs extracted from the first COP table.
    -- Stored as JSONB so new field types are captured automatically
    -- without schema changes.
    -- Examples of keys: "Carrier Site Name", "Project ID", "Site Name",
    --   "Structure Type", "Landlord", "Market-County", "GC Name",
    --   "CX Start", "CX Complete", "COP Complete", etc.
    fields       JSONB,

    dropbox_url  TEXT,
    swift_url    TEXT,

    -- Non-null when parsing failed (body unparseable, no COP table found, etc.)
    parse_error  TEXT,

    parsed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT stg_cop_emails_pkey PRIMARY KEY (message_id)
);

CREATE INDEX IF NOT EXISTS stg_cop_emails_package_type_idx
    ON data_staging.stg_cop_emails (package_type);

CREATE INDEX IF NOT EXISTS stg_cop_emails_parsed_at_idx
    ON data_staging.stg_cop_emails (parsed_at DESC);
