# Gmail Scraper

Incremental Gmail scraper that extracts COP (Close Out Package) emails from the Ontel Gmail inbox and stores structured data in Supabase.

## What It Does

1. **Scrapes** Gmail for emails matching `swiftprojects.io from:ontel.co` (COP workflow emails from all Ontel teams)
2. **Stores** raw email headers + full HTML body in `data_raw.raw_emails` and `data_staging.stg_emails`
3. **Parses** the first "CLOSE OUT PACKAGE" table from each email body, extracting all label:value pairs into `data_staging.stg_cop_emails`
4. **Runs** nightly at 11 PM via Windows Task Scheduler — incremental, only fetches emails newer than the last loaded date

## Project Structure

```
gmail-scraper/
├── config.py                  # Env vars, logging, schema constants
├── db.py                      # asyncpg pool + sync bridge (same pattern as local-pipeline)
├── gmail_client.py            # OAuth2 auth, Gmail API search + fetch
├── extractor.py               # Incremental scraper → raw_emails + stg_emails
├── parser.py                  # COP table HTML parser → stg_cop_emails
├── main.py                    # CLI entry point
├── scheduled_gmail_scraper.bat # Windows Task Scheduler wrapper
├── requirements.txt
├── .env                       # Local config (not committed)
├── .env.example               # Template
├── gmail_credentials/
│   ├── credentials.json       # OAuth client ID (not committed)
│   └── token.pickle           # Saved OAuth token (not committed)
└── migrations/
    ├── 001_create_emails_tables.sql
    └── 002_create_stg_cop_emails.sql
```

## Setup

### 1. Python environment

```bash
python -m venv venv
venv\Scripts\pip install -r requirements.txt
```

### 2. Environment variables

Copy `.env.example` to `.env` and fill in the Supabase password:

```bash
cp .env.example .env
```

### 3. Gmail credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/) → APIs & Services → Credentials
2. Download OAuth 2.0 Client ID JSON → save as `gmail_credentials/credentials.json`
3. Run the scraper once to trigger the browser OAuth flow:
   ```bash
   venv\Scripts\python main.py
   ```
   Log in with the Ontel Google account. Token saved to `gmail_credentials/token.pickle` — all future runs are headless.

### 4. Database migration

Run both migration files against the Supabase project (already applied to `voqfjfngdpcvevbkikud`):

```sql
-- migrations/001_create_emails_tables.sql
-- migrations/002_create_stg_cop_emails.sql
```

## Usage

```bash
# Normal incremental run (scrape + parse)
venv\Scripts\python main.py

# Re-fetch last 30 days + re-parse everything
venv\Scripts\python main.py --reprocess

# Re-parse already-scraped emails (no Gmail API call)
venv\Scripts\python main.py --parse-only --reparse

# Override query or result limit
venv\Scripts\python main.py --query "swiftprojects.io from:ontel.co" --max-results 1000
```

## Database Tables

### `data_raw.raw_emails`
Raw email as received from Gmail API.

| Column | Type | Description |
|--------|------|-------------|
| message_id | TEXT PK | Gmail message ID |
| thread_id | TEXT | Gmail thread ID |
| sender | TEXT | Raw From header |
| recipients | JSONB | `{"to": "...", "cc": "...", "bcc": "..."}` |
| subject | TEXT | |
| received_at | TIMESTAMPTZ | From Gmail `internalDate` |
| html_body | TEXT | Full HTML body (or plain text fallback) |
| headers | JSONB | All email headers as key:value |
| labels | JSONB | Gmail label IDs |

### `data_staging.stg_emails`
Parsed email with normalized sender/recipient fields.

| Column | Type | Description |
|--------|------|-------------|
| message_id | TEXT PK | |
| sender_email | TEXT | Extracted email address from From header |
| sender_name | TEXT | Display name from From header |
| recipients_to | TEXT[] | Parsed To addresses |
| recipients_cc | TEXT[] | Parsed Cc addresses |
| html_body | TEXT | Same as raw (avoids cross-schema joins) |

### `data_staging.stg_cop_emails`
Structured data extracted from the COP table in the email body.

| Column | Type | Description |
|--------|------|-------------|
| message_id | TEXT PK | |
| package_type | TEXT | `REVIEW`, `REVISION`, or `PMI` |
| fields | JSONB | All label:value pairs from the first COP table |
| dropbox_url | TEXT | Dropbox download link |
| swift_url | TEXT | Swift Projects link |
| parse_error | TEXT | Non-null if no COP table found (expected for reply threads) |

**`fields` JSONB examples by package type:**

- **REVIEW**: `Site Name`, `Project ID`, `Structure Type`, `Landlord`, `Market-County`, `GC Name`, `CX Start`, `CX Complete`, `COP Complete`, `Live Review Complete`, ...
- **REVISION**: `Site Name`, `Project ID`, `Structure Type`, `Market`, `GC Name`, `Revision Files Received`, `Revision Complete`, ...
- **PMI**: `Carrier Site Name`, `Carrier Project ID`, `Carrier Project Type`, `MDG Location ID`, `PMI COP Complete`, ...

New field types are captured automatically in JSONB without schema changes.

## Gmail Query

**Current:** `swiftprojects.io from:ontel.co`

- `swiftprojects.io` — body keyword present in every COP email regardless of team, layout, or reply status
- `from:ontel.co` — restricts to Ontel team senders, excludes external replies and pipeline notification emails

**Teams captured:** `vzw.cgc`, `vzw.bawa`, `vzw.norcal`, `vzw.mp`, `vzw.aahi`, `ftth`, `att.oh`

## Scheduled Run

Task: `GmailScraper-Nightly` — daily at 11:00 PM via Windows Task Scheduler.

To recreate the task:
```powershell
powershell -ExecutionPolicy Bypass -File create_task.ps1
```

Logs written to `logs/scraper_YYYYMMDD_HHMMSS.log`.
