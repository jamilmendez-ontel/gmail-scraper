#!/usr/bin/env python3
"""
Gmail API client — OAuth2 authentication and message retrieval.

Setup (one-time):
    1. Google Cloud Console → enable Gmail API
    2. Create OAuth 2.0 Client ID (Desktop app)
    3. Download credentials.json → gmail_credentials/credentials.json
    4. First run opens browser for Google login → saves token.pickle
"""

import pickle
import base64
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Read-only access is sufficient for scraping
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

_BASE_DIR       = Path(__file__).parent
CREDENTIALS_DIR = _BASE_DIR / "gmail_credentials"
CREDENTIALS_FILE = CREDENTIALS_DIR / "credentials.json"
TOKEN_FILE      = CREDENTIALS_DIR / "token.pickle"


def authenticate():
    """
    Authenticate with Gmail API using OAuth2.

    First run opens browser for consent. Subsequent runs use saved token.
    Returns an authorized Gmail API service object.
    """
    creds = None

    if TOKEN_FILE.exists():
        with open(TOKEN_FILE, "rb") as f:
            creds = pickle.load(f)

        # Force re-auth if stored token is missing required scopes
        if creds and hasattr(creds, "scopes") and creds.scopes:
            if not set(SCOPES).issubset(creds.scopes):
                creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CREDENTIALS_FILE.exists():
                raise FileNotFoundError(
                    f"Gmail credentials not found at {CREDENTIALS_FILE}\n"
                    "Download from: Google Cloud Console → APIs & Services → "
                    "Credentials → OAuth 2.0 Client IDs → Download JSON"
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDENTIALS_FILE), SCOPES
            )
            creds = flow.run_local_server(port=0)

        CREDENTIALS_DIR.mkdir(parents=True, exist_ok=True)
        with open(TOKEN_FILE, "wb") as f:
            pickle.dump(creds, f)

    return build("gmail", "v1", credentials=creds)


def search_messages(service, query: str, max_results: int = 500) -> List[Dict]:
    """
    Search Gmail messages matching a query string (paginated).

    Args:
        service: Authorized Gmail API service
        query: Gmail search query e.g. 'in:inbox after:2026/01/01'
        max_results: Maximum messages to return

    Returns:
        List of {id, threadId} dicts
    """
    messages = []
    page_token = None

    while len(messages) < max_results:
        result = service.users().messages().list(
            userId="me",
            q=query,
            pageToken=page_token,
            maxResults=min(max_results - len(messages), 100),
        ).execute()

        batch = result.get("messages", [])
        messages.extend(batch)

        page_token = result.get("nextPageToken")
        if not page_token:
            break

    return messages[:max_results]


def get_full_message(service, message_id: str) -> Dict:
    """
    Fetch a full message including headers and body.

    Returns the raw Gmail API message dict (payload, headers, labelIds, etc.)
    """
    return service.users().messages().get(
        userId="me",
        id=message_id,
        format="full",
    ).execute()


def extract_html_body(payload: dict) -> str:
    """
    Recursively extract the text/html body part from a message payload.

    Gmail messages can be nested multipart — this walks the tree and returns
    the first text/html part found.
    """
    mime_type = payload.get("mimeType", "")
    body_data = payload.get("body", {}).get("data", "")

    if mime_type == "text/html" and body_data:
        return base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")

    for part in payload.get("parts", []):
        result = extract_html_body(part)
        if result:
            return result

    return ""


def extract_plain_text(payload: dict) -> str:
    """
    Recursively extract the text/plain body part from a message payload.
    Used as fallback when there is no HTML body.
    """
    mime_type = payload.get("mimeType", "")
    body_data = payload.get("body", {}).get("data", "")

    if mime_type == "text/plain" and body_data:
        return base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")

    for part in payload.get("parts", []):
        result = extract_plain_text(part)
        if result:
            return result

    return ""
