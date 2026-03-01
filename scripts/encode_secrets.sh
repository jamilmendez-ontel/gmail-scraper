#!/usr/bin/env bash
# encode_secrets.sh — Base64-encode credential files for GitHub Secrets.
# Run from the gmail-scraper/ root directory.
#   bash scripts/encode_secrets.sh

set -euo pipefail

SCRAPER_CREDS="gmail_credentials/credentials.json"
SCRAPER_TOKEN="gmail_credentials/token.pickle"
NOTIFIER_TOKEN="../local-pipeline/swift_api_pipeline/gmail_credentials/token.pickle"

missing=0
for f in "$SCRAPER_CREDS" "$SCRAPER_TOKEN" "$NOTIFIER_TOKEN"; do
  if [ ! -f "$f" ]; then
    echo "ERROR: File not found: $f"
    missing=1
  fi
done
[ "$missing" -eq 1 ] && exit 1

echo ""
echo "=== SCRAPER_CREDENTIALS_JSON ==="
base64 -w 0 "$SCRAPER_CREDS" 2>/dev/null || base64 -i "$SCRAPER_CREDS"
echo ""

echo ""
echo "=== SCRAPER_TOKEN_PICKLE ==="
base64 -w 0 "$SCRAPER_TOKEN" 2>/dev/null || base64 -i "$SCRAPER_TOKEN"
echo ""

echo ""
echo "=== NOTIFIER_TOKEN_PICKLE ==="
base64 -w 0 "$NOTIFIER_TOKEN" 2>/dev/null || base64 -i "$NOTIFIER_TOKEN"
echo ""

echo ""
echo "Copy each value into the matching GitHub Secret."
echo "Repo -> Settings -> Secrets and variables -> Actions -> New repository secret"
echo ""
echo "Secrets needed:"
echo "  1. SUPABASE_PASSWORD     (plain text from .env)"
echo "  2. SCRAPER_CREDENTIALS_JSON  (value above)"
echo "  3. SCRAPER_TOKEN_PICKLE      (value above)"
echo "  4. NOTIFIER_TOKEN_PICKLE     (value above)"
