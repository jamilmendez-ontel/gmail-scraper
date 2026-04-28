-- Gmail Scraper — v_package_emails: per-email rows
-- Replaces the DISTINCT ON (thread_id) dedup with one row per successfully-parsed email.
-- Rationale: the old view kept only the earliest successfully-parsed message per thread,
-- which hid later replies that carried the real REVIEW/COP Complete data.
-- New view surfaces every parsed email; thread_id column lets consumers group as needed.

CREATE OR REPLACE VIEW analytics.v_package_emails AS
SELECT
    c.message_id,
    e.thread_id,
    (e.received_at AT TIME ZONE 'America/New_York'::text) AS received_at_et,
    e.sender_email,
    e.subject,
    TRIM(BOTH FROM regexp_replace(regexp_replace(regexp_replace(e.subject, '\[External\]\s*'::text, ''::text, 'gi'::text), '\m[Rr][Ee]:\s*'::text, ''::text, 'g'::text), '\s{2,}'::text, ' '::text, 'g'::text)) AS clean_subject,
    c.package_type,
    COALESCE(c.fields ->> 'SITE ID'::text, c.fields ->> 'Site ID'::text, c.fields ->> 'Landlord Site ID'::text) AS site_id,
    COALESCE(c.fields ->> 'SITE NAME'::text, c.fields ->> 'Site Name'::text, c.fields ->> 'Carrier Site Name'::text) AS site_name,
    COALESCE(c.fields ->> 'GC NAME'::text, c.fields ->> 'GC Name'::text) AS gc_name,
    COALESCE(c.fields ->> 'LANDLORD'::text, c.fields ->> 'Landlord'::text) AS landlord,
    COALESCE(c.fields ->> 'PROJECT'::text, c.fields ->> 'Project Type'::text, c.fields ->> 'Carrier Project Type'::text) AS project,
    COALESCE(c.fields ->> 'PROJECT ID'::text, c.fields ->> 'Project ID'::text, c.fields ->> 'Carrier Project ID'::text) AS project_id,
    COALESCE(c.fields ->> 'MARKET'::text, c.fields ->> 'Market-County'::text, c.fields ->> 'County'::text) AS market,
    COALESCE(c.fields ->> 'STRUCTURE TYPE'::text, c.fields ->> 'Structure Type'::text) AS structure_type,
    c.fields ->> 'CM Company'::text AS cm_company,
    c.fields ->> 'CM Name'::text AS cm_name,
    c.fields ->> 'Project Manager'::text AS project_manager,
    c.fields ->> 'Equipment Engineer'::text AS equipment_engineer,
    COALESCE(c.fields ->> 'Construction Engineer'::text, c.fields ->> 'A&E Company'::text) AS construction_engineer,
    COALESCE(c.fields ->> 'RAW FILES RECEIVED'::text, c.fields ->> 'COP Raw Files Received'::text) AS raw_files_received,
    COALESCE(c.fields ->> 'CX START'::text, c.fields ->> 'CX Start'::text) AS cx_start,
    COALESCE(c.fields ->> 'CX COMPLETE'::text, c.fields ->> 'CX Complete'::text) AS cx_complete,
    c.fields ->> 'CX Duration'::text AS cx_duration,
    COALESCE(c.fields ->> 'LIVE REVIEW COMPLETE'::text, c.fields ->> 'Live Review Complete'::text) AS live_review_complete,
    c.fields ->> 'Live Review Duration'::text AS live_review_duration,
    COALESCE(c.fields ->> 'REVISION FILES RECEIVED'::text, c.fields ->> 'Revision Files Received'::text) AS revision_files_received,
    COALESCE(c.fields ->> 'REVISION COMPLETE'::text, c.fields ->> 'Revision Complete'::text) AS revision_complete,
    COALESCE(c.fields ->> 'COP COMPLETE'::text, c.fields ->> 'COP Complete'::text) AS cop_complete,
    c.fields ->> 'COP Status'::text AS cop_status,
    c.fields ->> 'COP Duration'::text AS cop_duration,
    c.fields ->> 'COP Raw File Duration'::text AS cop_raw_file_duration,
    c.fields ->> 'Cutover Complete'::text AS cutover_complete,
    c.fields ->> '48Hr Raw File Duration'::text AS hr48_raw_file_duration,
    c.fields ->> '48Hr Package Duration'::text AS hr48_package_duration,
    c.fields ->> '48Hr Raw Files Received'::text AS hr48_raw_files_received,
    c.fields ->> '48Hr Package Complete'::text AS hr48_package_complete,
    c.fields ->> 'PMI COP Complete'::text AS pmi_cop_complete,
    c.fields ->> 'Smart Tool Project #'::text AS smart_tool_project_num,
    c.fields ->> 'MDG Location ID'::text AS mdg_location_id,
    c.fields ->> 'Landlord Site Name'::text AS landlord_site_name,
    COALESCE(c.fields ->> 'LL COP Complete'::text, c.fields ->> 'LL COP COMPLETE'::text) AS ll_cop_complete,
    c.fields ->> 'Open Items'::text AS open_items,
    c.dropbox_url,
    c.swift_url,
    (c.parsed_at AT TIME ZONE 'America/New_York'::text) AS parsed_at_et
FROM data_staging.stg_package_emails c
JOIN data_staging.stg_emails e USING (message_id)
WHERE c.parse_error IS NULL
ORDER BY e.thread_id, e.received_at;
