# Archive Fastmail Emails After YNAB Upload

## Problem

After the tool uploads transactions to YNAB, the source emails remain in the Fastmail Inbox. Since these emails have been processed, they clutter the Inbox unnecessarily. The user must manually archive them.

## Solution

After successful YNAB upload, automatically archive the source emails in Fastmail by removing them from Inbox and adding them to the Archive mailbox via JMAP `Email/set`.

## Behavior

- Archiving happens automatically after successful YNAB upload (no extra confirmation prompt)
- Only emails whose transactions were successfully created in YNAB are archived
- If a batch partially fails, only the successful emails are archived
- If archiving itself fails, log a warning but don't crash -- YNAB transactions are already safe
- "Archive" means removing from Inbox (standard Fastmail archive behavior); emails remain searchable in Archive/All Mail

## JMAP Approach

Use `Email/set` with PatchObject patch paths (RFC 8620 Section 5.3):

```json
["Email/set", {
  "accountId": "...",
  "update": {
    "emailId1": {
      "mailboxIds/<inbox_id>": null,
      "mailboxIds/<archive_id>": true
    },
    "emailId2": {
      "mailboxIds/<inbox_id>": null,
      "mailboxIds/<archive_id>": true
    }
  }
}, "0"]
```

- `null` removes the email from the Inbox mailbox
- `true` adds the email to the Archive mailbox
- Both changes are atomic per email, so the email is never in zero mailboxes (JMAP requirement)
- Other mailbox memberships (labels, folders) are preserved
- All emails are updated in a single `Email/set` call

### Mailbox Discovery

- Inbox: `Mailbox/query` with `filter: {"name": "Inbox"}` (matches existing pattern in `fetch_recent_emails`)
- Archive: `Mailbox/query` with `filter: {"role": "archive"}` (RFC 8621 standard role)
- Both queries are batched into a single JMAP HTTP request

## New Function

`archive_fastmail_emails(token: str, email_ids: list[str]) -> int`

1. Get JMAP session via existing `get_jmap_session()`
2. Batch-query for Inbox and Archive mailbox IDs
3. Build `Email/set` update with patch paths for all email IDs
4. Check `notUpdated` in response for per-email failures, log warnings
5. Return count of successfully archived emails

Placed in the Fastmail JMAP section of the file, after `fetch_recent_emails()`.

## Integration Point

In `process_emails()`, after the transaction summary table and before the final stats line. Uses `created_email_ids` (already tracked) as the input.

## Fastmail Token Permissions

The Fastmail API token now needs **mail read and write** access (previously only read). `Email/set` requires write permission. Documentation must be updated to reflect this.

## Edge Cases

| Case | Handling |
|------|----------|
| No emails to archive | Guard clause returns 0, no JMAP call made |
| Archive mailbox doesn't exist | Log warning, return 0, script continues |
| Email already removed from Inbox | JMAP returns `notUpdated` for that email; others still archived |
| Email in multiple mailboxes | Only Inbox membership removed; other memberships preserved |
| Token lacks write permission | Exception caught, warning logged, script continues |
| Network failure during archive | Exception caught, warning logged; YNAB transactions already safe |
| Partial failure | Successes counted from `updated`, failures logged from `notUpdated` |

## Documentation Updates

- **Module docstring**: Add archiving step to workflow, update FASTMAIL_TOKEN description
- **CLAUDE.md**: Update Architecture item 1
- **README.md**: Add step 7 to "How it works", update Fastmail token setup, add troubleshooting entry
