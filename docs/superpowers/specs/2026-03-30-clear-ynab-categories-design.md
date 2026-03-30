# Clear YNAB Auto-Assigned Categories

## Problem

YNAB auto-assigns categories to transactions based on payee history when `category_id` is null or omitted during creation. This causes incorrect categories (e.g., "Inflow: Ready to Assign" on outflow transactions). Setting `category_id: null` in the creation payload does not prevent this behavior.

## Solution

Two-step approach: create transactions normally, then immediately PATCH them to clear auto-assigned categories. Skip clearing for inflow transactions where YNAB's auto-assigned "Inflow: Ready to Assign" category is correct.

## Design

### Regular Transactions (batch path)

1. Create transactions via `POST /budgets/{id}/transactions` (existing flow, unchanged)
2. From the response, collect transaction IDs where `is_inflow=False` and the transaction was not a duplicate
3. Send a single `PATCH /budgets/{id}/transactions` with:
   ```json
   {"transactions": [{"id": "xxx", "category_id": null}, ...]}
   ```
4. Log the number of categories cleared

### Scheduled Transactions

1. Create via `POST /budgets/{id}/scheduled_transactions` (existing flow, unchanged)
2. If `is_inflow=False`, send `PUT /budgets/{id}/scheduled_transactions/{id}` with:
   ```json
   {"scheduled_transaction": {"category_id": null}}
   ```

### Skip Clearing For

- Inflow transactions (`is_inflow=True`) — "Inflow: Ready to Assign" is the correct category
- Duplicate transactions (no ID returned from creation)

### Error Handling

- If PATCH/PUT to clear categories fails, log a warning but do not fail the run
- The transactions are already created; worst case is they have an auto-assigned category the user must fix manually

## Files to Modify

- `fastmail2ynab.py` — add `clear_ynab_transaction_categories()` function and call it after batch creation; add category clearing after scheduled transaction creation

## Verification

1. `uv run fastmail2ynab.py --help` — no syntax errors
2. Hook auto-runs ruff + pyright
3. `uv run fastmail2ynab.py --force` — test with real email, verify in YNAB that outflow transactions have no category and inflow transactions have "Inflow: Ready to Assign"
