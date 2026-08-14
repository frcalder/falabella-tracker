# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pipenv install

# Run scraper (Playwright, writes to Supabase PostgreSQL)
pipenv run python main.py --mode scraper

# Run with limit for testing (N movements per page)
pipenv run python main.py --mode scraper --limit 1

# Run headless
pipenv run python main.py --mode scraper --headless

# Debug mode (saves screenshots to debug/)
pipenv run python main.py --mode scraper --debug

# Launch Streamlit dashboard
pipenv run python main.py --mode dashboard
# or directly:
pipenv run streamlit run dashboard/visualizer.py

# Install Playwright browsers (first time)
pipenv run playwright install chromium

# Apply DB schema (creates tables if not exist)
pipenv run python -c "from analytics.db import get_connection, init_db; conn = get_connection(); init_db(conn); conn.close()"
```

Credentials go in `.env` (see `env.example`): `FALABELLA_USER`, `FALABELLA_PASSWORD`, `DATABASE_URL`.
For local Streamlit secrets (GITHUB_TOKEN): `.streamlit/secrets.toml`.

## Architecture

The bank's frontend is an Angular SPA with **Shadow DOM** — standard CSS selectors and Playwright locators don't reach inside components. All DOM interaction (modal extraction, pagination buttons, close button) is done via `page.evaluate()` with custom JS that traverses shadow roots recursively.

Data is stored in **Supabase PostgreSQL**. The scraper runs daily via **GitHub Actions** and can also be triggered manually from the Streamlit dashboard.

### Key design decisions

**`scraper/bank_scraper.py`** — the active scraper (Playwright async). Writes directly to Supabase via psycopg2.

**Shadow DOM traversal pattern**: Three JS constants handle the main interactions:
- `JS_EXTRACT_FIELDS` — finds the detail modal by searching for anchor label pairs (`Rubro`+`Hora`, etc.) then climbs the DOM to a container that has both. Extracts label→value pairs for `rubro`, `comercio`, `modal_cuotas`, `fecha_compra`, `hora`. Uses `wait_for_function` to wait for `Comercio` to appear before extracting (headless timing fix). `modal_cuotas` present = confirmed, absent = pending.
- `JS_CLOSE_MODAL` — traverses shadow roots looking for a × button, falls back to backdrop click.
- `JS_NEXT_PAGE_RECT` — finds all `btn-move` class buttons (SVG only, no text), returns bounding rect of the last one (next›). Returns `null` if that button is disabled (last page).

**Single table**: All movements (pending and confirmed) appear in one table. `_read_row` always returns `pendiente=False`; the real signal is decided in `extract_all_movements` and depends on which structure the bank is currently serving (the scraper handles all three):
- Auth code present in the modal (**current behavior**, and the pre-April-2026 one): both pendientes and confirmados carry an auth code, so the discriminator is the table's `fecha` column — **no date = pendiente**. Pendientes do carry a cuotas value (`01/01`) in this structure.
- Auth code absent but modal loaded (April–early May 2026): `modal_cuotas` present = confirmed, absent = pendiente. Pendientes have no value in the cuotas column (`/`) and the table does show a date.
- Modal didn't load: fallback to `DATE_RE` over the table's date.

**Checkpoint / resume**: `existing_keys` (loaded from DB at init) tracks processed confirmed movements. Key format: `(fecha, descripcion, abs(monto_int), num_cuotas)` — derived from table columns, available before opening the modal. `num_cuotas` differentiates installments of the same purchase (e.g. cuota 01/03 vs 02/03). The skip check only applies to rows with a `num_cuotas` value; in the structure where pendientes have no cuota value, that alone re-processes them. When pendientes *do* carry a cuota value (current structure), they are still re-processed every run because `_load_existing_keys` only loads `pendiente = FALSE` rows, so a pendiente's key is never in `existing_keys`. Monto is normalized to int for comparison between raw cell text and DB Decimal.

`incomplete_keys` holds confirmed rows missing `rubro` or `comercio`. If a movement's key is in `incomplete_keys`, it is re-processed even if already in `existing_keys`, allowing a later scraper run to fill in modal data that failed previously.

**Pending movements strategy**: At the start of each run (`_reset_pending()`), all `pendiente=TRUE` rows are deleted from DB. They get re-inserted fresh during the run. This handles deduplication, monto changes, pending→confirmed transitions, and disappeared transactions.

**tx_hash**:
- Con `codigo_autorizacion`: `NULL` — auth code is the real identifier; hash is redundant
- Sin `codigo_autorizacion` (pendientes o confirmadas, según lo que sirva el banco): `sha256(fecha_compra|descripcion|monto)[:16]` — permite clasificar pendientes y que la clasificación persista cuando se confirman (mismo hash, mismos inputs)

**Installment uniqueness**: Two paths depending on whether the bank provides `codigo_autorizacion`. **Both are live** — the scraper always extracts the auth code when present and falls back to the hash when it isn't; this is the steady state, not a migration in progress:
- With auth code: `UNIQUE (codigo_autorizacion, num_cuotas)` — one row per installment. Upsert uses `ON CONFLICT (codigo_autorizacion, num_cuotas)`. Migration: `analytics/migrations/002_cuotas_unique.sql`.
- Without auth code: `tx_hash = sha256(fecha_compra|descripcion|monto)` per purchase. Uniqueness via partial index `(tx_hash, periodo) WHERE tx_hash IS NOT NULL` — within a period there is exactly one cuota per purchase. Upsert uses `ON CONFLICT (tx_hash, periodo)`. Migration: `analytics/migrations/005_tx_hash_cuotas_unique.sql`.
- **Which path applies**: whatever the modal served for that row — **pendientes included**. Since the auth code came back (~2026-05-08) most pendientes arrive with one and take the auth path directly (`tx_hash` NULL); the hash path covers rows whose modal had no auth code: everything scraped during the April–early May 2026 gap, plus the ~4–6 confirmed rows per period where the field is still missing. Coverage has been ~95% since June 2026. Assume either path can apply to any given row, pendiente or not.

**Pendiente → confirmado key migration**: relevant only for pendientes stored **without** an auth code (i.e. keyed by `tx_hash`). If such a pendiente is confirmed and the modal now serves an auth code, the row's `tx_hash` becomes NULL and its identity moves to `codigo_autorizacion`. A pendiente that already had an auth code keeps the same key when confirmed, so nothing needs migrating. `_save_movement` migrates `clasificaciones` and `splits` to the new key in the same transaction (see the block after the upsert), so classifications survive the switch. The same block covers the case where `tx_hash` itself changes because `fecha_compra` wasn't available on the pendiente.

**Pagination**: `_go_next_page` uses `wait_for_function` to detect when the first row text changes after clicking next, avoiding false loop detection when all rows are skipped.

**Execution logging**: Every run inserts a row in `scraper_runs` with start/end time, status, counters (nuevos, actualizados, pendientes, páginas) and error message.

### Database schema (`analytics/schema.sql`)

| Table | Purpose |
|---|---|
| `movimientos` | All scraped transactions |
| `categorias` | User-defined categories with color |
| `clasificaciones` | Maps `codigo_autorizacion` or `tx_hash` → `categoria_id` |
| `splits` | Split allocations: one row per (transaction, category) for divided movements |
| `presupuestos` | Monthly budget per category and billing period |
| `reglas_sugerencia` | Merchant → category frequency for suggestions |
| `scraper_runs` | Execution log for every scraper run |

### `splits` schema

Allows distributing a single transaction across multiple categories with partial amounts. A transaction with splits takes precedence over `clasificaciones` — `loader.py` marks it as `is_split=True` with `categoria_nombre="✂ DIVIDIDO"`. Analytics calls `expand_splits(df, conn)` before aggregating to expand each split transaction into N rows with their respective `categoria_id` and `monto`.

| Column | Notes |
|---|---|
| `codigo_autorizacion` | Auth code of the source transaction (or NULL) |
| `tx_hash` | Fallback key when no auth code (or NULL) |
| `categoria_id` | Category for this split part |
| `monto` | Amount assigned to this category |

Split key is `codigo_autorizacion` alone (same as `clasificaciones`) — a split applies to **all installments** of the same purchase, not per cuota. Splits can be created on pending movements too; the split persists when the movement is confirmed (same behavior as classifications). Reclassifying a split (from Clasificación or Análisis) deletes the split first and saves a direct classification — the full movement goes to one category.

### `movimientos` schema

| Column | Notes |
|---|---|
| `fecha` | Display date (all movements have a date, including pending) |
| `descripcion` | Transaction description |
| `persona` | TITULAR / additional cardholder |
| `monto` | Amount |
| `monto_periodo` | Installment amount for the period (valor_cuota or monto) |
| `pendiente` | TRUE if unconfirmed |
| `rubro` | Category from bank modal |
| `comercio` | Merchant name from modal |
| `codigo_autorizacion` | Auth code — part of composite unique key `(codigo_autorizacion, num_cuotas)`. The bank stopped serving it in April 2026 and resumed ~2026-05-08; ~95% of rows have it since June 2026. NULL only for rows whose modal didn't serve it — those use `tx_hash`. Pendientes normally do have an auth code now. |
| `fecha_compra` | Purchase date from modal (may differ from `fecha`) |
| `hora` | Purchase time from modal |
| `pais` | Country from modal — missing during the April–early May 2026 gap, served again since then |
| `origen` | Purchase origin type from modal — missing during the April–early May 2026 gap, served again since then |
| `periodo_facturacion` | "DD/MM/YYYY" closing date of billing cycle |
| `periodo` | "YYYY-MM" derived from `periodo_facturacion` |
| `num_cuotas` | Number of installments |
| `valor_cuota` | Amount per installment |
| `tx_hash` | Fallback unique key (see tx_hash rules above) |

### Analytics delta comparison

The "Total gastado" metric in Análisis compares current period spending against the **proportionally equivalent point** of the previous period. If today is day 5 of a 31-day period, it compares against the first `5/31 * prev_period_days` days of the previous period (filtered by `fecha`). For completed periods, compares full totals. The delta label shows "vs día X/Y mes anterior".

### Billing periods

The bank closes billing cycles on the 19th of each month. `periodo_facturacion` = "19/MM/YYYY" → `periodo` = "YYYY-MM". The `periodo_label` displayed in the UI is "20/MM-1 - 19/MM/YYYY". All pages (Clasificación, Presupuesto, Análisis) use the same `periodo` key for cross-referencing.

### Infrastructure

- **Scraper**: GitHub Actions (`.github/workflows/scraper.yml`), runs daily at 11:00 UTC, also triggerable from Streamlit dashboard via GitHub API.
- **Dashboard**: Streamlit Cloud (private app), auto-deploys on push to `main`.
- **Database**: Supabase PostgreSQL (NANO tier, West US Oregon). New projects use IPv6-only direct connection — use Session Pooler (`aws-0-us-west-2.pooler.supabase.com:5432`) for IPv4 compatibility.
- **Backups**: GitHub Actions (`.github/workflows/backup.yml`), runs daily at 12:00 UTC. Exports all tables to `backups/backup_YYYY-MM-DD.json`, keeps last 7 days. Script: `scripts/backup_db.py`.

## Workflow for bugs and features

### Bug workflow

1. **Identify** — reproduce or confirm the symptom (dashboard, DB query, or scraper log).
2. **Trace** — read the relevant files before touching anything. For data bugs: start from `loader.py` (how data is read) → `bank_scraper.py` (how data is written). For display bugs: start from the dashboard page → `loader.py`.
3. **Check the DB** — if the bug could have already corrupted data, query Supabase directly to assess the damage before fixing code.
4. **Fix** — minimal change. Don't refactor surrounding code.
5. **Clean DB if needed** — write a safe SELECT first to verify scope, then DELETE/UPDATE. Always show the SELECT result before running destructive SQL.
6. **Commit to `gastos-falabella`** — `git pull` first if behind origin, then commit and push.
7. **Mirror to `falabella-tracker`** — clone to `/tmp/falabella-tracker`, apply the same change, commit and push. This is the public repo that others fork.
8. **Document in `CHANGELOG.md`** — add an entry in both repos with: symptom, root cause, fix summary, and DB cleanup SQL if applicable.

### Feature workflow

1. **Understand before building** — read the files that will be touched. Check if the DB schema needs changes (migrations go in `analytics/migrations/`).
2. **DB migrations** — add a new `.sql` file in `analytics/migrations/` and apply it in the Supabase SQL Editor (DDL like `CREATE INDEX` and `ALTER TABLE` hits statement timeout through the session pooler — must run from the SQL Editor directly). Update `analytics/schema.sql` to reflect the final state.
3. **Code** — implement in the relevant layer (scraper, loader, repository, dashboard page).
4. **Test locally** — run `pipenv run python main.py --mode scraper --limit 1` to test scraper changes. Run `pipenv run streamlit run dashboard/visualizer.py` for dashboard changes.
5. **Commit, mirror, document** — same as steps 6–8 of the bug workflow.

### Two-repo rule

Every fix or feature that touches `scraper/`, `analytics/`, or `dashboard/` must be mirrored to `frcalder/falabella-tracker` (the public template). Backups, secrets, and personal data never go there. The `CHANGELOG.md` in `falabella-tracker` should include DB cleanup instructions since other users may have the same data issue.
