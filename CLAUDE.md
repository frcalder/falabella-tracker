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
- `JS_EXTRACT_FIELDS` — finds the detail modal by searching for anchor label pairs (`Rubro`+`Hora`, etc.) then climbs the DOM to a container that has both. Extracts label→value pairs for `rubro`, `comercio`, `codigo_autorizacion`, `fecha_compra`, `hora`, `pais`, `origen`. Uses `wait_for_function` to wait for `Código autorización` to appear before extracting (headless timing fix).
- `JS_CLOSE_MODAL` — traverses shadow roots looking for a × button, falls back to backdrop click.
- `JS_NEXT_PAGE_RECT` — finds all `btn-pagination` class buttons (SVG only, no text), returns bounding rect of the last one (next›). Returns `null` if that button is disabled (last page).

**Two table sections**: The movements page has "Pendientes de confirmación" (no date, clock icon) followed by confirmed movements (with date). `_read_row` detects pending via presence of the clock icon element.

**Checkpoint / resume**: `existing_keys` (loaded from DB at init) tracks processed confirmed movements. Key format: `(fecha, descripcion, abs(monto_int), num_cuotas)` — derived from table columns, available before opening the modal. `num_cuotas` differentiates installments of the same purchase (e.g. cuota 01/03 vs 02/03). The skip check only applies to confirmed movements (`fecha` truthy) — pending are always re-processed. Monto is normalized to int for comparison between raw cell text and DB Decimal.

`incomplete_keys` holds confirmed rows missing `codigo_autorizacion`, `rubro`, or `comercio`. If a movement's key is in `incomplete_keys`, it is re-processed even if already in `existing_keys`, allowing a later scraper run to fill in modal data that failed previously.

**Pending movements strategy**: At the start of each run (`_reset_pending()`), all `pendiente=TRUE` rows are deleted from DB. They get re-inserted fresh during the run. This handles deduplication, monto changes, pending→confirmed transitions, and disappeared transactions. Pending movements have `codigo_autorizacion` from the bank, so classifications made on pending rows persist automatically when the movement is confirmed — no re-classification needed.

**tx_hash**:
- Pendientes: `NULL`
- Confirmadas con `codigo_autorizacion`: `NULL` — auth code is the real identifier; hash is redundant
- Confirmadas sin `codigo_autorizacion`: `sha256(fecha_compra|descripcion|monto)[:16]` — only fallback identifier

**Installment uniqueness**: The UNIQUE constraint on `movimientos` is `(codigo_autorizacion, num_cuotas)` — not just `codigo_autorizacion`. This allows one row per installment (cuota 01/03, 02/03, 03/03) of the same purchase. The upsert uses `ON CONFLICT (codigo_autorizacion, num_cuotas)`. Migration: `analytics/migrations/002_cuotas_unique.sql`.

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
| `fecha` | Display date (NULL for pending) |
| `descripcion` | Transaction description |
| `persona` | TITULAR / additional cardholder |
| `monto` | Amount |
| `monto_periodo` | Installment amount for the period (valor_cuota or monto) |
| `pendiente` | TRUE if unconfirmed |
| `rubro` | Category from bank modal |
| `comercio` | Merchant name from modal |
| `codigo_autorizacion` | Auth code — part of composite unique key `(codigo_autorizacion, num_cuotas)` |
| `fecha_compra` | Purchase date from modal (may differ from `fecha`) |
| `hora` | Purchase time from modal |
| `pais` | Country |
| `origen` | Purchase origin type |
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

- **Scraper**: GitHub Actions (`.github/workflows/scraper.yml`), triggerable manually or on a daily schedule. Configure the schedule in the workflow file.
- **Dashboard**: Streamlit Cloud, auto-deploys on push to `main`.
- **Database**: Supabase PostgreSQL. New Supabase projects may use IPv6-only direct connections — if you have connectivity issues, use the Session Pooler connection string from your project's database settings.
- **Backups**: GitHub Actions (`.github/workflows/backup.yml`), runs daily. Exports all tables to `backups/backup_YYYY-MM-DD.json`, keeps last 7 days. Script: `scripts/backup_db.py`.
