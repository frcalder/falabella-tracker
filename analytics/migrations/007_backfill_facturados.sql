-- 007: backfill del período facturado
--
-- Dos columnas nuevas, ninguna tabla nueva (así que no hay que habilitar RLS):
--
-- 1. scraper_runs.backfill_periodo — marca de idempotencia. Guarda el 'YYYY-MM' del período
--    que esa corrida completó desde la pestaña "Movimientos facturados". Se escribe SOLO si el
--    pase leyó filas: el banco publica el detalle del estado de cuenta varios días después del
--    cierre, así que una corrida que lo encuentra vacío NO debe marcarlo — tiene que reintentar
--    al día siguiente.
--
-- 2. movimientos.backfill_run_id — procedencia. NULL = fila escrita por el pase normal. Permite
--    revertir un backfill con exactitud (DELETE ... WHERE backfill_run_id = N) sin depender del
--    backup diario, que además no respalda splits ni scraper_runs.
--
-- Ambos ALTER son metadata-only (columnas nullable, sin DEFAULT), así que no reescriben la tabla.
-- Necesitan ACCESS EXCLUSIVE de todos modos: si el statement queda colgado no es lentitud, es un
-- lock. Diagnosticar con la query de pg_stat_activity de CLAUDE.md antes de reintentar.

ALTER TABLE scraper_runs ADD COLUMN IF NOT EXISTS backfill_periodo TEXT;

ALTER TABLE movimientos  ADD COLUMN IF NOT EXISTS backfill_run_id  INTEGER;

-- Índice parcial: solo las filas back-filleadas, que son pocas. Sirve para el rollback y para
-- auditar qué escribió cada corrida.
CREATE INDEX IF NOT EXISTS idx_movimientos_backfill_run
    ON movimientos (backfill_run_id)
    WHERE backfill_run_id IS NOT NULL;
