-- Activa Row Level Security en todas las tablas del schema public, sin políticas.
--
-- Motivo: Supabase expone un endpoint REST (PostgREST) sobre el schema public. Por
-- defecto los roles `anon` y `authenticated` tienen TODOS los privilegios sobre las
-- tablas (SELECT, INSERT, UPDATE, DELETE, TRUNCATE), así que cualquiera con la anon
-- key del proyecto podría leer, modificar o borrar todos los datos. Es lo que reporta
-- el linter de Supabase como `rls_disabled_in_public` / "Table publicly accessible".
--
-- Con RLS activado y CERO políticas, esos roles quedan sin acceso a ninguna fila:
-- RLS deniega por defecto y sin políticas no hay nada que permita el acceso.
--
-- El scraper y el dashboard NO se ven afectados: conectan por DATABASE_URL con el rol
-- `postgres`, que tiene rolbypassrls = TRUE y pasa por encima de RLS. Verificar antes
-- de aplicar, por si tu proyecto usa otro rol:
--     SELECT current_user, rolbypassrls FROM pg_roles WHERE rolname = current_user;
--
-- Si el resultado es rolbypassrls = FALSE, NO apliques esto sin crear antes una
-- política que permita el acceso a ese rol, o dejarás sin datos a la aplicación.
--
-- Aplicar desde el SQL Editor de Supabase: el DDL por el session pooler llega a
-- statement timeout.

ALTER TABLE public.movimientos       ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.categorias        ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.clasificaciones   ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.splits            ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.presupuestos      ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.reglas_sugerencia ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.scraper_runs      ENABLE ROW LEVEL SECURITY;

-- Verificación (todas deben quedar en rls_activo = true, politicas = 0):
--   SELECT c.relname, c.relrowsecurity AS rls_activo,
--          (SELECT COUNT(*) FROM pg_policies p
--            WHERE p.schemaname = 'public' AND p.tablename = c.relname) AS politicas
--     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
--    WHERE n.nspname = 'public' AND c.relkind = 'r'
--    ORDER BY c.relname;

-- Opcional (segunda capa, defense in depth): quitar los privilegios de los roles de la
-- API. RLS ya los bloquea; esto además los deja sin permisos a nivel de GRANT. Ten en
-- cuenta que los default privileges de Supabase vuelven a otorgar permisos sobre las
-- tablas que crees después, así que RLS sigue siendo la capa durable.
--   REVOKE ALL ON ALL TABLES    IN SCHEMA public FROM anon, authenticated;
--   REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM anon, authenticated;
