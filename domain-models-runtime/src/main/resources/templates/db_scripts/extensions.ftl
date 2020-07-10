-- pg_trgm may have incorrectly been created in pg_catalog schema.
-- Therefore we drop pg_trgm if it is not loaded in public schema
-- to remove it from any other schema.
-- PostgreSQL can load an extension into only one schema.
-- References: https://issues.folio.org/browse/MODORDSTOR-161
-- https://issues.folio.org/browse/RMB-671
DO $$
BEGIN
  BEGIN
    -- This only succeeds if show_trgm, a pg_trgm function,
    -- has been loaded to public schema.
    PERFORM public.show_trgm('a');
  EXCEPTION
    WHEN undefined_function THEN
      DROP EXTENSION IF EXISTS pg_trgm CASCADE;
  END;
END $$;

CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;
