-- For FOLIO an PostgreSQL extension must reside in schema public
-- because within one database it can be loaded only once and
-- that one must be shared by all users from all schemas.
-- For background see https://issues.folio.org/browse/RMB-584
-- Extension pg_trgm may have been loaded into a wrong schema,
-- we fix this by moving it into schema public; PostgreSQL
-- automatically changes all extension usages to use the new
-- extension location.
-- https://issues.folio.org/browse/MODORDSTOR-161
-- https://issues.folio.org/browse/RMB-671
DO $$
BEGIN
  -- use advisory lock, otherwise concurrent installs running
  -- 'CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public'
  -- in parallel may fail with
  -- 'ERROR: duplicate key value violates unique constraint "pg_extension_name_index" (23505)'
  -- https://github.com/greenplum-db/gpdb/issues/4601
  PERFORM pg_advisory_xact_lock(20201101, 1234567890);

  BEGIN
    -- This only succeeds if show_trgm, a pg_trgm function,
    -- has been loaded into public schema.
    PERFORM public.show_trgm('a');
  EXCEPTION
    WHEN undefined_function THEN
      BEGIN
        ALTER EXTENSION pg_trgm SET SCHEMA public;
      EXCEPTION
        WHEN undefined_object THEN NULL;
      END;
  END;

  CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;
  CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;
END $$;

