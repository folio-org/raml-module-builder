-- This is run for any module, a removal of the schema and user for a specific tenant
-- There is no per module configuration here so a delete of tenant will always run the below

DO $$
DECLARE
  tablename text;
BEGIN
  DROP SCHEMA IF EXISTS ${myuniversity}_${mymodule} CASCADE;
  DROP ROLE IF EXISTS ${myuniversity}_${mymodule};
EXCEPTION
  WHEN insufficient_privilege THEN
    LOOP
      SELECT table_name INTO tablename
        FROM information_schema.tables
        WHERE table_type='BASE TABLE' AND table_schema='${myuniversity}_${mymodule}';
      EXIT WHEN NOT FOUND;
      EXECUTE format('DROP TABLE ${myuniversity}_${mymodule}.%I CASCADE', tablename);
    END LOOP;
END $$;
