DROP FUNCTION IF EXISTS rmb_internal_index(aname text, tops text, newdef text);

-- This function recreates index if its definition has changed,
-- drops it and replaces it by the replacement if a prepared replacement index exists,
-- drops it if tops = 'DELETE'.
-- Updates its entry in table rmb_internal_index.
-- Adds an entry in table rmb_internal_analyze if ALTER INDEX or CREATE INDEX was executed.
CREATE OR REPLACE FUNCTION rmb_internal_index(
  atable text, aname text, tops text, newdef text) RETURNS void AS
$$
DECLARE
  olddef text;
  namep CONSTANT text = concat(aname, '_p');
  prepareddef text;
BEGIN
  IF tops = 'DELETE' THEN
    -- use case insensitive %s, not case sensitive %I
    -- no SQL injection because the names are hard-coded in schema.json
    EXECUTE format('DROP INDEX IF EXISTS %s', aname);
    EXECUTE 'DELETE FROM ${myuniversity}_${mymodule}.rmb_internal_index WHERE name = $1' USING aname;
    RETURN;
  END IF;
  SELECT def INTO olddef      FROM ${myuniversity}_${mymodule}.rmb_internal_index WHERE name = aname;
  SELECT def INTO prepareddef FROM ${myuniversity}_${mymodule}.rmb_internal_index WHERE name = namep;
  prepareddef = replace(prepareddef, concat(' ', namep, ' ON '), concat(' ', aname, ' ON '));
  IF prepareddef = newdef THEN
    EXECUTE format('DROP INDEX IF EXISTS %s', aname);
    EXECUTE format('ALTER INDEX IF EXISTS %s RENAME TO %s', namep, aname);
    EXECUTE 'DELETE FROM rmb_internal_index WHERE name = $1' USING namep;
    EXECUTE 'INSERT INTO rmb_internal_analyze VALUES ($1)' USING atable;
  ELSIF olddef IS DISTINCT FROM newdef THEN
    EXECUTE format('DROP INDEX IF EXISTS %s', aname);
    EXECUTE newdef;
    EXECUTE 'INSERT INTO rmb_internal_analyze VALUES ($1)' USING atable;
  END IF;
  EXECUTE 'INSERT INTO ${myuniversity}_${mymodule}.rmb_internal_index VALUES ($1, $2, FALSE) '
          'ON CONFLICT (name) DO UPDATE SET def = EXCLUDED.def, remove = EXCLUDED.remove' USING aname, newdef;
END
$$ LANGUAGE plpgsql;
