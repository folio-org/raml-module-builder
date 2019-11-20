CREATE OR REPLACE FUNCTION tenants_raml_module_builder.count_estimate_smart2(rows bigint, lim bigint, query text) RETURNS integer AS $$
DECLARE
  rec   record;
  cnt integer;
BEGIN
  IF rows = lim THEN
      FOR rec IN EXECUTE 'EXPLAIN ' || query LOOP
        cnt := substring(rec."QUERY PLAN" FROM ' rows=([[:digit:]]+)');
        EXIT WHEN cnt IS NOT NULL;
      END LOOP;
      RETURN cnt;
  END IF;
  RETURN rows;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT;

CREATE OR REPLACE FUNCTION tenants_raml_module_builder.count_estimate_smart3(query text, cntquery text) RETURNS integer AS $$
DECLARE
  rows  integer;
  q text;
BEGIN
  q = 'SELECT COUNT(*) FROM (' || query || ' LIMIT 100) x';
  EXECUTE q INTO rows;
  IF rows < 100 THEN
    return rows;
  END IF;
  rows = tenants_raml_module_builder.count_estimate_smart2(100, 100, query);
  IF rows < 100 THEN
    return 101;
  END IF;
  RETURN rows;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
