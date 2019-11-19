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
BEGIN
  rows = tenants_raml_module_builder.count_estimate_smart2(2000, 2000, query);
  IF rows > 2000 THEN
    RETURN rows;
  END IF;
  EXECUTE cntquery INTO rows;
  RETURN rows;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
