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





-- this is a variant of count_estimate_smart2 , but the limit is read in as a placeholder, and the count is calculated
-- by rewriting the query - calls count_estimate_smart2 since it is ciritical for this function to be IMMUTABLE
-- and an EXPLAIN within an IMMUTABLE function is not allowed
CREATE OR REPLACE FUNCTION tenants_raml_module_builder.count_estimate_smart(query text) RETURNS integer AS $$
DECLARE
  rec   record;
  rows  integer;
  q   text;
BEGIN
  -- get a fast estimate
  rows = tenants_raml_module_builder.count_estimate_smart2(2000 , 2000 , query);
  -- if estimate is higher than what we allow for a fast count query (default 20,000) , then return the estimate
  IF rows > 2000  THEN
    RETURN rows;
  END IF;
  -- otherwise send the query with a limit of exactCount
  q = 'with counter as (' ||
      regexp_replace(
          query,
            '\mselect.*?from',
            'select null FROM',
            'i'
         )
         || ' LIMIT 2000  ), '
         || ' count_on as ( SELECT count(*) as count from counter ) '
         || ' SELECT count FROM count_on';
    -- RAISE NOTICE 'query %', q;
    EXECUTE q INTO rec;
    rows := rec."count";
    -- RAISE NOTICE 'rows %', rows;
    -- if the query has exactCount results, it means we have more than that and the explain plan is off
    IF rows = 2000  THEN
    -- try the explain plan again in case it has corrected itself (unlikely - maybe just remove)
    rows = tenants_raml_module_builder.count_estimate_smart2(2000, 2000, query);
        -- needed because EXPLAIN may severely under estimate the count
        IF rows < 2000 THEN
          rows = 2000;
        END IF;
    END IF;
  RETURN rows;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;