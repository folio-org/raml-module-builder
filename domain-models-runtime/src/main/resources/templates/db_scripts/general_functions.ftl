-- if the amount of results matches the LIMIT , it means that the result set is larger then the limit and we should return an estimate
-- for example, this is used in faceting with a limit of lets say 20,000 results. if we got back 20,000 results (passed in as the rows
-- parameters , then our result set is larger then the limit most probably and we should estimate a count
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.count_estimate_smart2(rows bigint, lim bigint, query text) RETURNS integer AS $$
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
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.count_estimate_smart(query text) RETURNS integer AS $$
DECLARE
  rec   record;
  rows  integer;
  q   text;
BEGIN
  q = 'with counter as (' || 
      regexp_replace(
          query,
            '\mselect.*?from',
            'select null FROM',
            'i'
         )
         || ' LIMIT ${exactCount}  ), '
         || ' count_on as ( SELECT count(*) as count from counter ) '
         || ' SELECT count FROM count_on';
    -- RAISE NOTICE 'query %', q;
    EXECUTE q INTO rec;
    rows := rec."count";
    -- RAISE NOTICE 'rows %', rows;
    IF rows = ${exactCount}  THEN
    rows = cql4_mod_inventory_storage.count_estimate_smart2(${exactCount}, ${exactCount}, query);
        -- needed because EXPLAIN my severely under estimate the count
        IF rows < ${exactCount}  THEN
          rows = ${exactCount} ;
        END IF;
    END IF;
  RETURN rows;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;




-- DEPRICATED estimate count for large result sets
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.count_estimate_smart_depricated(query text) RETURNS integer AS $$
DECLARE
  rec   record;
  rows  integer;
BEGIN
  FOR rec IN EXECUTE 'EXPLAIN ' || query LOOP
    rows := substring(rec."QUERY PLAN" FROM ' rows=([[:digit:]]+)');
    EXIT WHEN rows IS NOT NULL;
  END LOOP;
  IF rows < ${exactCount} THEN
    EXECUTE regexp_replace(
      query,
        '\mselect.*?from',
        'select count(*) FROM',
        'i'
     )
    INTO rec;
    rows := rec."count";
  END IF;
  RETURN rows;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT;





-- function used to convert accented strings into unaccented string
CREATE OR REPLACE FUNCTION f_unaccent(text)
  RETURNS text AS
$func$
SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
$func$  LANGUAGE sql IMMUTABLE;
  
