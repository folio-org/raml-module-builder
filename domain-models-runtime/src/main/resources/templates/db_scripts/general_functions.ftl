-- estimate count for large result sets
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.count_estimate_smart(query text) RETURNS integer AS $$
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

-- if the amount of results matches the LIMIT , it means that the result set is larger then the limit and we should return an estimate
-- for example, this is used in faceting with a limit of lets say 20,000 results. if we got back 20,000 results back (passed in as the rows
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

-- function used to convert accented strings into unaccented string
CREATE OR REPLACE FUNCTION f_unaccent(text)
  RETURNS text AS
$func$
SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
$func$  LANGUAGE sql IMMUTABLE;
  
  