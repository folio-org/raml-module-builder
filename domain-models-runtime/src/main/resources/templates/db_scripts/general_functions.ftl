-- if the amount of results matches the LIMIT , it means that the result set is larger than the limit and we should return an estimate
-- for example, this is used in faceting with a limit of lets say 20,000 results. if we got back 20,000 results (passed in as the rows
-- parameters , then our result set is larger than the limit most probably and we should estimate a count
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
  -- get a fast estimate
  rows = ${myuniversity}_${mymodule}.count_estimate_smart2(${exactCount} , ${exactCount} , query);
  -- if estimate is higher than what we allow for a fast count query (default 20,000) , then return the estimate
  IF rows > ${exactCount}  THEN
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
         || ' LIMIT ${exactCount}  ), '
         || ' count_on as ( SELECT count(*) as count from counter ) '
         || ' SELECT count FROM count_on';
    -- RAISE NOTICE 'query %', q;
    EXECUTE q INTO rec;
    rows := rec."count";
    -- RAISE NOTICE 'rows %', rows;
    -- if the query has exactCount results, it means we have more than that and the explain plan is off
    IF rows = ${exactCount}  THEN
    -- try the explain plan again in case it has corrected itself (unlikely - maybe just remove)
    rows = ${myuniversity}_${mymodule}.count_estimate_smart2(${exactCount}, ${exactCount}, query);
        -- needed because EXPLAIN may severely under estimate the count
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

-- This trigger function copies primary key id from NEW.id to NEW.jsonb->'id'.
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.set_id_in_jsonb()
RETURNS TRIGGER AS $$
BEGIN
  NEW.jsonb = jsonb_set(NEW.jsonb, '{id}', to_jsonb(NEW.id));
  RETURN NEW;
END;
$$ language 'plpgsql';

create or replace function concat_space_sql(VARIADIC text[])
RETURNS text AS $$ select concat_ws(' ', VARIADIC $1); 
$$ LANGUAGE SQL IMMUTABLE;

create or replace function concat_array_object_values(JSON text)
RETURNS text AS 
DECLARE
  result text
$func$
BEGIN
  FOR i IN SELECT * FROM json_array_elements(omgjson)
  LOOP
    RAISE NOTICE 'output from space %', i->>'type';
  END LOOP;
  RETURN RESULT;
END; 
$$ LANGUAGE SQL IMMUTABLE;
