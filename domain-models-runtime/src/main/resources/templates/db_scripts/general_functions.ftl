-- if the amount of results matches the LIMIT , it means that the result set is larger than the limit and we should return an estimate
-- for example, this is used in faceting with a limit of lets say 20,000 results. if we got back 20,000 results (passed in as the rows
-- parameters , then our result set is larger than the limit most probably and we should estimate a count
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.count_estimate_smart2(rows bigint, lim bigint, query text) RETURNS bigint AS $$
DECLARE
  rec   record;
  cnt bigint;
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

CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.count_estimate_default(query text) RETURNS bigint AS $$
DECLARE
  rows bigint;
  q text;
BEGIN
  q = 'SELECT COUNT(*) FROM (' || query || ' LIMIT ${exactCount}) x';
  EXECUTE q INTO rows;
  IF rows < ${exactCount} THEN
    return rows;
  END IF;
  rows = ${myuniversity}_${mymodule}.count_estimate_smart2(${exactCount}, ${exactCount}, query);
  IF rows < ${exactCount} THEN
    return ${exactCount};
  END IF;
  RETURN rows;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

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

create or replace function concat_array_object_values(jsonb_data jsonb, field text) RETURNS text AS $$
  SELECT string_agg(value->>$2, ' ') FROM jsonb_array_elements($1);
$$ LANGUAGE sql IMMUTABLE;

create or replace function concat_array_object(jsonb_data jsonb) RETURNS text AS $$
  SELECT string_agg(value::text, ' ') FROM jsonb_array_elements_text($1);
$$ LANGUAGE sql IMMUTABLE;
