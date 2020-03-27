-- if the amount of results matches the LIMIT , it means that the result set is larger than the limit and we should return an estimate
-- for example, this is used in faceting with a limit of lets say 20,000 results. if we got back 20,000 results (passed in as the rows
-- parameters , then our result set is larger than the limit most probably and we should estimate a count
DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.count_estimate_smart2(bigint,bigint,text);
CREATE FUNCTION ${myuniversity}_${mymodule}.count_estimate_smart2(rows bigint, lim bigint, query text) RETURNS bigint AS $$
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
$$
SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
$$  LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;

-- Convert a string into a tsquery. A star * before a space or at the end of the string
-- is converted into a tsquery right truncation operator.
--
-- Implementation note:
-- to_tsquery('simple', '''''') yields ERROR:  syntax error in tsquery: "''"
-- use to_tsquery('simple', '') instead
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.tsquery_and(text) RETURNS tsquery AS $$
  SELECT to_tsquery('simple', string_agg(CASE WHEN length(v) = 0 OR v = '*' THEN ''
                                              WHEN right(v, 1) = '*' THEN '''' || left(v, -1) || ''':*'
                                              ELSE '''' || v || '''' END,
                                         '&'))
  FROM (SELECT regexp_split_to_table(translate($1, '&''', ',,'), ' +')) AS x(v);
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.tsquery_or(text) RETURNS tsquery AS $$
  SELECT replace(${myuniversity}_${mymodule}.tsquery_and($1)::text, '&', '|')::tsquery;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.tsquery_phrase(text) RETURNS tsquery AS $$
  SELECT replace(${myuniversity}_${mymodule}.tsquery_and($1)::text, '&', '<->')::tsquery;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;

-- Normalize digits by removing spaces, tabs and hyphen-minuses from the first chunk.
-- Insert a space before the second chunk. The second chunk starts at the first character that is
-- neither digit, space, tab nor hyphen-minus. The first chunk may end with a star * for right
-- truncation.
-- Examples:
-- normalize_digits(' 0-1  2--3 4 ') = '01234'
-- normalize_digits(' 01 2- 3 -- 45 -a 7 -8 9') = '012345 a 7 -8 9'
-- normalize_digits(' 01 2- 3 -- 45* -a 7 -8 9') = '012345* a 7 -8 9'
-- normalize_digits('978 92 8011 565 9(Vol. 1011-1021)') = '9789280115659 (Vol. 1011-1021)'
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.normalize_digits(text) RETURNS text AS $$
  SELECT    translate((regexp_match($1, '^([0-9 \t-]*(?:\*[ \t]*)?)(.*)'))[1], E' \t-', '')
         || CASE WHEN (regexp_match($1, '^([0-9 \t-]*(?:\*[ \t]*)?)(.*)'))[1] = '' THEN ''
                 WHEN (regexp_match($1, '^([0-9 \t-]*(?:\*[ \t]*)?)(.*)'))[2] = '' THEN ''
                 ELSE ' '
            END
         || (regexp_match($1, '^([0-9 \t-]*(?:\*[ \t]*)?)(.*)'))[2];
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;

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
