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


-- count_estimate(query) returns an estimate for the number of records that query returns.
-- It uses "EXPLAIN SELECT" to quickly get an estimation from Postgres (see count_estimate_smart2).
-- It uses "SELECT COUNT(*) FROM query LIMIT ${exactCount}" to get an exact count when the
-- exact count is smaller than ${exactCount}, this query may take long if using full text query.
-- For details see https://github.com/folio-org/raml-module-builder#estimated-totalrecords
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.count_estimate(query text) RETURNS bigint AS $$
DECLARE
  count bigint;
  est_count bigint;
  q text;
BEGIN
  est_count = ${myuniversity}_${mymodule}.count_estimate_smart2(${exactCount}, ${exactCount}, query);
  IF est_count > 4*${exactCount} THEN
    RETURN est_count;
  END IF;
  q = 'SELECT COUNT(*) FROM (' || query || ' LIMIT ${exactCount}) x';
  EXECUTE q INTO count;
  IF count < ${exactCount} THEN
    RETURN count;
  END IF;
  IF est_count < ${exactCount} THEN
    RETURN ${exactCount};
  END IF;
  RETURN est_count;
END;
$$ LANGUAGE plpgsql STABLE STRICT;


-- upsert(table, id, value)
-- This properly works with optimistic locking triggers.
-- Using "INSERT INTO table ... ON CONFLICT (id) DO UPDATE" with optimistic locking
-- fails because the INSERT trigger overwrites the _version property that the
-- UPDATE trigger uses to detect an optimistic locking conflict.
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.upsert(text, uuid, anyelement) RETURNS uuid AS $$
DECLARE
  ret uuid;
BEGIN
  EXECUTE format('UPDATE ${myuniversity}_${mymodule}.%I SET jsonb=$3 WHERE id=$2 RETURNING id', $1)
          USING $1, $2, $3 INTO ret;
  IF ret IS NOT NULL THEN
    RETURN ret;
  END IF;
  EXECUTE format('INSERT INTO ${myuniversity}_${mymodule}.%I (id, jsonb) VALUES ($2, $3) RETURNING id', $1)
          USING $1, $2, $3 INTO STRICT ret;
  RETURN ret;
END;
$$ LANGUAGE plpgsql;


-- f_unaccent(text)
--
-- Convert accented string into unaccented string.
-- For Postgres versions below 12 we additionally need to remove
-- these Unicode combining characters:
-- 0x0300 ... 0x0362: Accents, IPA
-- 0x20dd ... 0x20e0: Symbols
-- 0x20e2 ... 0x20e4: Screen, keycap, triangle
-- For Postgres >= 12 unaccent removes them:
-- https://git.postgresql.org/gitweb/?p=postgresql.git;a=commitdiff;h=456e3718e7b72efe4d2639437fcbca2e4ad83099;hp=80579f9bb171350fccdd5f1d793c538254d9de62
-- https://issues.folio.org/browse/RMB-605
DO $$
  DECLARE ver integer;
  BEGIN
    SELECT current_setting('server_version_num') INTO ver;
    IF (ver >= 120000) THEN
      CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.f_unaccent(text)
        RETURNS text AS
      $f_unaccent$
        SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
      $f_unaccent$  LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;
    ELSE
      CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.f_unaccent(text)
        RETURNS text AS
      $f_unaccent$
        SELECT regexp_replace(public.unaccent('public.unaccent', $1),
          E'[\u0300\u0301\u0302\u0303\u0304\u0305\u0306\u0307\u0308\u0309\u030a\u030b\u030c\u030d\u030e\u030f' ||
           E'\u0310\u0311\u0312\u0313\u0314\u0315\u0316\u0317\u0318\u0319\u031a\u031b\u031c\u031d\u031e\u031f' ||
           E'\u0320\u0321\u0322\u0323\u0324\u0325\u0326\u0327\u0328\u0329\u032a\u032b\u032c\u032d\u032e\u032f' ||
           E'\u0330\u0331\u0332\u0333\u0334\u0335\u0336\u0337\u0338\u0339\u033a\u033b\u033c\u033d\u033e\u033f' ||
           E'\u0340\u0341\u0342\u0343\u0344\u0345\u0346\u0347\u0348\u0349\u034a\u034b\u034c\u034d\u034e\u034f' ||
           E'\u0350\u0351\u0352\u0353\u0354\u0355\u0356\u0357\u0358\u0359\u035a\u035b\u035c\u035d\u035e\u035f' ||
           E'\u0360\u0361\u0362' ||
           E'\u20dd\u20de\u20df\u20e0' ||
           E'\u20e2\u20e3\u20e4]',
          '',
          'g')
      $f_unaccent$  LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;
    END IF;
  END
$$;

-- Replace & by , because we use & as the AND operator when the query contains multiple words.
-- PostgreSQL removes punctuation but not in URLs:
-- https://www.postgresql.org/docs/current/textsearch-parsers.html
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.get_tsvector(text) RETURNS tsvector AS $$
  SELECT to_tsvector('simple', translate($1, '&', ','));
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;

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

-- Concatenate the parameters using space as separator
create or replace function ${myuniversity}_${mymodule}.concat_space_sql(VARIADIC text[])
RETURNS text AS $$ select concat_ws(' ', VARIADIC $1);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

-- For each element of the jsonb_array take the value of field; concatenate them using space as separator
create or replace function ${myuniversity}_${mymodule}.concat_array_object_values(jsonb_array jsonb, field text) RETURNS text AS $$
  SELECT string_agg(value->>$2, ' ') FROM jsonb_array_elements($1);
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;

-- Take each value of the field attribute of the jsonb_array elements where the filterkey of the element has filtervalue;
-- concate the values using space as separator.
create or replace function ${myuniversity}_${mymodule}.concat_array_object_values(
  jsonb_array jsonb, field text, filterkey text, filtervalue text) RETURNS text AS $$
SELECT string_agg(value->>$2, ' ') FROM jsonb_array_elements($1) WHERE value->>$3 = $4;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;

-- Return the value of the field attribute of the first jsonb_array element where filterkey has filtervalue.
create or replace function ${myuniversity}_${mymodule}.first_array_object_value(
  jsonb_array jsonb, field text, filterkey text, filtervalue text) RETURNS text AS $$
SELECT value->>$2 FROM jsonb_array_elements($1) WHERE value->>$3 = $4 LIMIT 1;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;

-- Concatenate the elements of the jsonb_array using space as separator
create or replace function ${myuniversity}_${mymodule}.concat_array_object(jsonb_array jsonb) RETURNS text AS $$
  SELECT string_agg(value::text, ' ') FROM jsonb_array_elements_text($1);
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;
