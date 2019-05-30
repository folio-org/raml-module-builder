DROP TABLE IF EXISTS tableb;
DROP TABLE IF EXISTS tablea;
CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;
CREATE TABLE tablea (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE TABLE tableb (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE OR REPLACE FUNCTION f_unaccent(text)
  RETURNS text AS
$func$
SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
$func$  LANGUAGE sql IMMUTABLE;
