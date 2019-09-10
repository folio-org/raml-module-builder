DROP TABLE IF EXISTS users cascade;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE users (id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(), user_data JSONB NOT NULL);
CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;
CREATE OR REPLACE FUNCTION f_unaccent(text)
  RETURNS text AS
$func$
SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
$func$  LANGUAGE sql IMMUTABLE;
