DROP TABLE IF EXISTS users cascade;
DROP TABLE IF EXISTS groups cascade;
DROP VIEW IF EXISTS users_groups_view;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE users (id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(), user_data JSONB NOT NULL);
CREATE TABLE groups (id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(), group_data JSONB NOT NULL);
CREATE VIEW users_groups_view  AS select users.id as id, COALESCE(users.user_data::jsonb || groups.group_data::jsonb,users.user_data::jsonb,groups.group_data::jsonb ) as ho_jsonb  from users inner JOIN groups ON (users.user_data->>'groupId')::uuid = groups.id;
CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;
CREATE OR REPLACE FUNCTION f_unaccent(text)
  RETURNS text AS
$func$
SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
$func$  LANGUAGE sql IMMUTABLE;
