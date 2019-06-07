DROP TABLE IF EXISTS tablec;
DROP TABLE IF EXISTS tableb;
DROP TABLE IF EXISTS tablea;
CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;
CREATE TABLE tablea (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE TABLE tableb (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE TABLE tablec (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE OR REPLACE FUNCTION f_unaccent(text)
  RETURNS text AS
$func$
SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
$func$  LANGUAGE sql IMMUTABLE;

INSERT into tablea (id, jsonb) VALUES
('A0000000-0000-0000-0000-000000000000', jsonb_build_object('name', 'test0')),
('A1111111-1111-1111-1111-111111111111', jsonb_build_object('name', 'test1')),
('A2222222-2222-2222-2222-222222222222', jsonb_build_object('name', 'test2'));
UPDATE tablea SET jsonb = jsonb_set(jsonb, '{id}', to_jsonb(id));

INSERT INTO tableb (id, jsonb) VALUES
('B1111111-1111-1111-1111-111111111111', jsonb_build_object('tableb_data', 'x1', 'tableaId', 'A1111111-1111-1111-1111-111111111111')),
('B2222222-2222-2222-2222-222222222222', jsonb_build_object('tableb_data', 'x2', 'tableaId', 'A2222222-2222-2222-2222-222222222222')),
('B3333333-3333-3333-3333-333333333333', jsonb_build_object('tableb_data', 'x2', 'tableaId', 'A2222222-2222-2222-2222-222222222222'));
UPDATE tableb SET jsonb = jsonb_set(jsonb, '{id}', to_jsonb(id));


