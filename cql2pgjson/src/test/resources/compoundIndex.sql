DROP TABLE IF EXISTS tablea cascade;
DROP TABLE IF EXISTS tableb cascade;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE tablea (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE TABLE tableb (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE INDEX IF NOT EXISTS tablea_idx_gin ON tablea using gin
    ((concat_ws(tablea.jsonb->'firstname',tablea.jsonb->'lastname')) gin_trgm_ops);
    
CREATE INDEX IF NOT EXISTS tablea_idx_ft ON tablea using gin
    ( to_tsvector('simple',concat_ws(tablea.jsonb->'field1',tablea.jsonb->'field2')));
    
CREATE INDEX IF NOT EXISTS tableb_idx_gin ON tableb using gin
    ((concat_ws(tableb.jsonb->'city',tableb.jsonb->'state')) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS tablea_idx_ft ON tableb using gin
    ( to_tsvector('simple',concat_ws(tableb.jsonb->'field1',tableb.jsonb->'field2')));
CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;
CREATE OR REPLACE FUNCTION f_unaccent(text)
  RETURNS text AS
$func$
SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
$func$  LANGUAGE sql IMMUTABLE;


INSERT INTO tablea (id,jsonb) VALUES
(A0000000-0000-0000-0000-000000000000,'{"id": "A0000000-0000-0000-0000-000000000000", "firstname": "first0", "lastname": "last0","field1": "first0", "field2": "last0"}'),
(A1111111-1111-1111-1111-111111111111,'{"id": "A1111111-1111-1111-1111-111111111111", "firstname": "first1", "lastname": "last1","field1": "first0", "field2": "last0"}'),
(A2222222-2222-2222-2222-222222222222,'{"id": "A2222222-2222-2222-2222-222222222222", "firstname": "first2", "lastname": "last2","field1": "first0", "field2": "last0"}'),
(A3333333-3333-3333-3333-333333333333,'{"id": "A3333333-3333-3333-3333-333333333333", "firstname": "first3", "lastname": "last3","field1": "first0", "field2": "last0"}'),
(A4444444-4444-4444-4444-444444444444,'{"id": "A4444444-4444-4444-4444-444444444444", "firstname": "first4", "lastname": "last4","field1": "first0", "field2": "last0"}');
INSERT INTO tablea (jsonb) VALUES (jsonb_build_object('id', md5('a' || generate_series(1, 2000)::text)));

INSERT INTO tableb (jsonb) VALUES
(B1111111-1111-1111-1111-111111111111,'{"id": "B1111111-1111-1111-1111-111111111111", "city": "Boston", "state": "MA","field1": "first0", "field2": "last0" }'),
(B2222222-2222-2222-2222-222222222222,'{"id": "B2222222-2222-2222-2222-222222222222", "city": "chicago", "state": "IL","field1": "first0", "field2": "last0"}'),
(B3333333-3333-3333-3333-333333333333,'{"id": "B3333333-3333-3333-3333-333333333333", "city": "San Francisco", "state": "CA","field1": "first0", "field2": "last0"}'),
(B4444444-4444-4444-4444-444444444444,'{"id": "B4444444-4444-4444-4444-444444444444",  "city": "first0", "state": "last0","field1": "first0", "field2": "last0"}');
INSERT INTO tableb (jsonb) VALUES (jsonb_build_object('id', md5('b' || generate_series(1, 2000)::text)));