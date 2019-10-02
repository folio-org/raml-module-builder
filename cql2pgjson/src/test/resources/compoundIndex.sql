DROP TABLE IF EXISTS tablea cascade;
DROP TABLE IF EXISTS tableb cascade;
DROP TABLE IF EXISTS tablec cascade;
DROP TABLE IF EXISTS tabled cascade;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE TABLE tablea (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE TABLE tableb (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE TABLE tablec (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE TABLE tabled (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);

CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;

CREATE OR REPLACE FUNCTION f_unaccent(text)
  RETURNS text AS
$func$
SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
$func$  LANGUAGE sql IMMUTABLE;
create or replace function concat_space_sql(VARIADIC text[])
RETURNS text AS $$ select concat_ws(' ', VARIADIC $1); $$ LANGUAGE SQL IMMUTABLE;

CREATE INDEX IF NOT EXISTS tablea_idx_gin ON tablea using gin
    ((concat_space_sql(tablea.jsonb->>'firstName',tablea.jsonb->>'lastName')) gin_trgm_ops);
    
CREATE INDEX IF NOT EXISTS tablea_idx_ft ON tablea using gin
    ( to_tsvector('simple',concat_space_sql(tablea.jsonb->>'field1',tablea.jsonb->>'field2')));
    
CREATE INDEX IF NOT EXISTS tableb_idx_gin ON tableb using gin
    ((concat_space_sql(tableb.jsonb->>'city',tableb.jsonb->>'state')) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS tablea_idx_ft ON tableb using gin
    ( to_tsvector('simple',concat_space_sql(tableb.jsonb->>'field1',tableb.jsonb->>'field2')));
CREATE INDEX IF NOT EXISTS tablec_idx_gin ON tablec using gin
    ((concat_space_sql(tablec.jsonb->>'firstName',tablec.jsonb->>'lastName')) gin_trgm_ops);
    
CREATE INDEX IF NOT EXISTS tablec_idx_ft ON tablec using gin
    ( to_tsvector('simple',concat_space_sql(tablec.jsonb->>'field1',tablec.jsonb->>'field2')));
    
CREATE INDEX IF NOT EXISTS tabled_idx_gin ON tabled using gin
    ((concat_space_sql(tabled.jsonb->>'city',tabled.jsonb->>'state')) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS tabled_idx_ft ON tabled using gin
    ( to_tsvector('simple',concat_space_sql(tabled.jsonb->>'field1',tabled.jsonb->>'field2')));
    
CREATE OR REPLACE FUNCTION update_tablea_references() RETURNS TRIGGER AS $$
BEGIN
  NEW.id       = NEW.jsonb->>'id';
  RETURN NEW;
END; $$ language 'plpgsql';
CREATE TRIGGER update_tablea_references BEFORE INSERT OR UPDATE ON tablea
  FOR EACH ROW EXECUTE PROCEDURE update_tablea_references();

CREATE OR REPLACE FUNCTION update_tableb_references() RETURNS TRIGGER AS $$
BEGIN
  NEW.id       = NEW.jsonb->>'id';
  RETURN NEW;
END; $$ language 'plpgsql';
CREATE TRIGGER update_tableb_references BEFORE INSERT OR UPDATE ON tableb
  FOR EACH ROW EXECUTE PROCEDURE update_tableb_references();   
 CREATE OR REPLACE FUNCTION update_tablec_references() RETURNS TRIGGER AS $$
BEGIN
  NEW.id       = NEW.jsonb->>'id';
  RETURN NEW;
END; $$ language 'plpgsql';
CREATE TRIGGER update_tablec_references BEFORE INSERT OR UPDATE ON tablec
  FOR EACH ROW EXECUTE PROCEDURE update_tablec_references();

CREATE OR REPLACE FUNCTION update_tabled_references() RETURNS TRIGGER AS $$
BEGIN
  NEW.id       = NEW.jsonb->>'id';
  RETURN NEW;
END; $$ language 'plpgsql';
CREATE TRIGGER update_tabled_references BEFORE INSERT OR UPDATE ON tabled
  FOR EACH ROW EXECUTE PROCEDURE update_tabled_references();    
INSERT INTO tablea (jsonb) VALUES
('{"id": "A0000000-0000-0000-0000-000000000000", "firstName": "Mike", "lastName": "Smith","field1": "first0", "field2": "last0"}'),
('{"id": "A1111111-1111-1111-1111-111111111111", "firstName": "Tom", "lastName": "Jones","field1": "first1", "field2": "last1"}'),
('{"id": "A2222222-2222-2222-2222-222222222222", "firstName": "Lucy", "lastName": "Williams","field1": "first2", "field2": "last2"}'),
('{"id": "A3333333-3333-3333-3333-333333333333", "firstName": "Anne", "lastName": "Davis","field1": "first3", "field2": "last3"}'),
('{"id": "A4444444-4444-4444-4444-444444444444", "firstName": "Mary", "lastName": "Miller","field1": "first4", "field2": "last4"}');

INSERT INTO tableb (jsonb) VALUES
('{"id": "B1111111-1111-1111-1111-111111111111", "city": "Boston", "state": "MA","field1": "first0", "field2": "last0" }'),
('{"id": "B2222222-2222-2222-2222-222222222222", "city": "Chicago", "state": "IL","field1": "first1", "field2": "last1"}'),
('{"id": "B3333333-3333-3333-3333-333333333333", "city": "San Francisco", "state": "CA","field1": "first2", "field2": "last2"}'),
('{"id": "B4444444-4444-4444-4444-444444444444",  "city": "Austin", "state": "TX","field1": "first3", "field2": "last3"}');

INSERT INTO tablec (jsonb) VALUES
('{"id": "C1111111-1111-1111-1111-111111111111", "user": "12", "firstName": "Mike", "lastName": "Smith" }'),
('{"id": "C2222222-2222-2222-2222-222222222222", "user": "23", "firstName": "Tom", "lastName": "Jones"}'),
('{"id": "C3333333-3333-3333-3333-333333333333", "user": "34", "firstName": "Lucy", "lastName": "Williams"}'),
('{"id": "C4444444-4444-4444-4444-444444444444", "user": "45", "firstName": "Mary", "lastName": "Miller"}');

INSERT INTO tabled (jsonb) VALUES
('{"id": "D1111111-1111-1111-1111-111111111111","user": "Mike", "proxy": { "personal" :{ "city": "Boston", "state": "MA"} } }'),
('{"id": "D2222222-2222-2222-2222-222222222222","user": "Bob", "proxy": { "personal" : { "city": "Chicago", "state": "IL"} } } '),
('{"id": "D3333333-3333-3333-3333-333333333333","user": "Tim", "proxy": { "personal" : { "city": "San Francisco", "state": "CA"} } } '),
('{"id": "D4444444-4444-4444-4444-444444444444","user": "Charles", "proxy": { "personal" : { "city": "Austin", "state": "TX"} } } ');
