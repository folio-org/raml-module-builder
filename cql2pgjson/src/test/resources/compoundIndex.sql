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

create or replace function concat_array_object_values(jsonb_data jsonb, field text) RETURNS text AS $$
  SELECT string_agg(value->>$2, ' ') FROM jsonb_array_elements($1);
$$ LANGUAGE sql IMMUTABLE;

create or replace function concat_array_object(jsonb_data jsonb) RETURNS text AS $$
  SELECT string_agg(value::text, ' ') FROM jsonb_array_elements_text($1);
$$ LANGUAGE sql IMMUTABLE;
    
CREATE OR REPLACE FUNCTION update_id() RETURNS TRIGGER AS $$
BEGIN
  NEW.id       = NEW.jsonb->>'id';
  RETURN NEW;
END; $$ language 'plpgsql';
CREATE TRIGGER update_tablea_references BEFORE INSERT OR UPDATE ON tablea
  FOR EACH ROW EXECUTE PROCEDURE update_id();

CREATE TRIGGER update_tableb_references BEFORE INSERT OR UPDATE ON tableb
  FOR EACH ROW EXECUTE PROCEDURE update_id();

CREATE TRIGGER update_tablec_references BEFORE INSERT OR UPDATE ON tablec
  FOR EACH ROW EXECUTE PROCEDURE update_id();

CREATE TRIGGER update_tabled_references BEFORE INSERT OR UPDATE ON tabled
  FOR EACH ROW EXECUTE PROCEDURE update_id();
INSERT INTO tablea (jsonb) VALUES
('{"id": "A0000000-0000-0000-0000-000000000000", "firstName": "Mike", "lastName": "Smith","field1": "first0", "field2": "last0", "field3" : { "info" :[{"city":"Boston","state": "MA"},{"city":"Philadelphia","state": "PA"}]}}'),
('{"id": "A1111111-1111-1111-1111-111111111111", "firstName": "Tom", "lastName": "Jones","field1": "first1", "field2": "last1","field3" :{ "info" :[{"city":"Tampa","state": "FL"},{"city":"Boston","state": "MA"}]}}'),
('{"id": "A2222222-2222-2222-2222-222222222222", "firstName": "Lucy", "lastName": "Williams","field1": "first2", "field2": "last2","field3" :{ "info" :[{"city":"Tampa","state": "FL"},{"city":"Philadelphia","state": "PA"}]}}'),
('{"id": "A3333333-3333-3333-3333-333333333333", "firstName": "Anne", "lastName": "Davis","field1": "first3", "field2": "last3", "field3" :{ "info" :[{"city":"Glendale","state": "AZ"},{"city":"Raleigh","state": "NC"}]}}'),
('{"id": "A4444444-4444-4444-4444-444444444444", "firstName": "Mary", "lastName": "Miller","field1": "first4", "field2": "last4" ,"field3" :{ "info" :[{"city":"Pittsburgh","state": "PA"},{"city":"Los Angeles","state": "CA"}]}}');

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
