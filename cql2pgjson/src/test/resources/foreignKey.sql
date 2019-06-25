DROP TABLE IF EXISTS tablea, tableb, tablec, tabled;
CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;
CREATE OR REPLACE FUNCTION f_unaccent(text) RETURNS text AS $$
  SELECT public.unaccent('public.unaccent', $1);
$$ LANGUAGE sql IMMUTABLE;
CREATE TABLE tabled (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE TABLE tablea (id UUID PRIMARY KEY, jsonb JSONB NOT NULL, tabledId UUID references tabled);
CREATE TABLE tableb (id UUID PRIMARY KEY, jsonb JSONB NOT NULL, tableaId UUID references tablea);
CREATE TABLE tablec (id UUID PRIMARY KEY, jsonb JSONB NOT NULL, tablebId UUID references tableb);


CREATE OR REPLACE FUNCTION update_tablea_references() RETURNS TRIGGER AS $$
BEGIN
  NEW.id       = NEW.jsonb->>'id';
  NEW.tabledId       = NEW.jsonb->>'tabledId';
  RETURN NEW;
END; $$ language 'plpgsql';
CREATE TRIGGER update_tablea_references BEFORE INSERT OR UPDATE ON tablea
  FOR EACH ROW EXECUTE PROCEDURE update_tablea_references();

CREATE OR REPLACE FUNCTION update_tableb_references() RETURNS TRIGGER AS $$
BEGIN
  NEW.id       = NEW.jsonb->>'id';
  NEW.tableaId = NEW.jsonb->>'tableaId';
  RETURN NEW;
END; $$ language 'plpgsql';
CREATE TRIGGER update_tableb_references BEFORE INSERT OR UPDATE ON tableb
  FOR EACH ROW EXECUTE PROCEDURE update_tableb_references();
  
CREATE INDEX tableb_prefix_idx ON tableb (lower(jsonb->>'prefix'));
CREATE INDEX tableb_otherindex_idx ON tableb (f_unaccent(jsonb->>'otherindex'));

CREATE OR REPLACE FUNCTION update_tablec_references() RETURNS TRIGGER AS $$
BEGIN
  NEW.id       = NEW.jsonb->>'id';
  NEW.tablebId = NEW.jsonb->>'tablebId';
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
INSERT INTO tabled (jsonb) VALUES
('{"id": "D1111111-1111-1111-1111-111111111111", "prefix": "a1", "otherindex": "z1"}'),
('{"id": "D2222222-2222-2222-2222-222222222222", "prefix": "a2", "otherindex": "z2"}'),
('{"id": "D3333333-3333-3333-3333-333333333333", "prefix": "a2", "otherindex": "z3"}');  

INSERT INTO tablea (jsonb) VALUES
('{"id": "A0000000-0000-0000-0000-000000000000", "name": "test0","tabledId": "D1111111-1111-1111-1111-111111111111"}'),
('{"id": "A1111111-1111-1111-1111-111111111111", "name": "test1","tabledId": "D1111111-1111-1111-1111-111111111111"}'),
('{"id": "A2222222-2222-2222-2222-222222222222", "name": "test2","tabledId": "D2222222-2222-2222-2222-222222222222"}'),
('{"id": "A3333333-3333-3333-3333-333333333333", "name": "test3","tabledId": "D3333333-3333-3333-3333-333333333333"}'),
('{"id": "A4444444-4444-4444-4444-444444444444", "name": "test4"}');

INSERT INTO tableb (jsonb) VALUES
('{"id": "B1111111-1111-1111-1111-111111111111", "prefix": "x1", "otherindex": "y1","tableaId": "A1111111-1111-1111-1111-111111111111"}'),
('{"id": "B2222222-2222-2222-2222-222222222222", "prefix": "x2", "otherindex": "y2", "tableaId": "A2222222-2222-2222-2222-222222222222"}'),
('{"id": "B3333333-3333-3333-3333-333333333333", "prefix": "x2", "otherindex": "y3","tableaId": "A2222222-2222-2222-2222-222222222222"}'),
('{"id": "B4444444-4444-4444-4444-444444444444", "prefix" : "x0'')));(((''DROP tableb","otherindex": "y4", "tableaId": "A3333333-3333-3333-3333-333333333333"}');



