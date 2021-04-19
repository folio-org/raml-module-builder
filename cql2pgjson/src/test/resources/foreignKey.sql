DROP TABLE IF EXISTS tablea, tableb, tablec, tablef, tablej;
CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;
CREATE OR REPLACE FUNCTION f_unaccent(text) RETURNS text AS $$
  SELECT public.unaccent('public.unaccent', $1);
$$ LANGUAGE sql IMMUTABLE;
CREATE TABLE tablef (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE TABLE tablej (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE TABLE tablea (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
CREATE TABLE tableb (id UUID PRIMARY KEY, jsonb JSONB NOT NULL, tableaId UUID references tablea, tablefId UUID references tablef, tablejId UUID references tablej, copyrighttracking_copyrightstatusid UUID references tablej);
CREATE TABLE tablec (id UUID PRIMARY KEY, jsonb JSONB NOT NULL, tablebId UUID references tableb);

CREATE OR REPLACE FUNCTION update_id() RETURNS TRIGGER AS $$
BEGIN
  NEW.id       = NEW.jsonb->>'id';
  RETURN NEW;
END; $$ language 'plpgsql';
CREATE TRIGGER update_tablea_references BEFORE INSERT OR UPDATE ON tablea
  FOR EACH ROW EXECUTE PROCEDURE update_id();
CREATE TRIGGER update_tablef_references BEFORE INSERT OR UPDATE ON tablef
  FOR EACH ROW EXECUTE PROCEDURE update_id();
CREATE TRIGGER update_tablej_references BEFORE INSERT OR UPDATE ON tablej
  FOR EACH ROW EXECUTE PROCEDURE update_id();

CREATE OR REPLACE FUNCTION update_tableb_references() RETURNS TRIGGER AS $$
BEGIN
  NEW.id       = NEW.jsonb->>'id';
  NEW.tableaId = NEW.jsonb->>'tableaId';
  NEW.tablefId = NEW.jsonb->>'tablefId';
  NEW.copyrightTracking_copyrightStatusId = NEW.jsonb->>'copyrightTracking_copyrightStatusId';
  RETURN NEW;
END; $$ language 'plpgsql';
CREATE TRIGGER update_tableb_references BEFORE INSERT OR UPDATE ON tableb
  FOR EACH ROW EXECUTE PROCEDURE update_tableb_references();

CREATE INDEX tableb_prefix_idx ON tableb (lower(jsonb->>'prefix'));
CREATE INDEX tableb_otherindex_idx ON tableb (f_unaccent(jsonb->>'otherindex'));
CREATE INDEX tablec_cindex_idx ON tablec (f_unaccent(jsonb->>'cindex'));
CREATE OR REPLACE FUNCTION update_tablec_references() RETURNS TRIGGER AS $$
BEGIN
  NEW.id       = NEW.jsonb->>'id';
  NEW.tablebId = NEW.jsonb->>'tablebId';
  RETURN NEW;
END; $$ language 'plpgsql';
CREATE TRIGGER update_tablec_references BEFORE INSERT OR UPDATE ON tablec
  FOR EACH ROW EXECUTE PROCEDURE update_tablec_references();

INSERT INTO tablef (jsonb) VALUES
('{"id": "F1111111-1111-4000-8000-000000000000"}');


INSERT INTO tablej (jsonb) VALUES
('{"id": "A1111111-1111-1111-1111-111111111111", "name": "cc1"}'),
('{"id": "A2222222-2222-2222-2222-222222222222", "name": "cc2"}');

INSERT INTO tablea (jsonb) VALUES
('{"id": "A0000000-0000-0000-0000-000000000000", "name": "test0"}'),
('{"id": "A1111111-1111-1111-1111-111111111111", "name": "test1"}'),
('{"id": "A2222222-2222-2222-2222-222222222222", "name": "test2"}'),
('{"id": "A3333333-3333-3333-3333-333333333333", "name": "test3"}'),
('{"id": "A4444444-4444-4444-4444-444444444444", "name": "test4"}'),
('{"id": "A5555555-5555-4000-8000-000000000000", "name": "test5"}');
INSERT INTO tablea (jsonb) VALUES (jsonb_build_object('id', md5('a' || generate_series(1, 2000)::text)));

INSERT INTO tableb (jsonb) VALUES
('{"id": "B1111111-1111-1111-1111-111111111111", "prefix": "x1", "otherindex": "y1","tableaId": "A1111111-1111-1111-1111-111111111111", "copyrightTracking_copyrightStatusId" : "a1111111-1111-1111-1111-111111111111"}'),
('{"id": "B2222222-2222-2222-2222-222222222222", "prefix": "x2", "otherindex": "y2","tableaId": "A2222222-2222-2222-2222-222222222222", "copyrightTracking_copyrightStatusId" : "A2222222-2222-2222-2222-222222222222"}'),
('{"id": "B3333333-3333-3333-3333-333333333333", "prefix": "x2", "otherindex": "y3","tableaId": "A2222222-2222-2222-2222-222222222222", "copyrightTracking_copyrightStatusId" : "A2222222-2222-2222-2222-222222222222"}'),
('{"id": "B4444444-4444-4444-4444-444444444444", "prefix": "x0'')));(((''DROP tableb","otherindex": "y4", "tableaId": "A3333333-3333-3333-3333-333333333333", "copyrightTracking_copyrightStatusId" : "A2222222-2222-2222-2222-222222222222"}'),
('{"id": "B5555555-5555-4000-8000-000000000000", "prefix": "x5", "tableaId": "A3333333-3333-3333-3333-333333333333", "copyrightTracking_copyrightStatusId" : "A2222222-2222-2222-2222-222222222222"}'),
('{"id": "B6666666-6666-4000-8000-000000000000", "prefix": "x6", "tableaId": "A4444444-4444-4444-4444-444444444444", "tablefId": "F1111111-1111-4000-8000-000000000000", "copyrightTracking_copyrightStatusId" : "A2222222-2222-2222-2222-222222222222"}'),
('{"id": "B7777777-7777-4000-8000-000000000000", "prefix": "x7", "tablejId": "A1111111-1111-1111-1111-111111111111", "copyrightTracking_copyrightStatusId" : "A2222222-2222-2222-2222-222222222222"}');
INSERT INTO tableb (jsonb) VALUES (jsonb_build_object('id', md5('b' || generate_series(1, 2000)::text)));

INSERT INTO tablec (jsonb) VALUES
('{"id": "C1111111-1111-1111-1111-111111111111", "cprefix": "x1", "cindex": "z1", "tablebId": "B1111111-1111-1111-1111-111111111111"}'),
('{"id": "C2222222-2222-2222-2222-222222222222", "cprefix": "x2", "cindex": "z2", "tablebId": "B2222222-2222-2222-2222-222222222222"}'),
('{"id": "C3333333-3333-3333-3333-333333333333", "cprefix": "x2", "cindex": "z3", "tablebId": "B2222222-2222-2222-2222-222222222222"}'),
('{"id": "C4444444-4444-4444-4444-444444444444", "cprefix": "x4", "cindex": "z4", "tablebId": "B3333333-3333-3333-3333-333333333333"}');
INSERT INTO tablec (jsonb) VALUES (jsonb_build_object('id', md5('c' || generate_series(1, 2000)::text)));
