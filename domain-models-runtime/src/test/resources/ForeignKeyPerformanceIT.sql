CREATE TABLE foreignkeyperformanceit_uuid (id UUID PRIMARY KEY);
INSERT INTO  foreignkeyperformanceit_uuid VALUES
  ('00000000-0000-0000-0000-000000000000'),
  ('00000000-0000-0000-0000-000000000001'),
  ('00000000-0000-0000-0000-000000000002'),
  ('00000000-0000-0000-0000-000000000003');
CREATE TABLE foreignkeyperformanceit_withouttrigger (i int PRIMARY KEY, jsonb JSONB);
CREATE TABLE foreignkeyperformanceit_withtrigger    (i int PRIMARY KEY, jsonb JSONB,
                                                    id UUID NOT NULL REFERENCES foreignkeyperformanceit_uuid(id));
CREATE OR REPLACE FUNCTION foreignkeyperformanceit_trigger()
  RETURNS TRIGGER AS $$
  BEGIN
    NEW.id = NEW.jsonb->>'id';
    RETURN NEW;
  END;
  $$ language 'plpgsql';
CREATE TRIGGER foreignkeyperformanceit_trigger
  BEFORE INSERT OR UPDATE ON foreignkeyperformanceit_withtrigger
  FOR EACH ROW EXECUTE PROCEDURE foreignkeyperformanceit_trigger();
