DROP TABLE IF EXISTS instances;
CREATE TABLE instances (id text PRIMARY KEY, jsonb JSONB NOT NULL);
INSERT INTO instances (id, jsonb) VALUES
  ('a', '{"name":"a","text":"text without www"}'),
  ('b', '{"name":"b","text":"text with xxx-yyy"}'),
  ('c', '{"name":"c","text":"text with xxx-"}'),
  ('d', '{"name":"d","text":"text with -yyy"}'),
  ('e', '{"name":"e","text":"text with zzz?"}'),
  ('f', '{"name":"f","text":"text with zzz?*‘’‛′＇ŉ&"}');

