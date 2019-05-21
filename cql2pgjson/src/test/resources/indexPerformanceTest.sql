CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;
CREATE OR REPLACE FUNCTION f_unaccent(text)
  RETURNS text AS
$func$
SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
$func$  LANGUAGE sql IMMUTABLE;

DROP TABLE IF EXISTS config_data;
CREATE TABLE config_data (
   id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
   jsonb jsonb NOT NULL);
INSERT INTO config_data
  SELECT id, jsonb_build_object('id',id, 'module','circulation', 'configName','loans',
    'code',id2, 'num',(id2%100000)/100000.0, 'description',description, 'value',value, 'default',true,
    'enabled',true)
  FROM (  select gen_random_uuid() AS id,
          generate_series(1, 200004) AS id2,
          (md5(random()::text) || ' 123445 4ewfsdfqw' || now()) AS description,
          (md5(random()::text) || now()) AS value
       ) AS alias;
UPDATE config_data SET jsonb=jsonb_set(jsonb, '{value}', '"a1b2c3d4e5f6 xxxx"') WHERE jsonb->'code'='100000';
UPDATE config_data SET jsonb=jsonb_set(jsonb, '{value}', '"036000291452"')      WHERE jsonb->'code'='100001';
UPDATE config_data SET jsonb=jsonb_set(jsonb, '{value}', '"36000291452"')       WHERE jsonb->'code'='100002';
ANALYSE config_data;

