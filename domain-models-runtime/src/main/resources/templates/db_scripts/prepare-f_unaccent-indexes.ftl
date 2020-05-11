-- Prepare indexes for the non-public f_unaccent(text)

-- f_unaccent(text)
--
-- Convert accented string into unaccented string.
-- For Postgres versions below 12 we additionally need to remove
-- these Unicode combining characters:
-- 0x0300 ... 0x0362: Accents, IPA
-- 0x20dd ... 0x20e0: Symbols
-- 0x20e2 ... 0x20e4: Screen, keycap, triangle
-- For Postgres >= 12 unaccent removes them:
-- https://git.postgresql.org/gitweb/?p=postgresql.git;a=commitdiff;h=456e3718e7b72efe4d2639437fcbca2e4ad83099;hp=80579f9bb171350fccdd5f1d793c538254d9de62
-- https://issues.folio.org/browse/RMB-605
DO $$
  DECLARE ver integer;
  BEGIN
    SELECT current_setting('server_version_num') INTO ver;
    IF (ver >= 120000) THEN
      CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.f_unaccent(text)
        RETURNS text AS
      $f_unaccent$
        SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
      $f_unaccent$  LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;
    ELSE
      CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.f_unaccent(text)
        RETURNS text AS
      $f_unaccent$
        SELECT regexp_replace(public.unaccent('public.unaccent', $1),
          E'[\u0300\u0301\u0302\u0303\u0304\u0305\u0306\u0307\u0308\u0309\u030a\u030b\u030c\u030d\u030e\u030f' ||
           E'\u0310\u0311\u0312\u0313\u0314\u0315\u0316\u0317\u0318\u0319\u031a\u031b\u031c\u031d\u031e\u031f' ||
           E'\u0320\u0321\u0322\u0323\u0324\u0325\u0326\u0327\u0328\u0329\u032a\u032b\u032c\u032d\u032e\u032f' ||
           E'\u0330\u0331\u0332\u0333\u0334\u0335\u0336\u0337\u0338\u0339\u033a\u033b\u033c\u033d\u033e\u033f' ||
           E'\u0340\u0341\u0342\u0343\u0344\u0345\u0346\u0347\u0348\u0349\u034a\u034b\u034c\u034d\u034e\u034f' ||
           E'\u0350\u0351\u0352\u0353\u0354\u0355\u0356\u0357\u0358\u0359\u035a\u035b\u035c\u035d\u035e\u035f' ||
           E'\u0360\u0361\u0362' ||
           E'\u20dd\u20de\u20df\u20e0' ||
           E'\u20e2\u20e3\u20e4]',
          '',
          'g')
      $f_unaccent$  LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;
    END IF;
  END
$$;

-- Create an index where public.f_unaccent is replaced by ${myuniversity}_${mymodule}.f_unaccent
-- for each index declared in schema.json managed by RMB where the index expression contains public.f_unaccent.
-- The name of this prepared/prearranged index is the regular index name with _p appended.
-- On module upgrade to RMB >= 24.4.0 the rmb_internal_index function will replace the
-- regular index by this prepared index: https://github.com/folio-org/raml-module-builder/pull/638/files
DO $$
DECLARE
  i RECORD;
  p_def TEXT;
  p_name TEXT;
BEGIN
  FOR i IN SELECT * FROM ${myuniversity}_${mymodule}.rmb_internal_index WHERE remove = FALSE
  LOOP
    p_def := regexp_replace(i.def,
      -- \m = beginning of a word, \M = end of a word
      '\m(${myuniversity}_${mymodule}.)?f_unaccent\M',
      '${myuniversity}_${mymodule}.f_unaccent',
      'g');
    CONTINUE WHEN p_def = i.def;
    p_name = concat(i.name, '_p');
    UPDATE ${myuniversity}_${mymodule}.rmb_internal_index SET remove = FALSE WHERE name = p_name;
    CONTINUE WHEN found;
    -- replace only first occurrence of i.name
    p_def = regexp_replace(p_def, i.name, p_name);
    EXECUTE p_def;
    -- save with i.def because function rmb_internal_index will use it unchanged
    EXECUTE 'INSERT INTO ${myuniversity}_${mymodule}.rmb_internal_index VALUES ($1, $2, FALSE) '
            'ON CONFLICT (name) DO UPDATE SET def = EXCLUDED.def, remove = EXCLUDED.remove'
            USING p_name, i.def;
  END LOOP;
END $$;
