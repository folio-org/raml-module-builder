CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.count_estimate_smart(query text) RETURNS integer AS $$
DECLARE
  rec   record;
  rows  integer;
BEGIN
  FOR rec IN EXECUTE 'EXPLAIN ' || query LOOP
    rows := substring(rec."QUERY PLAN" FROM ' rows=([[:digit:]]+)');
    EXIT WHEN rows IS NOT NULL;
  END LOOP;
  IF rows < ${exactCount} THEN
    EXECUTE regexp_replace(
      query,
        '\mselect.*?from',
        'select count(*) FROM',
        'i'
     )
    INTO rec;
    rows := rec."count";
  END IF;
  RETURN rows;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT;
