INSERT INTO ${myuniversity}_${mymodule}.${table.tableName}
  SELECT id, jsonb_build_object('id', id, 'loanRulesAsTextFile', '')
  FROM (SELECT to_char(current_timestamp,'YYYY-MM-DD"T"HH24:MI:SS.MS') AS id) AS alias;
