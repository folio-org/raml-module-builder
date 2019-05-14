INSERT INTO ${myuniversity}_${mymodule}.${table.tableName}
  SELECT id, jsonb_build_object('id', id, 'loanRulesAsTextFile', '')
  FROM (SELECT MD5(current_timestamp + "${myuniversity}_${mymodule}.${table.tableName}" ) AS id) AS alias;
