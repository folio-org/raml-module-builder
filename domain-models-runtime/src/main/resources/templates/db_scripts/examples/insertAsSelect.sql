INSERT INTO ${myuniversity}_${mymodule}.${table.tableName}
  SELECT id, jsonb_build_object('id', id, 'loanRulesAsTextFile', '')
  FROM (SELECT gen_random_uuid() AS id) AS alias;
