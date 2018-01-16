<#if table.populateJsonWithId == true>
  CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.set_id_injson_${table.tableName}()
  RETURNS TRIGGER AS $$
  DECLARE
    injectedId text;
  BEGIN
    injectedId = '"'||NEW.${table.pkColumnName}||'"';
    NEW.jsonb = jsonb_set(NEW.jsonb, '{id}' ,  injectedId::jsonb , true);
      RETURN NEW;
  END;
  $$ language 'plpgsql';

  DROP TRIGGER IF EXISTS set_id_injson_${table.tableName} ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

  CREATE TRIGGER set_id_injson_${table.tableName} BEFORE INSERT OR UPDATE ON ${myuniversity}_${mymodule}.${table.tableName} FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.set_id_injson_${table.tableName}();
<#else>
  DROP TRIGGER IF EXISTS set_id_injson_${table.tableName} ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;
  DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.set_id_injson_${table.tableName}();
</#if>