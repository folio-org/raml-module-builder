CREATE OR REPLACE FUNCTION set_id_injson_${table.tableName}()
RETURNS TRIGGER AS $$
DECLARE
  injectedId text;
BEGIN
  injectedId = '"'||NEW.id||'"';
  NEW.jsonb = jsonb_set(NEW.jsonb, '{id}' ,  injectedId::jsonb , true);
    RETURN NEW;
END;
$$ language 'plpgsql';
CREATE TRIGGER set_id_injson_${table.tableName} BEFORE INSERT OR UPDATE ON ${myuniversity}_${mymodule}.${table.tableName} FOR EACH ROW EXECUTE PROCEDURE  set_id_injson_${table.tableName}();