
-- auto populate the meta data schema

-- on create of ${myuniversity}_${mymodule}.${table.tableName} record - pull creation date and creator into dedicated column - rmb auto-populates these fields in the md fields
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.${table.tableName}_set_md()
RETURNS TRIGGER AS $$
BEGIN
  NEW.creation_date = to_timestamp(NEW.jsonb->'metadata'->>'createdDate', 'YYYY-MM-DD"T"HH24:MI:SS.MS');
  NEW.created_by = NEW.jsonb->'metadata'->>'createdByUserId';
  RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS set_${table.tableName}_md_trigger ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

CREATE TRIGGER set_${table.tableName}_md_trigger BEFORE INSERT ON ${myuniversity}_${mymodule}.${table.tableName}
   FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.${table.tableName}_set_md();

-- on update populate md fields from the creation date and creator fields

CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.set_${table.tableName}_md_json()
RETURNS TRIGGER AS $$
DECLARE
  createdDate timestamp WITH TIME ZONE;
  createdBy text;
  updatedDate timestamp WITH TIME ZONE;
  updatedBy text;

  dateFormat text := 'YYYY-MM-DD"T"HH24:MI:SS.MSOF';
  injectedMetadata jsonb;

BEGIN
  SET TIME ZONE 'UTC';

  createdBy = NEW.created_by;
  createdDate = NEW.creation_date;
  updatedDate = NEW.jsonb->'metadata'->>'updatedDate';
  updatedBy = NEW.jsonb->'metadata'->>'updatedByUserId';

  if createdDate IS NOT NULL then

    injectedMetadata = jsonb_build_object(
      'createdByUserId', createdBy,
      'createdDate', to_char(createdDate, dateFormat),
      'updatedDate', to_char(updatedDate, dateFormat)
    );
    if updatedBy IS NOT NULL then
      injectedMetadata = jsonb_set(injectedMetadata, '{updatedByUserId}', to_jsonb(updatedBy));
    end if;

    NEW.jsonb = jsonb_set(NEW.jsonb, '{metadata}', injectedMetadata);
  end if;

RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS set_${table.tableName}_md_json_trigger ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

CREATE TRIGGER set_${table.tableName}_md_json_trigger BEFORE UPDATE ON ${myuniversity}_${mymodule}.${table.tableName}
  FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.set_${table.tableName}_md_json();

----- end auto populate meta data schema ------------
