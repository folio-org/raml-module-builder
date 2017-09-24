
-- auto populate the meta data schema

-- on create of ${table.tableName} record - pull creation date and creator into dedicated column - rmb auto-populates these fields in the md fields
CREATE OR REPLACE FUNCTION ${table.tableName}_set_md()
RETURNS TRIGGER AS $$
BEGIN
  NEW.creation_date = to_timestamp(NEW.jsonb->'metadata'->>'createdDate', 'YYYY-MM-DD"T"HH24:MI:SS.MS');
  NEW.created_by = NEW.jsonb->'metadata'->>'createdByUserId';
  RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS set_${table.tableName}_md_trigger ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

CREATE TRIGGER set_${table.tableName}_md_trigger BEFORE INSERT ON ${myuniversity}_${mymodule}.${table.tableName}
   FOR EACH ROW EXECUTE PROCEDURE ${table.tableName}_set_md();

-- on update populate md fields from the creation date and creator fields

CREATE OR REPLACE FUNCTION set_${table.tableName}_md_json()
RETURNS TRIGGER AS $$
DECLARE
  createdDate timestamp WITH TIME ZONE;
  createdBy text ;
  updatedDate timestamp WITH TIME ZONE;
  updatedBy text ;
  injectedId text;
BEGIN
  createdBy = NEW.created_by;
  createdDate = NEW.creation_date;
  updatedDate = NEW.jsonb->'metadata'->>'updatedDate';
  updatedBy = NEW.jsonb->'metadata'->>'updatedByUserId';

  if createdBy ISNULL then
    createdBy = 'undefined';
  end if;
  if updatedBy ISNULL then
    updatedBy = 'undefined';
  end if;
  if createdDate IS NOT NULL then
-- creation date and update date will always be injected by rmb - if created date is null it means that there is no meta data object
-- associated with this object - so only add the meta data if created date is not null -- created date being null may be a problem
-- and should be handled at the app layer for now -- currently this protects against an exception in the db if no md is present in the json
    injectedId = '{"createdDate":"'||to_char(createdDate,'YYYY-MM-DD"T"HH24:MI:SS.MS')||'" , "createdByUserId":"'||createdBy||'", "updatedDate":"'||to_char(updatedDate,'YYYY-MM-DD"T"HH24:MI:SS.MSOF')||'" , "updatedByUserId":"'||updatedBy||'"}';
    NEW.jsonb = jsonb_set(NEW.jsonb, '{metadata}' ,  injectedId::jsonb , false);
  else
    NEW.jsonb = NEW.jsonb;
  end if;
RETURN NEW;

END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS set_${table.tableName}_md_json_trigger ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

CREATE TRIGGER set_${table.tableName}_md_json_trigger BEFORE UPDATE ON ${myuniversity}_${mymodule}.${table.tableName}
  FOR EACH ROW EXECUTE PROCEDURE set_${table.tableName}_md_json();

----- end auto populate meta data schema ------------
