-- auto populate the meta data schema

-- add the two needed columns per table

IF COL_LENGTH('myuniversity_mymodule.<table_name>', 'creation_date') IS NULL
BEGIN
    ALTER TABLE myuniversity_mymodule.<table_name>
    ADD COLUMN creation_date timestamp WITH TIME ZONE,
    ADD COLUMN created_by text;
END

-- on create of user record - pull creation date and creator into dedicated column - rmb auto-populates these fields in the md fields
CREATE OR REPLACE FUNCTION set_md()
RETURNS TRIGGER AS $$
BEGIN
  NEW.creation_date = to_timestamp(NEW.jsonb->'metadata'->>'createdDate', 'YYYY-MM-DD"T"HH24:MI:SS.MS');
  NEW.created_by = NEW.jsonb->'metadata'->>'createdByUserId';
  RETURN NEW;
END;
$$ language 'plpgsql';
CREATE TRIGGER set_md_trigger BEFORE INSERT ON myuniversity_mymodule.<table_name>
   FOR EACH ROW EXECUTE PROCEDURE  set_md();

-- on update populate md fields from the creation date and creator fields
CREATE OR REPLACE FUNCTION set_md_json()
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
CREATE TRIGGER set_md_json_trigger BEFORE UPDATE ON myuniversity_mymodule.<table_name>
  FOR EACH ROW EXECUTE PROCEDURE set_md_json();

-- --- end auto populate meta data schema ------------

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA myuniversity_mymodule TO myuniversity_mymodule;
