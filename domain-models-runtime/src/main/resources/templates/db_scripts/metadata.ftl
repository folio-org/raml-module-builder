
-- auto populate the meta data schema

-- These are the metadata fields:
-- creation_date
-- created_by
-- jsonb->'metadata'->>'createdDate'
-- jsonb->'metadata'->>'createdByUserId'
-- jsonb->'metadata'->>'updatedDate'
-- jsonb->'metadata'->>'updatedByUserId'

-- RestVerticle sets all 4 jsonb->'metadata' fields to the current date and current user on insert and on update.
-- The insert trigger copies createdDate and createdByUserId to creation_date and created_by.
-- The update trigger overwrites createdDate and createdByUserId by the values stored
-- in creation_date and created_by.
-- Special case: If NEW.creation_date is null on update then save NEW.jsonb->'metadata' without changes.

-- Restrictions specified in metadata.schema:
-- jsonb->'metadata' is optional, but if it exists then jsonb->'metadata'->>'createdDate' is required.

-- Trigger for insert: Copy createdDate and createdByUserId to creation_date and created_by.
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.${table.tableName}_set_md()
RETURNS TRIGGER AS $$
BEGIN
  NEW.creation_date = NEW.jsonb->'metadata'->>'createdDate' || '+0000';
  NEW.created_by = NEW.jsonb->'metadata'->>'createdByUserId';
  RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS set_${table.tableName}_md_trigger ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

CREATE TRIGGER set_${table.tableName}_md_trigger BEFORE INSERT ON ${myuniversity}_${mymodule}.${table.tableName}
   FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.${table.tableName}_set_md();

-- Trigger for update:
-- Overwrite createdDate and createdByUserId by the values stored in creation_date and created_by.
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.set_${table.tableName}_md_json()
RETURNS TRIGGER AS $$
BEGIN
  if NEW.creation_date IS NULL then
    RETURN NEW;
  end if;

  NEW.jsonb = jsonb_set(NEW.jsonb, '{metadata,createdDate}', to_jsonb(NEW.creation_date AT TIME ZONE '+0000'));
  if NEW.created_by IS NULL then
    NEW.jsonb = NEW.jsonb #- '{metadata,createdByUserId}';
  else
    NEW.jsonb = jsonb_set(NEW.jsonb, '{metadata,createdByUserId}', to_jsonb(NEW.created_by));
  end if;
  RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS set_${table.tableName}_md_json_trigger ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

CREATE TRIGGER set_${table.tableName}_md_json_trigger BEFORE UPDATE ON ${myuniversity}_${mymodule}.${table.tableName}
  FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.set_${table.tableName}_md_json();

----- end auto populate meta data schema ------------
