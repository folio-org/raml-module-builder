
-- auto populate the meta data schema

-- These are the metadata fields:
-- creation_date TIMESTAMP
-- created_by TEXT
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

<#if table.withMetadata == true>

-- Trigger for insert: Copy createdDate and createdByUserId to creation_date and created_by.
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.${table.tableName}_set_md()
RETURNS TRIGGER AS $$
DECLARE
  input text;
  createdDate timestamp;
BEGIN
  input = NEW.jsonb->'metadata'->>'createdDate';
  IF input IS NULL THEN
    RETURN NEW;
  END IF;
  -- time stamp without time zone?
  IF (input::timestamp::timestamptz = input::timestamptz) THEN
    -- createdDate already has no time zone, normalize using ::timestamp
    createdDate = input::timestamp;
  ELSE
    -- createdDate has a time zone string
    -- normalize using ::timestamptz, convert to '+00' time zone and remove time zone string
    createdDate = input::timestamptz AT TIME ZONE '+00';
  END IF;
  NEW.jsonb = jsonb_set(NEW.jsonb, '{metadata,createdDate}', to_jsonb(to_char(createdDate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')));
  NEW.jsonb = jsonb_set(NEW.jsonb, '{metadata,updatedDate}', to_jsonb(to_char(createdDate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')));
  NEW.creation_date = createdDate;
  NEW.created_by = NEW.jsonb->'metadata'->>'createdByUserId';
  RETURN NEW;
END;
$$ language 'plpgsql';

<#else>

DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.${table.tableName}_set_md() CASCADE;

</#if>

DROP TRIGGER IF EXISTS set_${table.tableName}_md_trigger ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

<#if table.withMetadata == true>

CREATE TRIGGER set_${table.tableName}_md_trigger BEFORE INSERT ON ${myuniversity}_${mymodule}.${table.tableName}
   FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.${table.tableName}_set_md();

</#if>

<#if table.withMetadata == true>

-- Trigger for update:
-- Overwrite createdDate and createdByUserId by the values stored in creation_date and created_by.
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.set_${table.tableName}_md_json()
RETURNS TRIGGER AS $$
DECLARE
  input text;
  updatedDate timestamp;
BEGIN
  if NEW.creation_date IS NULL then
    RETURN NEW;
  end if;

  NEW.jsonb = jsonb_set(NEW.jsonb, '{metadata,createdDate}', to_jsonb(to_char(NEW.creation_date, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')));
  if NEW.created_by IS NULL then
    NEW.jsonb = NEW.jsonb #- '{metadata,createdByUserId}';
  else
    NEW.jsonb = jsonb_set(NEW.jsonb, '{metadata,createdByUserId}', to_jsonb(NEW.created_by));
  end if;

  input = NEW.jsonb->'metadata'->>'updatedDate';
  if input IS NOT NULL then
    -- time stamp without time zone?
    IF (input::timestamp::timestamptz = input::timestamptz) THEN
      -- updatedDate already has no time zone, normalize using ::timestamp
      updatedDate = input::timestamp;
    ELSE
      -- updatedDate has a time zone string
      -- normalize using ::timestamptz, convert to '+00' time zone and remove time zone string
      updatedDate = input::timestamptz AT TIME ZONE '+00';
    END IF;
    NEW.jsonb = jsonb_set(NEW.jsonb, '{metadata,updatedDate}', to_jsonb(to_char(updatedDate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')));
  end if;
  RETURN NEW;
END;
$$ language 'plpgsql';

<#else>

DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.set_${table.tableName}_md_json() CASCADE;

</#if>

DROP TRIGGER IF EXISTS set_${table.tableName}_md_json_trigger ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

<#if table.withMetadata == true>

CREATE TRIGGER set_${table.tableName}_md_json_trigger BEFORE UPDATE ON ${myuniversity}_${mymodule}.${table.tableName}
  FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.set_${table.tableName}_md_json();

</#if>

----- end auto populate meta data schema ------------
