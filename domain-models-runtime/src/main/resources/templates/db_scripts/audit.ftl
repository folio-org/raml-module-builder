-- trigger for the audit table to keep a history of the changes made to a record.
CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.audit_${table.tableName}_changes() RETURNS TRIGGER AS $${table.tableName}_audit$
  DECLARE
  <#if (table.auditingSnippet.delete.declare)??>
    ${table.auditingSnippet.delete.declare}
  </#if>
  <#if (table.auditingSnippet.update.declare)??>
    ${table.auditingSnippet.update.declare}
  </#if>
  <#if (table.auditingSnippet.insert.declare)??>
    ${table.auditingSnippet.insert.declare}
  </#if>
    jsonb JSONB;
    uuidtext TEXT;
    uuid UUID;
  BEGIN
    jsonb = CASE WHEN TG_OP = 'DELETE' THEN OLD.jsonb ELSE NEW.jsonb END;

    -- create uuid based on the jsonb value so that concurrent updates of different records are possible.
    uuidtext = md5(jsonb::text);
    -- UUID version byte
    uuidtext = overlay(uuidtext placing '4' from 13);
    -- UUID variant byte
    uuidtext = overlay(uuidtext placing '8' from 17);
    uuid = uuidtext::uuid;
    -- If uuid is already in use increment until an unused is found. This can only happen if the jsonb content
    -- is exactly the same. This should be very rare when it includes a timestamp.
    WHILE EXISTS (SELECT 1 FROM ${myuniversity}_${mymodule}.${table.auditingTableName} WHERE id = uuid) LOOP
      uuid = ${myuniversity}_${mymodule}.next_uuid(uuid);
    END LOOP;

    jsonb = jsonb_build_object(
      'id', to_jsonb(uuid::text),
      '${table.auditingFieldName}', jsonb,
      'operation', to_jsonb(left(TG_OP, 1)),
      'createdDate', to_jsonb(current_timestamp::text));
    IF (TG_OP = 'DELETE') THEN
      <#if (table.auditingSnippet.delete.statement)??>
        ${table.auditingSnippet.delete.statement}
      </#if>
    ELSIF (TG_OP = 'UPDATE') THEN
      <#if (table.auditingSnippet.update.statement)??>
        ${table.auditingSnippet.update.statement}
      </#if>
    ELSIF (TG_OP = 'INSERT') THEN
      <#if (table.auditingSnippet.insert.statement)??>
        ${table.auditingSnippet.insert.statement}
      </#if>
    END IF;
    INSERT INTO ${myuniversity}_${mymodule}.${table.auditingTableName} VALUES (uuid, jsonb);
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
  END;
$${table.tableName}_audit$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS audit_${table.tableName} ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

CREATE TRIGGER audit_${table.tableName} AFTER INSERT OR UPDATE OR DELETE ON ${myuniversity}_${mymodule}.${table.tableName}
  FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.audit_${table.tableName}_changes();
