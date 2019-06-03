
-- audit table to keep a history of the changes
-- made to a record.
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.audit_${table.tableName} (
   id UUID PRIMARY KEY,
   orig_id UUID NOT NULL,
   operation char(1) NOT NULL,
   jsonb jsonb,
   created_date timestamp not null
   );

CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.audit_${table.tableName}_changes() RETURNS TRIGGER AS $${table.tableName}_audit$
    DECLARE
    <#if table.auditingSnippet??>
      <#if table.auditingSnippet.delete??>
      ${table.auditingSnippet.delete.declare}
      </#if>
      <#if table.auditingSnippet.update??>
      ${table.auditingSnippet.update.declare}
      </#if>
      <#if table.auditingSnippet.insert??>
      ${table.auditingSnippet.insert.declare}
      </#if>
    </#if>
      seed TEXT;
      maxid UUID;
    BEGIN
        maxid = (SELECT ${myuniversity}_${mymodule}.max(id) FROM ${myuniversity}_${mymodule}.audit_${table.tableName});
        IF maxid IS NULL THEN
            seed = md5(concat('${myuniversity}_${mymodule}.audit_${table.tableName}', NEW.jsonb));
            -- UUID version byte
            seed = overlay(seed placing '4' from 13);
            -- UUID variant byte
            seed = overlay(seed placing '8' from 17);
            maxid = seed::uuid;
        ELSE
            maxid = ${myuniversity}_${mymodule}.next_uuid(maxid);
        END IF;
        IF (TG_OP = 'DELETE') THEN
          <#if table.auditingSnippet?? && table.auditingSnippet.delete??>
            ${table.auditingSnippet.delete.statement}
          </#if>
            INSERT INTO ${myuniversity}_${mymodule}.audit_${table.tableName} SELECT maxid, OLD.id, 'D', OLD.jsonb, current_timestamp;
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
          <#if table.auditingSnippet?? && table.auditingSnippet.update??>
            ${table.auditingSnippet.update.statement}
          </#if>
            INSERT INTO ${myuniversity}_${mymodule}.audit_${table.tableName} SELECT maxid, NEW.id, 'U', NEW.jsonb, current_timestamp;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
          <#if table.auditingSnippet?? && table.auditingSnippet.insert??>
            ${table.auditingSnippet.insert.statement}
          </#if>
            INSERT INTO ${myuniversity}_${mymodule}.audit_${table.tableName} SELECT maxid, NEW.id, 'I', NEW.jsonb, current_timestamp;
            RETURN NEW;
        END IF;
        RETURN NULL;
    END;
$${table.tableName}_audit$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS audit_${table.tableName} ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

CREATE TRIGGER audit_${table.tableName} AFTER INSERT OR UPDATE OR DELETE ON ${myuniversity}_${mymodule}.${table.tableName} FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.audit_${table.tableName}_changes();
