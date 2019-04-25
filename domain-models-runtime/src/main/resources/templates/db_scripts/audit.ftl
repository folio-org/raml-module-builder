
-- audit table to keep a history of the changes
-- made to a record.
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.audit_${table.tableName} (
   ${table.pkColumnName} UUID PRIMARY KEY,
   orig_id UUID NOT NULL,
   operation char(1) NOT NULL,
   jsonb jsonb,
   created_date timestamp not null
   );

CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.audit_${table.tableName}_changes() RETURNS TRIGGER AS $${table.tableName}_audit$
    <#if table.auditingSnippet??>
    DECLARE
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
    id TEXT
    BEGIN
    IF((select COUNT(*) from ${myuniversity}_${mymodule}.audit_${table.tableName}) = 0)
      id = MD5(current_timestamp + "${myuniversity}_${mymodule}.audit_${table.tableName}" + jsonb)
    ELSE
      id = maxUUID(${myuniversity}_${mymodule}.audit_${table.tableName}) + 1;
    END IF 
        IF (TG_OP = 'DELETE') THEN
          <#if table.auditingSnippet?? && table.auditingSnippet.delete??>
            ${table.auditingSnippet.delete.statement}
          </#if>
            INSERT INTO ${myuniversity}_${mymodule}.audit_${table.tableName} SELECT id, OLD.${table.pkColumnName}, 'D', OLD.jsonb, current_timestamp;
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
          <#if table.auditingSnippet?? && table.auditingSnippet.update??>
            ${table.auditingSnippet.update.statement}
          </#if>
            INSERT INTO ${myuniversity}_${mymodule}.audit_${table.tableName} SELECT id, NEW.${table.pkColumnName}, 'U', NEW.jsonb, current_timestamp;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
          <#if table.auditingSnippet?? && table.auditingSnippet.insert??>
            ${table.auditingSnippet.insert.statement}
          </#if>
            INSERT INTO ${myuniversity}_${mymodule}.audit_${table.tableName} SELECT id, NEW.${table.pkColumnName}, 'I', NEW.jsonb, current_timestamp;
            RETURN NEW;
        END IF;
        RETURN NULL;
    END;
$${table.tableName}_audit$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS audit_${table.tableName} ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

CREATE TRIGGER audit_${table.tableName} AFTER INSERT OR UPDATE OR DELETE ON ${myuniversity}_${mymodule}.${table.tableName} FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.audit_${table.tableName}_changes();

GRANT ALL PRIVILEGES ON audit_${table.tableName} TO ${myuniversity}_${mymodule};
