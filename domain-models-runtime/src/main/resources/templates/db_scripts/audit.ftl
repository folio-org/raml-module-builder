
-- audit table to keep a history of the changes
-- made to a record.
CREATE TABLE IF NOT EXISTS audit_${table.tableName} (
   ${table.pkColumnName} UUID PRIMARY KEY,
   orig_id UUID NOT NULL,
   operation char(1) NOT NULL,
   jsonb jsonb,
   created_date timestamp not null
   );

CREATE OR REPLACE FUNCTION audit_${table.tableName}_changes() RETURNS TRIGGER AS $${table.tableName}_audit$
    <#if table.auditingSnippet.delete.declare?? || table.auditingSnippet.update.declare?? || table.auditingSnippet.insert.declare??>
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
    BEGIN
        IF (TG_OP = 'DELETE') THEN
          <#if table.auditingSnippet.delete??>
            ${table.auditingSnippet.delete.statement}
          </#if>
            INSERT INTO ${myuniversity}_${mymodule}.audit_${table.tableName} SELECT gen_random_uuid(), OLD.${table.pkColumnName}, 'D', OLD.jsonb, current_timestamp;
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
          <#if table.auditingSnippet.update??>
            ${table.auditingSnippet.update.statement}
          </#if>
            INSERT INTO ${myuniversity}_${mymodule}.audit_${table.tableName} SELECT gen_random_uuid(), NEW.${table.pkColumnName}, 'U', NEW.jsonb, current_timestamp;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
          <#if table.auditingSnippet.insert??>
            ${table.auditingSnippet.insert.statement}
          </#if>
            INSERT INTO ${myuniversity}_${mymodule}.audit_${table.tableName} SELECT gen_random_uuid(), NEW.${table.pkColumnName}, 'I', NEW.jsonb, current_timestamp;
            RETURN NEW;
        END IF;
        RETURN NULL;
    END;
$${table.tableName}_audit$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS audit_${table.tableName} ON ${table.tableName} CASCADE;

CREATE TRIGGER audit_${table.tableName} AFTER INSERT OR UPDATE OR DELETE ON ${table.tableName} FOR EACH ROW EXECUTE PROCEDURE audit_${table.tableName}_changes();

GRANT ALL PRIVILEGES ON audit_${table.tableName} TO ${myuniversity}_${mymodule};
