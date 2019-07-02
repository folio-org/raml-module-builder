-- trigger for the audit table to keep a history of the changes made to a record.
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
    jsonb JSONB;
  BEGIN
    maxid = (SELECT ${myuniversity}_${mymodule}.max(id) FROM ${myuniversity}_${mymodule}.${table.auditingTableName});
    IF maxid IS NULL THEN
      -- don't use random, that will be different on each pg-pool replication node
      seed = md5(concat('${myuniversity}_${mymodule}.${table.auditingTableName}', NEW.jsonb));
      -- avoid overflow
      IF (left(seed, 13) = 'ffffffff-ffff') THEN
        seed = overlay(seed placing '00000000-0000' from 1);
      END IF;
      -- UUID version byte
      seed = overlay(seed placing '4' from 13);
      -- UUID variant byte
      seed = overlay(seed placing '8' from 17);
      maxid = seed::uuid;
    ELSE
      maxid = ${myuniversity}_${mymodule}.next_uuid(maxid);
    END IF;
    jsonb = jsonb_build_object(
      'id', to_jsonb(maxid::text),
      '${table.auditingFieldName}', CASE WHEN TG_OP = 'DELETE' THEN OLD.jsonb ELSE NEW.jsonb END,
      'operation', to_jsonb(left(TG_OP, 1)),
      'createdDate', to_jsonb(current_timestamp::text));
    IF (TG_OP = 'DELETE') THEN
      <#if table.auditingSnippet?? && table.auditingSnippet.delete??>
        ${table.auditingSnippet.delete.statement}
      </#if>
    ELSIF (TG_OP = 'UPDATE') THEN
      <#if table.auditingSnippet?? && table.auditingSnippet.update??>
        ${table.auditingSnippet.update.statement}
      </#if>
    ELSIF (TG_OP = 'INSERT') THEN
      <#if table.auditingSnippet?? && table.auditingSnippet.insert??>
        ${table.auditingSnippet.insert.statement}
      </#if>
    END IF;
    INSERT INTO ${myuniversity}_${mymodule}.${table.auditingTableName} VALUES (maxid, jsonb);
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
  END;
$${table.tableName}_audit$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS audit_${table.tableName} ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

CREATE TRIGGER audit_${table.tableName} AFTER INSERT OR UPDATE OR DELETE ON ${myuniversity}_${mymodule}.${table.tableName}
  FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.audit_${table.tableName}_changes();
