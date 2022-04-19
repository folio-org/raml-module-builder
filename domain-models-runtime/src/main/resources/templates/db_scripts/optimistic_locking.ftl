-- auto update optimistic locking version
-- ERROR code 23F09: 23 for class 23 — Integrity Constraint Violation, F for FOLIO, 09 for 409 HTTP status code

<#if table.withOptimisticLocking?? && table.withOptimisticLocking.name() != "OFF">
  <#assign ol_version = "_version">
  CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.${table.tableName}_set_ol_version()
  RETURNS trigger AS $$
  BEGIN
    CASE TG_OP
      WHEN 'INSERT' THEN
          NEW.jsonb = jsonb_set(NEW.jsonb, '{${ol_version}}', to_jsonb(1));
      WHEN 'UPDATE' THEN
        IF NEW.jsonb->'${ol_version}' IS DISTINCT FROM OLD.jsonb->'${ol_version}' THEN
          <#if table.withOptimisticLocking.name() == "FAIL">
            RAISE 'Cannot update record % because it has been changed (optimistic locking): '
          <#else>
            RAISE NOTICE 'Ignoring optimistic locking conflict while overwriting changed record %: '
          </#if>
                'Stored ${ol_version} is %, ${ol_version} of request is %',
                OLD.id, OLD.jsonb->'${ol_version}', NEW.jsonb->'${ol_version}' 
                USING ERRCODE = '23F09', TABLE = '${table.tableName}', SCHEMA = '${myuniversity}_${mymodule}';
        END IF;
        NEW.jsonb = jsonb_set(NEW.jsonb, '{${ol_version}}',
            to_jsonb(COALESCE(((OLD.jsonb->>'${ol_version}')::numeric + 1) % 2147483648, 1)));
    END CASE;
    RETURN NEW;
  END;
  $$ LANGUAGE plpgsql;
  
  DROP TRIGGER IF EXISTS set_${table.tableName}_ol_version_trigger
    ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;
    
  CREATE TRIGGER set_${table.tableName}_ol_version_trigger BEFORE INSERT OR UPDATE
    ON ${myuniversity}_${mymodule}.${table.tableName}
    FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.${table.tableName}_set_ol_version();
  
<#else>
  DROP TRIGGER IF EXISTS set_${table.tableName}_ol_version_trigger
    ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;
  DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.${table.tableName}_set_ol_version() CASCADE;
</#if>

----- end auto update optimistic locking version ------------
