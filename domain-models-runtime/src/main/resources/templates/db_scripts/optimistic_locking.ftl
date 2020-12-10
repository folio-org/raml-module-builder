-- auto update optimistic locking version

<#if table.withOptimisticLocking??>
  <#assign ol_version = "_version">
  <#assign ol_notice_level = "WARNING">
  CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.${table.tableName}_set_ol_version()
  RETURNS trigger AS $$
  BEGIN
    CASE TG_OP
      WHEN 'INSERT' THEN
        <#if table.withOptimisticLocking.name() == "OFF">
          NEW.jsonb = NEW.jsonb - '${ol_version}';
        <#else>
          NEW.jsonb = jsonb_set(NEW.jsonb, '{${ol_version}}', to_jsonb(1));
        </#if>
      WHEN 'UPDATE' THEN
        <#if table.withOptimisticLocking.name() == "OFF">
          NEW.jsonb = NEW.jsonb - '${ol_version}';
        <#else>
          IF NEW.jsonb->'${ol_version}' IS DISTINCT FROM OLD.jsonb->'${ol_version}' THEN
            <#if table.withOptimisticLocking.name() == "FAIL">
              <#assign ol_notice_level = "EXCEPTION">
            </#if>
            RAISE ${ol_notice_level} 'Cannot update record % because it has been changed: '
                'Stored ${ol_version} is %, ${ol_version} of request is %',
                OLD.id, OLD.jsonb->'${ol_version}', NEW.jsonb->'${ol_version}';
          END IF;
          NEW.jsonb = jsonb_set(NEW.jsonb, '{${ol_version}}',
              to_jsonb(COALESCE((OLD.jsonb->>'${ol_version}')::numeric + 1, 1)));
        </#if>
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