
  <#-- Create / Drop foreign keys -->
  <#if table.foreignKeys??>
    <#list table.foreignKeys?filter(key -> key.fieldName??) as key>
      <#if key.tOps.name() == "ADD">
        ALTER TABLE ${myuniversity}_${mymodule}.${table.tableName}
          ADD COLUMN IF NOT EXISTS ${key.fieldName} UUID;
        DO $$
        BEGIN
          <#-- PostgreSQL doesn't have an "ADD CONSTRAINT IF NOT EXISTS" statement.
               Reference: https://stackoverflow.com/a/32526723
               Try to add the constraint and ignore the exception if it already exists.
          -->
          BEGIN
            ALTER TABLE ${table.tableName}
              ADD CONSTRAINT ${key.fieldName}_${key.targetTable}_fkey
              FOREIGN KEY (${key.fieldName}) REFERENCES ${key.targetTable};
          EXCEPTION
            WHEN duplicate_object OR duplicate_table THEN NULL;
          END;
        END $$;
        CREATE INDEX IF NOT EXISTS ${table.tableName}_${key.fieldName}_idx
          ON ${myuniversity}_${mymodule}.${table.tableName} (${key.fieldName});
        INSERT INTO rmb_internal_analyze VALUES ('${table.tableName}');
      <#else>
        ALTER TABLE ${myuniversity}_${mymodule}.${table.tableName}
          DROP COLUMN IF EXISTS ${key.fieldName} CASCADE;
        <#-- DROP COLUMN also drops the index on that column. -->
      </#if>
    </#list>
  </#if>

  <#-- Does foreign key list has at least one "ADD" entry? -->
  <#if (table.foreignKeys!)?filter(key -> key.fieldName?? && key.tOps.name() == "ADD")?size gt 0>

    <#-- function which pulls data from json into the created foreign key columns -->
    CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.update_${table.tableName}_references()
    RETURNS TRIGGER AS $$
    BEGIN
      <#list table.foreignKeys?filter(key -> key.fieldName?? && key.tOps.name() == "ADD") as key>
      NEW.${key.fieldName} = ${key.fieldPath};
      </#list>
      RETURN NEW;
    END;
    $$ language 'plpgsql';

    <#-- in update mode try to drop the trigger and re-create (below) since there is no "CREATE TRIGGER IF NOT EXISTS" -->
    DROP TRIGGER IF EXISTS update_${table.tableName}_references ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

    <#-- Create trigger to call foreign key function -->
    CREATE TRIGGER update_${table.tableName}_references
      BEFORE INSERT OR UPDATE ON ${myuniversity}_${mymodule}.${table.tableName}
      FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.update_${table.tableName}_references();

    -- Remove duplicate foreign key constraints created by RMB before 30.1.0
    -- https://issues.folio.org/browse/RMB-555
    DO $$
    DECLARE
      version TEXT;
      i INT;
    BEGIN
      SELECT jsonb->>'rmbVersion' INTO version FROM rmb_internal;
      IF version !~ '^(\d\.|1\d\.|2\d\.|30\.0\.)' THEN
        RETURN;
      END IF;
      <#list table.foreignKeys?filter(key -> key.fieldName?? && key.tOps.name() == "ADD") as key>
      FOR i IN 1..50 LOOP
        EXECUTE 'ALTER TABLE ${table.tableName} DROP CONSTRAINT IF EXISTS '
                || '${key.fieldName}_${key.targetTable}_fkey' || i;
      END LOOP;
      </#list>
    END $$;

  <#else>
    <#-- foreign key list is empty, attempt to drop trigger and then function -->
    DROP TRIGGER IF EXISTS update_${table.tableName}_references ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;
    DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.update_${table.tableName}_references();
  </#if>
