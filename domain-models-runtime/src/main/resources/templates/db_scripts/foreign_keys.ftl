
  <#-- Create / Drop foreign keys -->
  <#if table.foreignKeys??>
    ALTER TABLE ${myuniversity}_${mymodule}.${table.tableName}
    <#list table.foreignKeys as key>
      <#if key.tOps.name() == "ADD">
        ADD COLUMN IF NOT EXISTS ${key.fieldName} UUID REFERENCES ${myuniversity}_${mymodule}.${key.targetTable}<#sep>,
      <#else>
        DROP COLUMN IF EXISTS ${key.fieldName} CASCADE;
      </#if>
    </#list>;
  </#if>

  <#-- Create / Drop foreign keys function which pulls data from json into the created foreign key columns -->
  <#if table.foreignKeys??>
    <#-- foreign key list has at least one entry, create / re-create the function with the current keys -->
    CREATE OR REPLACE FUNCTION ${myuniversity}_${mymodule}.update_${table.tableName}_references()
    RETURNS TRIGGER AS $$
    BEGIN
      <#list table.foreignKeys as key>
      NEW.${key.fieldName} = ${key.fieldPath};
      </#list>
      RETURN NEW;
    END;
    $$ language 'plpgsql';

    <#-- in update mode try to drop the trigger and re-create (below) since there is no create trigger if not exists -->
    DROP TRIGGER IF EXISTS update_${table.tableName}_references ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;

    <#-- Create / Drop trigger to call foreign key function -->
    CREATE TRIGGER update_${table.tableName}_references
      BEFORE INSERT OR UPDATE ON ${myuniversity}_${mymodule}.${table.tableName}
      FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.update_${table.tableName}_references();

  <#else>
    <#-- foreign key list is empty attempt to drop trigger and then function -->
    DROP TRIGGER IF EXISTS update_${table.tableName}_references ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;
    DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.update_${table.tableName}_references();
  </#if>