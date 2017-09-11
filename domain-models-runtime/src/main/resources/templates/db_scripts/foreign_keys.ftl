  
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
    CREATE OR REPLACE FUNCTION update_${table.tableName}_references()
    RETURNS TRIGGER AS $$
    BEGIN
      <#list table.foreignKeys as key>
      NEW.${key.fieldName} = NEW.${key.fieldPath};
      </#list>
      RETURN NEW;
    END;
    $$ language 'plpgsql';
    
    <#if table.mode == "update">
      DROP TRIGGER IF EXISTS update_${table.tableName}_references ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;
    </#if>
    <#-- Create / Drop trigger to call foreign key function -->
    CREATE TRIGGER update_${table.tableName}_references
      BEFORE INSERT OR UPDATE ON ${myuniversity}_${mymodule}.${table.tableName}
      FOR EACH ROW EXECUTE PROCEDURE update_${table.tableName}_references();
  </#if>