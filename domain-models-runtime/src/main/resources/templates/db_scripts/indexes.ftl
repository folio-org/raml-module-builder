
  <#-- Create / Drop unique indexes -->
  <#if table.uniqueIndex??>
    <#list table.uniqueIndex as indexes>
        <#if indexes.tOps.name() == "ADD">
    CREATE UNIQUE INDEX ${table.tableName}_${indexes.fieldName}_unique_idx ON ${myuniversity}_${mymodule}.${table.tableName}((${indexes.fieldPath}));
        <#else>
    DROP INDEX IF EXISTS ${table.tableName}_${indexes.fieldName}_unique_idx;
        </#if>
    </#list>
  </#if>
  
  <#-- Create / Drop indexes for fuzzy LIKE matching -->
  <#if table.likeIndex??>
    <#list table.likeIndex as indexes>
      <#if indexes.tOps.name() == "ADD">
    CREATE INDEX ${table.tableName}_${indexes.fieldName}_idx_like ON ${myuniversity}_${mymodule}.${table.tableName}(((${indexes.fieldPath})) varchar_pattern_ops);
      <#else>
    DROP INDEX IF EXISTS ${table.tableName}_${indexes.fieldName}_idx_like;
      </#if>
    </#list>
  </#if>
  
  <#if table.ginIndex??>
    <#list table.ginIndex as indexes>
      <#if indexes.tOps.name() == "ADD">
    CREATE INDEX ${table.tableName}_${indexes.fieldName}_idx_gin ON ${myuniversity}_${mymodule}.${table.tableName} USING GIN ((${indexes.fieldPath}));
      <#else>
    DROP INDEX IF EXISTS ${table.tableName}_${indexes.fieldName}_idx_gin;
      </#if>
    </#list>
  </#if>
