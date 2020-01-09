  <#-- Create / Drop btree indexes -->
  <#if table.index??>
    <#list table.index as indexes>
    DROP INDEX IF EXISTS ${table.tableName}_${indexes.fieldName}_idx;
    <#if indexes.tOps.name() == "ADD">
    <#-- Truncate using left(..., 600) to fit into the 2712 byte limit of PostgreSQL indexes (600 multi-byte characters) -->
    CREATE INDEX IF NOT EXISTS ${table.tableName}_${indexes.fieldName}_idx ON ${myuniversity}_${mymodule}.${table.tableName}
    (${indexes.getFinalTruncatedSqlExpression(table.tableName)})
     <#if indexes.whereClause??> ${indexes.whereClause};<#else>;</#if>
    </#if>
    </#list>
  </#if>

  <#-- Create / Drop unique btree indexes -->
  <#if table.uniqueIndex??>
    <#list table.uniqueIndex as indexes>
    DROP INDEX IF EXISTS ${table.tableName}_${indexes.fieldName}_idx_unique;
    <#if indexes.tOps.name() == "ADD">
    <#-- Do not truncate the value using left(..., 600) -- use complete value for uniqueness check -->
    CREATE UNIQUE INDEX IF NOT EXISTS ${table.tableName}_${indexes.fieldName}_idx_unique ON ${myuniversity}_${mymodule}.${table.tableName}
    (${indexes.getFinalSqlExpression(table.tableName)})
     <#if indexes.whereClause??> ${indexes.whereClause};<#else>;</#if>
    </#if>
    </#list>
  </#if>

  <#-- Create / Drop indexes for fuzzy LIKE matching -->
  <#if table.likeIndex??>
    <#list table.likeIndex as indexes>
    DROP INDEX IF EXISTS ${table.tableName}_${indexes.fieldName}_idx_like;
      <#if indexes.tOps.name() == "ADD">
    CREATE INDEX IF NOT EXISTS ${table.tableName}_${indexes.fieldName}_idx_like ON ${myuniversity}_${mymodule}.${table.tableName}
    ((${indexes.getFinalSqlExpression(table.tableName)}) text_pattern_ops)
    <#if indexes.whereClause??> ${indexes.whereClause};<#else>;</#if>
    </#if>
    </#list>
  </#if>

  <#if table.ginIndex??>
    <#list table.ginIndex as indexes>
    DROP INDEX IF EXISTS ${table.tableName}_${indexes.fieldName}_idx_gin;
      <#if indexes.tOps.name() == "ADD">
    CREATE INDEX IF NOT EXISTS ${table.tableName}_${indexes.fieldName}_idx_gin ON ${myuniversity}_${mymodule}.${table.tableName} USING GIN
        ((${indexes.getFinalSqlExpression(table.tableName)}) gin_trgm_ops)
    <#if indexes.whereClause??> ${indexes.whereClause};<#else>;</#if>
      </#if>
    </#list>
  </#if>

  <#if table.fullTextIndex??>
    <#list table.fullTextIndex as indexes>
     DROP INDEX IF EXISTS ${table.tableName}_${indexes.fieldName}_idx_ft;
      <#if indexes.tOps.name() == "ADD">
     CREATE INDEX IF NOT EXISTS ${table.tableName}_${indexes.fieldName}_idx_ft ON ${myuniversity}_${mymodule}.${table.tableName} USING GIN
        ( to_tsvector('simple', ${indexes.getFinalSqlExpression(table.tableName)}) );
      </#if>
    </#list>
  </#if>
