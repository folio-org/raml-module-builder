<#-- Create / Drop btree indexes -->
<#if table.index??>
  <#list table.index as indexes>
    DO $do$
    BEGIN
      PERFORM ${myuniversity}_${mymodule}.rmb_internal_index('${table.tableName}_${indexes.fieldName}_idx', '${indexes.tOps.name()}',
      'CREATE INDEX IF NOT EXISTS ${table.tableName}_${indexes.fieldName}_idx ON ${myuniversity}_${mymodule}.${table.tableName} '
      <#-- Truncate using left(..., 600) to fit into the 2712 byte limit of PostgreSQL indexes (600 multi-byte characters) -->
      || $rmb$(${indexes.getFinalTruncatedSqlExpression(table.tableName)})$rmb$
      <#if indexes.whereClause??>|| $rmb$ ${indexes.whereClause}$rmb$</#if>);
    END $do$;
  </#list>
</#if>

<#-- Create / Drop unique btree indexes -->
<#if table.uniqueIndex??>
  <#list table.uniqueIndex as indexes>
    DO $do$
    BEGIN
      PERFORM ${myuniversity}_${mymodule}.rmb_internal_index('${table.tableName}_${indexes.fieldName}_idx_unique', '${indexes.tOps.name()}',
      'CREATE UNIQUE INDEX IF NOT EXISTS ${table.tableName}_${indexes.fieldName}_idx_unique ON ${myuniversity}_${mymodule}.${table.tableName} '
      <#-- Do not truncate the value using left(..., 600) -- use complete value for uniqueness check -->
      || $rmb$(${indexes.getFinalSqlExpression(table.tableName)})$rmb$
      <#if indexes.whereClause??>|| $rmb$ ${indexes.whereClause}$rmb$</#if>);
    END $do$;
  </#list>
</#if>

<#-- Create / Drop indexes for fuzzy LIKE matching -->
<#if table.likeIndex??>
  <#list table.likeIndex as indexes>
    DO $do$
    BEGIN
      PERFORM ${myuniversity}_${mymodule}.rmb_internal_index('${table.tableName}_${indexes.fieldName}_idx_like', '${indexes.tOps.name()}',
      'CREATE INDEX IF NOT EXISTS ${table.tableName}_${indexes.fieldName}_idx_like ON ${myuniversity}_${mymodule}.${table.tableName} '
      || $rmb$((${indexes.getFinalSqlExpression(table.tableName)}) text_pattern_ops)$rmb$
      <#if indexes.whereClause??>|| $rmb$ ${indexes.whereClause}$rmb$</#if>);
    END $do$;
  </#list>
</#if>

<#if table.ginIndex??>
  <#list table.ginIndex as indexes>
    DO $do$
    BEGIN
      PERFORM ${myuniversity}_${mymodule}.rmb_internal_index('${table.tableName}_${indexes.fieldName}_idx_gin', '${indexes.tOps.name()}',
      'CREATE INDEX IF NOT EXISTS ${table.tableName}_${indexes.fieldName}_idx_gin ON ${myuniversity}_${mymodule}.${table.tableName} USING GIN '
      || $rmb$((${indexes.getFinalSqlExpression(table.tableName)}) public.gin_trgm_ops)$rmb$
      <#if indexes.whereClause??>|| $rmb$ ${indexes.whereClause}$rmb$</#if>);
    END $do$;
  </#list>
</#if>

<#if table.fullTextIndex??>
  <#list table.fullTextIndex as indexes>
    DO $do$
    BEGIN
      PERFORM ${myuniversity}_${mymodule}.rmb_internal_index('${table.tableName}_${indexes.fieldName}_idx_ft', '${indexes.tOps.name()}',
      'CREATE INDEX IF NOT EXISTS ${table.tableName}_${indexes.fieldName}_idx_ft ON ${myuniversity}_${mymodule}.${table.tableName} USING GIN '
      || $rmb$( to_tsvector('simple', ${indexes.getFinalSqlExpression(table.tableName)}) )$rmb$);
    END $do$;
  </#list>
</#if>
