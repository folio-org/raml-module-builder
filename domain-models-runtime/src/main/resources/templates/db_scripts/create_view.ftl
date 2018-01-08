-- left join between tables
CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.${view.viewName} AS select 
  <#list view.join as joins>
    <#if joins?index==0>
    ${joins.table.prefix}.${view.pkColumnName},
    </#if>
    ${joins.table.prefix}.jsonb as ${joins.table.jsonFieldAlias},
    ${joins.joinTable.prefix}.jsonb as ${joins.joinTable.jsonFieldAlias}<#sep>,
  </#list>
  from
  <#list view.join as joins>
    <#if joins?index==0>
    ${myuniversity}_${mymodule}.${joins.table.tableName} ${joins.table.prefix}
    </#if>
    ${view.joinType}
    ${myuniversity}_${mymodule}.${joins.joinTable.tableName} ${joins.joinTable.prefix}
    on
    <#if joins.table.indexUsesCaseSensitive == false>lower</#if>
    (<#if joins.table.indexUsesRemoveAccents == true>f_unaccent(${joins.table.prefix}.${joins.table.joinOnField})</#if>)  =
    <#if joins.joinTable.indexUsesCaseSensitive == false>lower</#if>
    (<#if joins.joinTable.indexUsesRemoveAccents == true>f_unaccent(${joins.joinTable.prefix}.${joins.joinTable.joinOnField})</#if>)
  </#list>;
