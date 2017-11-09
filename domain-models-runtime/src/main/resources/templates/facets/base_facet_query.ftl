with facets as (
     SELECT
      <#list facets as facet>
        ${facet.fieldPath} as ${facet.alias},
      </#list>
        ${idField}
     FROM
      ${table}
     <#if where??>${where}</#if>
     <#if ("0" != calculateOnFirst)>LIMIT ${calculateOnFirst}</#if>
 )
 ,
 grouped_by as (
    SELECT count(*) as count4facets, * FROM facets
    GROUP BY GROUPING SETS (
      ${idField},
      <#list facets as facet>
        ${facet.alias}<#sep>,
      </#list>
    )
 )
 ,
 <#list facets as facet>
  <#include "additional_facet_clause.ftl">,
 </#list>

lst999 as (
       ${mainQuery}
       )
 <#list facets as facet>
  (SELECT <#if isCountQuery == true>count4facets, </#if> ${idField}, jsonb FROM lst${facet_index + 1} limit ${facet.topFacets2return})<#sep> UNION </#sep>
 </#list>
UNION ALL
(select <#if isCountQuery == true>count, </#if>${idField}, jsonb from lst999);
