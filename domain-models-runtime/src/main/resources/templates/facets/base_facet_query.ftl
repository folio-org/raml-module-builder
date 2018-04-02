with facets as (
    ${mainQuery}
     <#if ("0" != calculateOnFirst)>LIMIT ${calculateOnFirst} </#if>
 )
 ,
 count_on as (
    SELECT
      ${schema}.count_estimate_smart2(count(*) , ${calculateOnFirst}, '${countQuery}') AS count
    FROM facets
 )
 ,
 grouped_by as (
    SELECT
      <#list facets as facet>
        ${facet.fieldPath} as ${facet.alias},
      </#list>
      count(*) as count
    FROM facets
    GROUP BY GROUPING SETS (
      <#list facets as facet>
        ${facet.alias}<#sep>,
      </#list>
    )
 )
 ,
 <#list facets as facet>
  <#include "additional_facet_clause.ftl">,
 </#list>
ret_records as (
       select ${idField} as ${idField}, jsonb  FROM facets
       )
 <#list facets as facet>
  (SELECT '00000000-0000-0000-0000-000000000000'::uuid as ${idField}, jsonb FROM lst${facet_index + 1} limit ${facet.topFacets2return?c})
  <#sep> UNION </#sep>
 </#list>
  UNION
  (SELECT '00000000-0000-0000-0000-000000000000'::uuid as ${idField},  jsonb_build_object('count' , count) FROM count_on)
  UNION ALL
  (select ${idField} as ${idField}, jsonb from ret_records <#if limitClause??>${limitClause}</#if> <#if offsetClause??>${offsetClause}</#if>);
