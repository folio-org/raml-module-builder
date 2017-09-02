with facets as (
 SELECT
  <#list facets as facet>
    ${facet.fieldPath} as ${facet.alias},
  </#list>
  count(*) as cnt,
  count(id) OVER() AS count
     FROM
      ${table}    
      ${where}
    GROUP BY GROUPING SETS (
      id,
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
  (SELECT count, jsonb FROM lst${facet_index + 1} limit ${facet.topFacets2return})<#sep> UNION </#sep>
 </#list>
UNION ALL 
(select count, jsonb from lst999 ${limitClause} );