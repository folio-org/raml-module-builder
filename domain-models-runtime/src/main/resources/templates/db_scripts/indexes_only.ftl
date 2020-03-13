<#-- used to create the indexes only, needed if indexes were dropped before a large batch load -->
UPDATE ${myuniversity}_${mymodule}.rmb_internal_index SET def = '';
<#list tables as table>
    <#include "indexes.ftl">
</#list>
