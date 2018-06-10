<#-- used to create the indexes only, needed if indexes were dropped before a large batch load -->
<#list tables as table>
    <#include "indexes.ftl">
</#list>
