
<#if mode.name() == "CREATE">

<#include "extensions.ftl">

CREATE ROLE ${myuniversity}_${mymodule} PASSWORD '${myuniversity}' NOSUPERUSER NOCREATEDB INHERIT LOGIN;
GRANT ${myuniversity}_${mymodule} TO CURRENT_USER;
CREATE SCHEMA ${myuniversity}_${mymodule} AUTHORIZATION ${myuniversity}_${mymodule};

CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.rmb_internal (
      id SERIAL PRIMARY KEY,
      jsonb JSONB NOT NULL
    );

insert into ${myuniversity}_${mymodule}.rmb_internal (jsonb) values ('{"rmbVersion": "${rmbVersion}", "moduleVersion": "${newVersion}!0"}'::jsonb);

-- rmb version ${rmbVersion}

</#if>

SET search_path TO ${myuniversity}_${mymodule},  public;

<#if scripts??>
  <#list scripts as script>
    <#if script.run == "before">
      <#if (script.isNewForThisInstall(version)) || mode.name() == "CREATE">
        <#if mode.name() != "CREATE">
-- Run script - created in version ${(script.fromModuleVersion)!0}
        </#if>
        ${script.snippet}
      </#if>
    </#if>
  </#list>
</#if>

SET search_path TO public, ${myuniversity}_${mymodule};

<#include "general_functions.ftl">

<#-- Loop over all tables that need updating / adding / deleting -->
<#list tables as table>

<#-- the table version indicates which version introduced this feature hence all versions before this need the schema upgrade-->
<#-- the from module version - if not set, is set to zero as it assumes that a version not set indicates to create the table always -->
<#if (table.isNewForThisInstall(version)) || mode.name() == "CREATE">

-- Previous module version ${version}
  <#if mode.name() != "CREATE">
-- Run upgrade of table: ${table.tableName} since table created in version ${(table.fromModuleVersion)!0}
  </#if>
  <#if table.mode != "delete">
    CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.${table.tableName} (
      ${table.pkColumnName} UUID PRIMARY KEY <#if table.generateId == true>DEFAULT gen_random_uuid()</#if>,
      jsonb JSONB NOT NULL
    );
  <#else>
    DROP TABLE IF EXISTS ${myuniversity}_${mymodule}.${table.tableName} CASCADE;
  </#if>

  <#if table.mode != "delete">
    <#-- if we are in delete table mode only the drop table casacade above is needed, skip everything else -->
    <#if table.withMetadata == true>
    <#-- add the two needed columns per table -->
    ALTER TABLE ${myuniversity}_${mymodule}.${table.tableName}
      ADD COLUMN IF NOT EXISTS creation_date timestamp WITH TIME ZONE,
      ADD COLUMN IF NOT EXISTS created_by text;
    <#else>
    ALTER TABLE ${myuniversity}_${mymodule}.${table.tableName}
      DROP COLUMN IF EXISTS creation_date CASCADE,
      DROP COLUMN IF EXISTS created_by CASCADE;
    </#if>

    <#if table.deleteFields??>
      <#list table.deleteFields as fields2del>
    UPDATE ${myuniversity}_${mymodule}.${table.tableName} SET jsonb = jsonb #- ${fields2del.fieldPath};
      </#list>
    </#if>

    <#if table.addFields??>
      <#list table.addFields as fields2add>
        <#if fields2add.defaultValue?is_string>
    UPDATE ${myuniversity}_${mymodule}.${table.tableName} SET jsonb = jsonb_set(jsonb , ${fields2add.fieldPath}, '"${fields2add.defaultValue}"', true);
        <#else>
    UPDATE ${myuniversity}_${mymodule}.${table.tableName} SET jsonb = jsonb_set(jsonb , ${fields2add.fieldPath}, ${fields2add.defaultValue?c}, true);
        </#if>
      </#list>
    </#if>

    <#include "indexes.ftl">

    <#include "foreign_keys.ftl">

    <#include "populate_id.ftl">

    <#if table.withMetadata == true>
      <#include "metadata.ftl">
    </#if>

    <#if table.withAuditing == true>
      <#include "audit.ftl">
    </#if>

    <#if table.customSnippetPath??>
      <#include table.customSnippetPath>
    </#if>
  </#if>
</#if>
</#list>

<#include "views.ftl">

SET search_path TO ${myuniversity}_${mymodule},  public;

<#if scripts??>
  <#list scripts as script>
    <#if script.run == "after">
      <#if (script.isNewForThisInstall(version)) || mode.name() == "CREATE">
        <#if mode.name() != "CREATE">
-- Run script - created in version ${(script.fromModuleVersion)!0}
        </#if>      
        ${script.snippet}
      </#if>
    </#if>
  </#list>
</#if>

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${myuniversity}_${mymodule} TO ${myuniversity}_${mymodule};
