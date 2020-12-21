SET search_path TO ${myuniversity}_${mymodule};

-- List of all indexes maintained by RMB
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.rmb_internal_index (
  name text PRIMARY KEY,
  def text NOT NULL,
  remove boolean NOT NULL
);
UPDATE ${myuniversity}_${mymodule}.rmb_internal_index SET remove = TRUE;

-- Collect all tables where we need to run ANALYZE
CREATE TABLE IF NOT EXISTS rmb_internal_analyze (
  tablename text
);

<#if mode.name() == "CREATE">
  <#include "uuid.ftl">
</#if>

<#include "general_functions.ftl">
<#include "rmb_internal_index.ftl">

<#if scripts??>
  <#list scripts as script>
    <#if script.run == "before">
      <#if (script.isNewForThisInstall(version)) || mode.name() == "CREATE">
        <#if mode.name() != "CREATE">
-- Run script - created in version ${(script.fromModuleVersion)!0}
        </#if>
        <#if script.snippetPath??>
          <#include script.snippetPath>
        <#elseif script.snippet??>
          ${script.snippet}
        </#if>
      </#if>
    </#if>
  </#list>
</#if>

DO $$
BEGIN
  -- use advisory lock to prevent "tuple concurrently updated"
  -- https://issues.folio.org/browse/RMB-744
  PERFORM pg_advisory_xact_lock(20201101, 1234567890);
  REVOKE ALL PRIVILEGES ON SCHEMA public FROM ${myuniversity}_${mymodule};
  REVOKE CREATE ON SCHEMA public FROM PUBLIC;
END $$;

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
      id UUID PRIMARY KEY,
      jsonb JSONB NOT NULL
    );
    <#if table.withAuditing == true>
    CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.${table.auditingTableName} (
      id UUID PRIMARY KEY,
      jsonb JSONB NOT NULL
    );
    </#if>
    -- old trigger name
    DROP TRIGGER IF EXISTS set_id_injson_${table.tableName} ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;
    -- current trigger name
    DROP TRIGGER IF EXISTS set_id_in_jsonb ON ${myuniversity}_${mymodule}.${table.tableName} CASCADE;
    CREATE TRIGGER set_id_in_jsonb BEFORE INSERT OR UPDATE ON ${myuniversity}_${mymodule}.${table.tableName}
      FOR EACH ROW EXECUTE PROCEDURE ${myuniversity}_${mymodule}.set_id_in_jsonb();
  <#else>
    DROP TABLE IF EXISTS ${myuniversity}_${mymodule}.${table.tableName} CASCADE;
    <#if table.auditingTableName??>
    DROP TABLE IF EXISTS ${myuniversity}_${mymodule}.${table.auditingTableName} CASCADE;
    </#if>
    -- drop function that updates foreign key fields
    DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.update_${table.tableName}_references();
    -- drop function that updates optimistic locking version
    DROP FUNCTION IF EXISTS ${myuniversity}_${mymodule}.${table.tableName}_set_ol_version() CASCADE;
  </#if>

  <#if table.mode != "delete">
    <#-- if we are in delete table mode only the drop table casacade above is needed, skip everything else -->
    <#if table.withMetadata == true>
    <#-- add the two needed columns per table -->
    ALTER TABLE ${myuniversity}_${mymodule}.${table.tableName}
      ADD COLUMN IF NOT EXISTS creation_date timestamp,
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

    <#include "metadata.ftl">

    <#include "optimistic_locking.ftl">

    <#if table.withAuditing == true>
      <#include "audit.ftl">
    </#if>

    <#if table.customSnippetPath??>
      <#include table.customSnippetPath>
    </#if>
  </#if>
<#else>
    <#-- The table has not changed, but we always check all its indexes and foreign keys
         because they may have changed. -->
    <#include "indexes.ftl">
    <#include "foreign_keys.ftl">
    
    <#-- Always check optimistic locking configuration -->
    <#include "optimistic_locking.ftl">
</#if>
</#list>

<#include "views.ftl">

<#if scripts??>
  <#list scripts as script>
    <#if script.run == "after">
      <#if (script.isNewForThisInstall(version)) || mode.name() == "CREATE">
        <#if mode.name() != "CREATE">
-- Run script - created in version ${(script.fromModuleVersion)!0}
        </#if>
        <#if script.snippetPath??>
          <#include script.snippetPath>
        <#elseif script.snippet??>
          ${script.snippet}
        </#if>
      </#if>
    </#if>
  </#list>
</#if>

-- Drop all indexes that schema.json no longer defines but had been defined by schema.json before.
DO $$
DECLARE
  aname TEXT;
BEGIN
  FOR aname IN SELECT name FROM ${myuniversity}_${mymodule}.rmb_internal_index WHERE remove = TRUE
  LOOP
    EXECUTE 'DROP INDEX IF EXISTS ' || aname;
  END LOOP;
END $$;

-- Fix functions calls with "public." in indexes https://issues.folio.org/browse/RMB-583
-- All functions (except functions of Postgres extensions) have been moved
-- from public schema into ${myuniversity}_${mymodule} schema.
-- https://github.com/folio-org/raml-module-builder/commit/872c1f80da4c8d49e6836ca9221f637dc5e7420b
DO $$
DECLARE
  version TEXT;
  i RECORD;
  newindexdef TEXT;
BEGIN
  SELECT jsonb->>'rmbVersion' INTO version FROM ${myuniversity}_${mymodule}.rmb_internal;
  IF version !~ '^(\d\.|1\d\.|2[0-8]\.|29\.[0-3]\.)' THEN
    -- skip this upgrade if last install/upgrade was made by RMB >= 29.4.x
    RETURN;
  END IF;
  FOR i IN SELECT * FROM pg_catalog.pg_indexes WHERE schemaname = '${myuniversity}_${mymodule}'
  LOOP
    newindexdef := regexp_replace(i.indexdef,
      -- \m = beginning of a word, \M = end of a word
      '\mpublic\.(f_unaccent|concat_space_sql|concat_array_object_values|concat_array_object)\M',
      '${myuniversity}_${mymodule}.\1',
      'g');
    IF newindexdef <> i.indexdef THEN
      EXECUTE 'DROP INDEX ' || i.indexname;
      EXECUTE newindexdef;
      EXECUTE 'INSERT INTO rmb_internal_analyze VALUES ($1)' USING i.tablename;
    END IF;
  END LOOP;
END $$;

-- For each table where we have created an index run ANALYZE to collect statistic about the new index.
-- PostgreSQL does not automatically do it: https://issues.folio.org/browse/FOLIO-2625
DO $$
DECLARE
  t TEXT;
BEGIN
  FOR t IN SELECT DISTINCT tablename FROM rmb_internal_analyze
  LOOP
    EXECUTE 'ANALYZE ' || t;
  END LOOP;
END $$;
TRUNCATE rmb_internal_analyze;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${myuniversity}_${mymodule} TO ${myuniversity}_${mymodule};

UPDATE ${myuniversity}_${mymodule}.rmb_internal
  SET jsonb = jsonb || jsonb_build_object(
    'rmbVersion', '${rmbVersion}',
    'moduleVersion', '${newVersion}',
    'schemaJson', $mainftl$${schemaJson}$mainftl$);
