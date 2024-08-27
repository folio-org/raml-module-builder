
<#if mode.name() == "CREATE">

CREATE ROLE ${myuniversity}_${mymodule} PASSWORD '${myuniversity}' NOSUPERUSER NOCREATEDB INHERIT LOGIN;
GRANT ${myuniversity}_${mymodule} TO CURRENT_USER;
CREATE SCHEMA ${myuniversity}_${mymodule} AUTHORIZATION ${myuniversity}_${mymodule};

</#if>

ALTER ROLE ${myuniversity}_${mymodule} SET search_path = "${myuniversity}_${mymodule}";

<#include "extensions.ftl">

<#if mode.name() == "CREATE">

CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.rmb_internal (
      id SERIAL PRIMARY KEY,
      jsonb JSONB NOT NULL
    );

</#if>

<#-- Insert row if table is empty -->
INSERT INTO ${myuniversity}_${mymodule}.rmb_internal (jsonb)
SELECT '{"rmbVersion": "${rmbVersion}", "moduleVersion": "${version}"}'::jsonb
WHERE NOT EXISTS (SELECT * FROM ${myuniversity}_${mymodule}.rmb_internal);

CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.rmb_job (
      id UUID PRIMARY KEY,
      jsonb JSONB NOT NULL
    );


