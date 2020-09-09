-- This is run for any module, a removal of the schema and user for a specific tenant
-- There is no per module configuration here so a delete of tenant will always run the below

DROP SCHEMA IF EXISTS ${myuniversity}_${mymodule} CASCADE;
DROP ROLE IF EXISTS ${myuniversity}_${mymodule};
