-- This is run for any module, a removal of the schema and user for a specific tenant
-- There is no per module configuration here so a delete of tenant will always run the below

REVOKE ALL PRIVILEGES ON DATABASE postgres from ${myuniversity}_${mymodule};
DROP SCHEMA IF EXISTS ${myuniversity}_${mymodule} CASCADE;
DROP USER IF EXISTS ${myuniversity}_${mymodule};
