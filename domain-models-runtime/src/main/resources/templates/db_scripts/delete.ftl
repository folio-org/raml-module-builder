REVOKE ALL PRIVILEGES ON DATABASE postgres from ${myuniversity}_${mymodule};
DROP SCHEMA IF EXISTS ${myuniversity}_${mymodule} CASCADE;
DROP USER IF EXISTS ${myuniversity}_${mymodule};