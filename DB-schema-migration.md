# DB Schema migration for module update

## Introduction

This documentation includes information about possible strategies regarding DB schema migration that can be used under a module's version update.

For RMB based modules a DB migration will be done automatically based on the content of the schema.json provided within the module. Indeed there are exceptions, but each such case must be considered separately.

DB Schema migration script can only support forward compatibility. To rollback the DB, please check Database rollback commands topic https://docs.liquibase.com/commands/home.html and https://docs.liquibase.com/tools-integrations/maven/commands/maven-rollback.html?Highlight=backward

## Recommendations for the creation and maintenance of the schema.json file

* For each table in the “tables” section provide “fromModuleVersion” value, even if this table has not been added in this version and already exists in the database. It will eliminate the generation and running of unnecessary SQL statements for the table. You can provide a value for the first production version for example
    ```
      {
        "tableName": "loan_type",
        "fromModuleVersion": "mod-inventory-storage-1.0.0",
        ...
    ```

* In case you need to perform additional actions for a table, use “customSnippetPath” to provide a relative path to a file with custom SQL commands for this specific table. Statements from this file will be run at the very end of the process of updating or changing this table.
    ```
    {
     "tableName": "instance_source_marc",
     "fromModuleVersion": "mod-inventory-storage-1.0.0",
     "withMetadata": true,
     "customSnippetPath": "instanceSourceMarc.sql"
    }
    ```

* Use “scripts” section to run custom SQLs before table/view creation/updates and/or after all tables/views have been created/updated. You can omit a schema name in your SQL statements because “SET search_path TO public, <your_module_schema>” statement will be executed beforehand.

### “fromModuleVersion” notes

fromModuleVersion - this field indicates the version which the table was created/updated in. The same for scripts.

If no fromModuleVersion is provided the SQL runs on each upgrade. This is reasonable if the SQL runs fast and is idempotent, for example ```CREATE OR REPLACE FUNCTION``` and you want to avoid the error-prone fromModuleVersion value maintenance needed when changing the function.

SemVer.java from Okapi is used under the hood for comparison. So, for example, mod-inventory-storage-1.0.0-SNAPSHOT.265 is a valid version number.

This is a result of a comparison between mod-inventory-storage-1.0.0-SNAPSHOT.265 and mod-inventory-storage-1.0.0-SNAPSHOT:
```
Display parameters as parsed by Maven (in canonical form) and comparison result:
    mod-inventory-storage-1.0.0-SNAPSHOT.265 == mod-inventory-storage-1-snapshot.265
    mod-inventory-storage-1.0.0-SNAPSHOT.265 > mod-inventory-storage-1.0.0-SNAPSHOT
    mod-inventory-storage-1.0.0-SNAPSHOT == mod-inventory-storage-1-snapshot
```

#### Example 1

* The current version of a module enabled for a tenant is mod-inventory-storage-SNAPSHOT.274
* The version of a module that will be enabled for a tenant (it is provided in the ModuleDescriptor.json) is mod-inventory-storage-SNAPSHOT.275
* The version specified in the “fromModuleVersion” in schema.json is mod-inventory-storage-1.0.0

The comparison will be done between ```mod-inventory-storage-1.0.0``` and ```mod-inventory-storage-SNAPSHOT.274```

The result is ```mod-inventory-storage-1.0.0 > mod-inventory-storage-SNAPSHOT.274``` so a script/table will be applied/created or changed.

#### Example 2

* The current version of a module enabled for a tenant is mod-inventory-storage-1.0.0-SNAPSHOT.274
* The version of a module that will be enabled for a tenant (it is provided in the ModuleDescriptor.json) is mod-inventory-storage-1.0.0-SNAPSHOT.275
* The version specified in the “fromModuleVersion” in schema.json is mod-inventory-storage-1.0.0

The comparison will be done between ```mod-inventory-storage-1.0.0``` and ```mod-inventory-storage-1.0.0-SNAPSHOT.274```

The result is ```mod-inventory-storage-1.0.0 > mod-inventory-storage-1.0.0-SNAPSHOT.274``` so a script/table will be applied/created or changed as well.

### DoD statement for DB schema migration

It is proposed that these points should be added to DoD of teams who work with RMB-based modules.
* Schema.json is changed correctly to reflect new entities
* Upgrade and downgrade scripts and sample and reference data is updated to match the feature or schema change.

It is likely that for some changes there are no compensation scripts because the change cannot be compensated or the effort for implementing the compensation is too high.
Therefore the release notes should state for which versions compensations scripts are available and how to run them.

## Schema migration using  the same DB and schema for a module

A general approach for roll out a new version for RMB based modules (platform-core, platform-complete) using the same DB schema is as follows:
1. Prerequisites:
    - A new version of a module is built and an artifact (docker image for this version) is published into a repository.
2. Register a module descriptor for the new version of the module using POST `/_/proxy/modules`
3. Deploy a new version of the module using POST `/_/discovery/modules`
4. A Tenant decides to update the module’s version.
5. Install the new version of the module for a tenant using POST `/_/proxy/tenants/{tenant}/install`
6. Check the log of the module for any errors or issues.

### Use cases for DB changes

1. **Add a new table**
    - It is a backward-compatible change.
    - Change schema.json appropriately, add a new table object into “tables” section
2. **Add a new JSON property**
    - Potentially this is a backward-incompatible change. If the new property is required and a default value will be provided it is a backward-incompatible change for sure.
    - If there should be a default value
        - provide a custom SQL script which fills in that for all rows in a table.
        - specify the script’s file name for “customSnippetPath” for the table in schema.json.
        - change “fromModuleVersion” for the table to a correct value.
    - Provide a compensation script. This must remove new property values from all rows in a table. For now, it will be run manually by the administrator in case there are some problems during a module’s update rollout.
3. **Rename a JSON property**
    - It is a backward-incompatible change
    - Provide a custom SQL script that changes the property name in all rows in a table.
        - specify the script’s file name for “customSnippetPath” for the table in schema.json.
        - change “fromModuleVersion” for the table to a correct value.
    - Provide a compensation script. This must return the property name to a previous one for all rows in a table. For now, it will be run manually by the administrator in case there are some problems during a module’s update rollout.
4. **Change a data type of a JSON property**
    - It can be either a backward-compatible or backward-incompatible change.
    - If the data type changes from less broad to a wider one, for example from Integer to String, this is a backward-compatible change and there is nothing to do here. Values for the property will be cast to the new data type automatically.
    - Otherwise, the solution is almost the same as for the renaming.
        - Provide a custom SQL script that changes property values in all rows in a table to be compliant with the new data type.
        - specify the script’s file name for “customSnippetPath” for the table in schema.json.
        - change “fromModuleVersion” for the table to a correct value.
     - Provide a compensation script. This must ensure that property values are compliant with an old data type for all rows in a table. For now, it will be run manually by the administrator in case there are some problems during a module’s update rollout.
5. **Remove a JSON property**
    - It is a backward-incompatible change by its nature.
    - Provide a custom SQL script that removes the property and its values from all rows in a table. It is recommended that this script preserves data, that will be deleted, in a separate table beforehand.
        - specify the script’s filename for “customSnippetPath” for the table in schema.json.
        - change “fromModuleVersion” for the table to a correct value.
    -  Provide a compensation script. This must return the property and its values for all rows in a table. Preserved data should be used for that. For now, it will be run manually by the administrator in case there are some problems during a module’s update rollout.

## Schema migration using DB backup

The main difference between this strategy and previous one is that a backup of a DB schema for a module must be created before the update.
1. Prerequisites:
    - A new version of a module is built and an artifact (docker image for this version) is publisher into a repository.
2. Register a module descriptor for the new version of the module using POST `/_/proxy/modules`
3. Deploy a new version of the module using POST `/_/discovery/modules`
4. A Tenant decides to update the module’s version.
5. A maintenance period must be started here so users can't work with the FOLIO applications that uses this module.
6. Create a dump of the DB schema used by this module for a tenant.
5. Install the new version of the module for a tenant using POST `/_/proxy/tenants/{tenant}/install`
6. Check the log of the module for any errors or issues.

All the use cases for DB changes remain valid and must be taken into consideration except that compensation scripts are not needed in this scenario.

In case of any issues or errors during a module’s update, all we need to do is to restore the schema's backup and  enable the previous version of the module for the tenant.
But pay attention that all changes done after the migration are lost.

## Schema migration using DB cloning

Another option is to clone a DB schema used by a module for a tenant for the next version of the module. At the high-level, the procedure is as follows.
1. Prerequisites:
    - A new version of a module is built and an artifact (docker image for this version) is published into a repository.
2. A platform DevOps registers a module descriptor for the new version of the module using POST `/_/proxy/modules`
3. A platform DevOps creates a new instance of the database and performs all steps required for initialization (he create users, roles, grants permissions, etc.)
4. A platform DevOps deploys the new version of the module using POST `/_/discovery/modules`. New credentials for the new DB instance created in the previous step must be provided for the deployment.
5. A Tenant decides to update the module’s version.
6. A platform DevOps exports module’s schema from the DB instance used by the “current” version of the module and imports it into the new DB instance created for the new version of the module.
7. This process can be quite time consuming so:
    - The “current” version of the module must be fully operational during the whole time while data is being copied.
    - The timestamp when a schema export was started must be registered.
8. When the data copying process is completed, a platform's DevOps installs the new version of the module for a tenant using POST `/_/proxy/tenants/{tenant}/install`
    - It is important to emphasize that during the module’s version update neither old nor new version of the module will be available for the tenant.
9. The last but the most complex step is to transfer data that was created in the DB schema of the old version during data transfer. Main issues are
    - To determine the dataset that was created after the start of the data transfer process.
    - If needed, to transform that data to the JSON schemas that are valid for the new version of the module. Indeed, it will require additional effort to find an appropriate solution. For example, we can check for logical replication capabilities in PostgreSQL (https://www.postgresql.org/docs/current/logical-replication.html) or the pglogical extension (https://www.2ndquadrant.com/en/resources/pglogical/pglogical-docs/).
    - Certainly, the simplest option here is to perform module's version update at the maintenance period when tenant's users do not work with the platform. It eliminates all these additional tricky actions necessary to not lose data.

Certainly, all the use cases for DB changes remain valid and must be taken into consideration, except that compensation scripts are not needed in this scenario.

In case of any issues or errors during a module’s update, all we need to do is to enable the previous version of the module for the tenant. But pay attention that all changes done after the migration are lost.
