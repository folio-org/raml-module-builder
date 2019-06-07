# RMB upgrading notes

These are notes to assist upgrading to newer versions.
See the [NEWS](../NEWS.md) summary of changes for each version.

* [Version 25](#version-25)
* [Version 20](#version-20)

## Version 25

* Remove any `"pkColumnName"`, `"generateId"` and `"populateJsonWithId"` entries in `src/main/resources/templates/db_scripts/schema.json`.
    * Hint: `grep -v -e '"pkColumnName"' -e '"generateId"' -e '"populateJsonWithId"' < schema.json > schema.json.new`, review, `mv schema.json.new schema.json`
* In Java files change cql2pgjson import statements:
    * old: `import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;`
    * old: `import org.z3950.zing.cql.cql2pgjson.FieldException;`
    * new: `import org.folio.cql2pgjson.CQL2PgJSON;`
    * new: `import org.folio.cql2pgjson.exception.FieldException;`
* In Java files replace any `Criteria.setValue`.
    * Consider using some org.folio.rest.persist.PgUtil method or some PostgresClient
      method that takes a CQL query or an id because those methods have
      optimizations that Criteria doesn't have.
    * If you still want to use Criteria please note that setVal does both quoting
      and masking.
    * old: `setValue(s)`  // prone to SQL injection
    * old: `setValue("'" + s + "'")`  // prone to SQL injection
    * old: `setValue("'" + s.replace("'", "''") + "'")`
    * new: `setVal(s)`
* In SQL code replace `_id` (or whatever you used as pkColumnName) by `id`.
* In SQL code remove `gen_random_uuid()` and generate the UUID before sending the query to the database, for example with Java's `UUID.randomUUID()`. This is needed because `gen_random_uuid()` produces different values on different nodes of a replicated database like Pgpool-II. Therefore we no longer support the extension `pgcrypto`.

## Version 20

RMB v20+ is based on RAML 1.0. This is a breaking change from RAML 0.8 and there are multiple changes that must be implemented by modules that upgrade to this version.

```
1. Update the "raml-util" git submodule to use its "raml1.0" branch.
2. MUST change 0.8 to 1.0 in all RAML files (first line)
3. MUST remove the '-' signs from the RAML
   For example:
        CHANGE:  - configs: !include... TO: configs: !include...
4. MUST change the "schemas:" section to "types:"
5. MUST change 'repeat: true' attributes in traits (see our facets) TO type: string[]
6. MUST ensure that documentation field is this format:
   documentation:
     - title: Foo
       content: Bar
7. In resource types change 'schema:' to 'type:'
   This also means that the '- schema:' in the raml is replaced with 'type:'
   For example:
          body:
            application/json:
              type: <<schema>>
8. Remove suffixes from key names in the RAML file.
   Any suffix causes a problem (even `.json`) when it is used to populate
   placeholders in the RAML file.
   Declare only types/schemas in RAML that are used in RAML (no need to declare types
   that are only used in JSON schema references).
   For example:
        CHANGE:
            notify.json: !include notify.json
        TO:
            notify: !include notify.json
        WHEN:
            "notify" is referenced anywhere in the raml
9. JSON schema references may use relative pathname (RMB will dereference them).
   No need to declare them in the RAML file.

10. The resource type examples must not be strict (will result in invalid json content otherwise)
        CHANGE:
            example: <<exampleItem>>
        TO:
            example:
                strict: false
                value: <<exampleItem>>
11. Generated interfaces do not have the 'Resource' suffix
    For example:
        ConfigurationsResource -> Configurations
12. Names of generated pojos (also referenced by the generated interfaces) may change
    For example:
        kv_configuration: !include ../_schemas/kv_configuration.schema
        will produce a pojo called: KvConfiguration

    Referencing the kv_configuration in a schema (example below will produce a pojo called Config)
    which means the same pojo will be created twice with different names.
    Therefore, it is preferable to synchronize names.
            "configs": {
              "id": "configurationData",
              "type": "array",
              "items": {
                "type": "object",
                "$ref": "kv_configuration"
            }
    This may affect which pojo is referenced by the interface - best to use the same name.
13. Generated methods do not throw exceptions anymore.
    This will require removing the 'throws Exception' from the implementing methods.
14. Names of generated methods has changed
15. The response codes have changed:
        withJsonOK -> respond200WithApplicationJson
        withNoContent -> respond204
        withPlainBadRequest -> respond400WithTextPlain
        withPlainNotFound -> respond404WithTextPlain
        withPlainInternalServerError -> respond500WithTextPlain
        withPlainUnauthorized -> respond401WithTextPlain
        withJsonUnprocessableEntity -> respond422WithApplicationJson
        withAnyOK -> respond200WithAnyAny
        withPlainOK -> respond200WithTextPlain
        withJsonCreated -> respond201WithApplicationJson

    Since RMB v23.3.0 PgUtil provides the methods deleteById, getById, post and put that
    automatically create the correct response.

    Note: For 201 / created codes, the location header has changed and is no longer a string
    but an object and should be passed in as:
      PostConfigurationsEntriesResponse.headersFor201().withLocation(LOCATION_PREFIX + ret)
16. Multipart formdata is currently not supported
17. Remove the declaration of trait "secured" auth.raml and its use from RAML files.
    It has been removed from the shared raml-util.
```
