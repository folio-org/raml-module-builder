## 29.2.2 2020-01-09

* [RMB-550](https://issues.folio.org/browse/RMB-550) Index recreation on module upgrade

## 29.2.1 2020-01-08

 * RMB-549 Mismatch lower() for compound index (keyword searches slow
   in mod-inventory-storage)

## 29.2.0 2020-01-06

 * RMB-498 Truncate b-tree string for 2712 index row size
 * RMB-533 Performance: Fix lower/f_unaccent usage by checking all 5
   index types
 * RMB-532 Criterion get does not select id (only jsonb)
 * RMB-536 Missing f_unaccent for compound fields full text index
 * RMB-537 f_unaccent single quote fullText tsquery sql injection
 * RMB-540 Unique index must not truncate for 2712 max index byte size
 * RMB-542 Implement streamGet with CQLWrapper as parameter
 * RMB-541 contributors and identifiers search perf regression with
   RMB 29.1.3/29.1.4

## 29.1.0 2019-12-02

 * RMB-529 Upgrade Vert.x 3.8.4 fixing Netty HTTP request smuggling
 * RMB-471 Fix ddlgen/Index.java StringBuilder code smells
 * RMB-486 Use indexed id fields in foreign tables, such as
   `holdingsRecords.permanentLocationId==abc*` from mod-inventory-storage
 * RMB-487 support also field[*] notation
 * RMB-404 Use org.folio.okapi.common.SemVer rather than copying Apache
   Maven code
 * RMB-506 Estimate hit counts
 * RMB-522 TenantLoading in sequence - rather than parallel
 * RMB-523 TenantLoading Retry with 422 for PUT/POST attempt
 * RMB-524 TenantLoading does not work on Windows (path problem)
 * RMB-528 schema.json's fromModuleVersion messy and buggy

## 29.0.0 2019-11-20

 * Update jackson to 2.10.1
 * RMB-518 Extract index2sqlText and index2sqlJson for reuse
 * RMB-516 Rename Cql2SqlUtil.hasCqlWildCardd to hasCqlWildCard
 * RMB-514 Update to aspectj 1.9.4 and aspectj-maven-plugin 1.11
 * RMB-511 Use log4js MDC class, rather than slf4j
 * RMB-510 Breaking change: POST /_/tenant requires JSON body with module_to
 * RMB-509 POST /_/tenant returns invalid JSON: "[ ]"
 * RMB-508 Integration tests for /_/tenant with X-Okapi-Request-Id
 * RMB-507 TenantAPI POST NullPointerException if tenant already exists
 * RMB-497 Replace JSQL parser by using SqlSelect from CQL2PgJSON

## 28.1.0 2019-11-06

 * RMB-504 Jackson-* version 2.10.0, fixes jackson-databind security
 * RMB-474 Support 'field[*].subfield' syntax in multiFieldNames
 * Mention Vert.x 3.8 Future/Premise changes in upgrading.md
 * Fix upgrading wording for fullText defaultDictionary

## 28.0.0 2019-10-30

 * RMB-433 Fix saveBatch must not overwrite existing id
 * RMB-492 Remove unnecessary JSQLParserException error log if query
 * RMB-493 Fix CQLWrapper.addWrapper not left associative as it should be
   was parsed successfully
 * RMB-496 connectionReleaseDelay: Use 1 minute as default, add config

## 27.1.2 2019-10-23

 * RMB-485: RMB modules leak PostgreSQLConnection objects
 * RMB-476: PgUtil.post: Return after-trigger value (INSERT ... RETURNING jsonb)
 * RMB-482: Dropwizard metrics disabling
 * RMB-427: StatsTracker not being synchronized can fail PostgresClient operations when run in parallel
 * RMB-469: "multiFieldNames": "firstName , lastName" fails on spaces around the comma
 * RMB-470: "multiFieldNames" fail on nested fields (subfields) like proxy.personal.firstName
 * RMB-462: fullText / defaultDictionary not used in queries

## 27.1.1 2019-09-25

 * RMB-479 mvn install leaves embedded postgres running
 * RMB-478 RMB echoes all headers

## 25.0.2 2019-09-25

 * RMB-478 RMB echoes all headers

## 27.1.0 2019-09-24

See also [27.1 upgrading instructions](doc/upgrading.md#version-271).
 * RMB-444 Documentation: Name of generated .java files (resource, model)
 * RMB-385 add 'queryIndexName' to schema.json and allow compound indexes
 * RMB-421 422 JSON error message for PgUtil
 * RMB-457 option to enable: analyze explain queries
 * RMB-466 drop CQL2PgJSON view constructor
 * RMB-464 Upgrade to Vert.x 3.8.1
 * RMB-468 Foreign key field index
 * RMB-477 jackson-databind 2.9.10 fix CVE-2019-16335, CVE-2019-14540

## 27.0.0 2019-08-26

Breaking changes:
 * RMB-451 add "targetPath" to disambiguate multi-table joins
 * RMB-452 make tableAlias and targetTableAlias explicit properties
 * RMB-318 Database config: Drop deprecated dot (db.port) variables, use underscore (DB_PORT)
 * RMB-460 Add 'snippetPath' to 'scripts' in schema.json
 * RMB-445 rename HttpStatus.HTTP_VALIDATION_ERROR to HttpStatus.HTTP_UNPROCESSABLE_ENTITY for 422

Bug fixes, new features:
 * RMB-200 Single quote SQL Injection in PostgresClient.update(table, updateSection, ...)
 * RMB-459 Populate metadata for reference data/sample data
 * RMB-460 Add 'snippetPath' to 'scripts' in schema.json

## 26.4.0 2019-08-12

 * RMB-438 contributor search returns no results when name
   contains "-"
 * RMB-432 fulltext: word splitting and punctuation removal

## 26.3.0 2019-08-07

 * RMB-443 HttpStatus.java: Explain FOLIO's 400 and 422 status code usage
 * RMB-442 Fix security vulnerabilities reported in
   jackson-databind >= 2.0.0, < 2.9.9.2
 * RMB-440 Skip logging GIN index missing if b-tree index can be used
 * RMB-439 Remove "-" token for full search to unblock CIRCSTORE-138
 * RMB-437 Fix queries with facet fail
 * RMB-436 Log both CQL and SQL within the missing index line
 * RMB-435 CVE-2019-12814 jackson-databind security vulnerability
 * RMB-411 Implement poLine.id = * style cql for subquery
 * RMB-395 Aupport nested sub queries across multiple tables
 * RMB-430 Audit table PK (id) unique constraint violation on
   concurrent queries

## 26.2.0 2019-07-11

 * RMB-417 Optimize search for array relation modifiers
 * RMB-422 Fix Invalid SQL for array modifiers
 * RMB-426 Consider also arraySubfield as relation modifier (with or
   without value)
 * RMB-429 Fix fulltext mismatch WRT accents. Note that this affects
   schema generation, so tenant data should be re-generated to ensure
   good performance.

## 26.1.0 2019-07-04

 * RMB-419 Fix syntax error at or near "." because table name is reserved word
 * #460 Bug fix: Reject foreign table processing if current table is unknown
 * #462 Bug fix: Do auditingSnippet null tests separately for declare and
   statement

## 26.0.0 2019-07-03

 * RMB-380 RMB-416 RMB-418 CQL extension: Implement /@ modifier for searching
   array-of-objects fields.
 * RMB-387 RMB-391 CQL extension: Implement foreign key joins (two tables).
 * RMB-397 Bug fix: Regression with $ref paths
 * RMB-400 Bug fix: PgUtil should return 400 (not 500) on invalid UUID id
 * RMB-401 Bug fix: PgUtil.post: Object or POJO type for entity
   parameter of respond201WithApplicationJson
 * RMB-402 RMB-414: Major change: Restructured audit (history) table
 * RMB-405 Bug fix: PostgresClient.save: Create random UUID if id is missing

## 25.0.0 2019-06-07

There are several breaking changes, see [upgrading instructions](doc/upgrading.md#version-25).

 * RMB-199 Fixed Single quote SQL Injection in
   `PostgresClient.delete(table, pojo, handler)`
 * RMB-376 Merge CQL2PG into RMB. Project cql2pgjson-java is now a
   subject part of RMB. This should not impact users of RMB.
 * RMB-379 Remove `Critiera.setValue` and replace it with `Criteria.setVal`
   to fix wrong Criteria value masking results in SQL Injection. This is
   major change.
 * RMB-125 Fix no way to indicate a removal of the metadata trigger in
   the schema.json
 * RMB-277 Always 'id' for primary key name, drop configuration option.
   This is a major change.
 * RMB-346 Drop the `populateJsonWithId` option. This is a major change.
 * RMB-347 Remove `gen_random_uuid()` as it fails in replication
   environments. The `generateId` option is removed from schema.json.
   This is major change.
 * RMB-375 Fix AnnotationGrabberTest not executed
 * RMB-377 jackson-databind 2.9.9: Block one more gadget type (CVE-2019-12086)
 * RMB-378 Update jersey to 2.28, fixing security issues
 * RMB-383 `PgUtil.deleteById`: make it return 400 on foreign key violation
 * RMB-384 Fix Generated models might have "readOnly" properties which
   are not in schema.
 * RMB-392 Move CQL2PgJSON.java from `org.z3950.zing.cql.cql2pgjson` to
   `org.folio.cql2pgjson` package . This is major change.
 * RMB-396 mask ) ] } in regexp to prevent SQL injection

## 24.0.0 2019-04-25

 * RMB-368 Update RMB for non-schema usage and CQLPG-90
   CQL2PgJSON constructors removed:
```
     CQL2PgJSON(Map<String,String> fieldsAndSchemaJsons)
     CQL2PgJSON(String field, String schemaJson)
     CQL2PgJSON(Map<String,String> fieldsAndSchemaJsons, List<String> serverChoiceIndexes)
```
   Public Criteria removes:
```
     Criteria(String schema)
     boolean isJSONB()
     String getArrayField()
     void setArrayField(String arrayField)
     String getForceCast()
     Criteria setForceCast(String forceCast)
```
 * PostgresClient.streamGet: use Void for stream async result handler
 * RMB-357 Increase code coverage of PostgresClient
 * RMB-352 Increase code coverage of PgUtil

## 23.12.0 2019-04-05

 * RMB-350 PgUtilIT not tested
 * RMB-331 Tenant upgrade failed if older version has no db schema
 * RMB-335 Signal stream closed (prematurely)
 * RMB-343 Get rid of " WARNING Problem parsing x-okapi-token header" message
 * RMB-345 Document TenantLoading and proper behavior of CRUD services
 * RMB-349 Do not spawn postgres-runner when using -Dmaven.test.skip=true
 * RMB-351 Enable Junit4 + Junit5 and make it work in Eclipse
 * RMB-354 Add PostgresClient.saveBatch variant with SQLConnection parameter
 * RMB-355 Facets broken in RMB 22 (and later)

## 23.11.0 2019-03-14

 * RMB-342 TenantLoading / empty list for empty directory
 * RMB-341 Log CQL and generated SQL WHERE clause together

## 23.10.0 2019-03-12

 * RMB-339/RMB-340 More facilities for TenantLoading
 * RMB-334 Add option to generate toString, hashCode and equals
   methods in generated classes
 * Enable JUnit5 in Eclipse
 * Minimum Java version runtime check

## 23.9.0 2019-03-01

 * RMB-338 TenantLoading: allow property of JSON Object to be set
 * Generated HTTP client now sets X-Okapi-Url header
 * TenantLoading: handle null TenantAttributes

## 23.8.0 2019-02-27

 * RMB-337 Extend TenangLoading API

## 23.7.0 2019-02-25

 * RMB-330 Gin index lost lower and f_unaccent functions
 * RMB-329 TenantLoading API

## 23.6.0 2019-02-18

 * RMB-332 Move BooksDemoAPI impl to test
 * Update cql2pgjson from 3.0.3 to 3.1.0 CQLPG-76 CQLPG-86

## 23.5.0 2019-01-29

 * RMB-324 Fix No Content-Type for most errors returned by RMB
 * RMB-325 Make /_/jsonSchemas to return schemas from subfolders
 * RMB-326 Fix Upload: complete state set before end-of-stream
 * Document headers streamed_id and complete

## 23.4.0 2019-01-16

 * RMB-304 PostgresClient.get(txConn, sql, params, replyHandler) for SELECT in a transaction
 * RMB-305 Add support for DISTINCT ON
 * RMB-310 XML handling different when going to RAML 1.0 / RMB 20+
 * RMB-312 Integration tests fails: too many open files
 * RMB-313 NullPointerException PostgresClientIT.tearDownClass on Windows
 * RMB-315 Fix security vulnerabilities in jackson-databind >= 2.9.0, < 2.9.8
 * RMB-322 Add offset and limit getter in CQLWrapper

## 23.3.0 2018-12-14

 * RMB-162 Simplify PostgresClient.getInstance
 * RMB-185 Nicer constructor for the CQLWrapper
 * RMB-288 StorageHelper post put deleteById getById
 * RMB-293 executeTransParamSyntaxError: IllegalStateException: Test
          already completed
 * RMB-308 enum values not matched
 * RMB-311 Validation of URI parameters
 * Update to cql2pgjson 3.0.3

## 23.2.0 2018-11-29

 * RMB-296 Tenant init for loading reference data
 * RMB-299 simplify embedded postgres start
 * RMB-300 Optional leading slash for ResourceUtil::asString
 * RMB-303 Remove org.junit.Assert dependency from UtilityClassTester
 * RMB-306 Add support for application/vnd.api+json content type for POST requests
 * RMB-307 Type check for query parameters broken

## 23.1.0 2018-11-08
 * RMB-297 Several connection.close() missing - totalCount calculation hangs after 5-12 errors.
 * RMB-282 Use description fields in RAML JSON schemas.
 * RMB-290 Document why environment variables with dots/periods are deprecated.
 * RMB-291 Exclude android, fix jzlib compression bug.
 * RMB-292 Enable compression of HTTP traffic in RestVerticle.
 * RMB-294 Broken maven-surefire-plugin.
 * RMB-295 Fix broken link to schema.json.example.json.

## 23.0.0 2018-10-18
 * RMB-284 id="foo\*" search results in QueryValidationException: CQL: Invalid UUID foo\*.
 * RMB-18 The "local apidocs" for "admin" and "jobs" has extra "/v1/" in path.
 * RMB-275 Windows backslash: Illegal character in opaque part at index 7: file:C:\Users\...
 * RMB-279 Fix false positive password vulnerability warning (sonarqube/sonarlint).
 * RMB-280 Provide a POM snippet to configure local apidocs.
 * RMB-281 Update vert.x libraries to fix security vulnerabilities (CVE-2018-12537)
 * RMB-255 Add streaming support to RMB.

## 22.1.0 2018-10-16
 * RMB-265 Allow recursion in JSON schema references (loops).
 * RMB-274 Implement PostgresClient::save(sqlConnection, table, id, entity, replyHandler).

## 22.0.1 2018-10-14
 * RMB-273 CQL2PG v3.0.1: Fix validating of array field names.

## 22.0.0 2018-10-13
 * RMB-272 Update cql2pg-json to version 3.0.0:
   * CQLPG-63 Drop field name to full field path resolution. This is a breaking change,
     using the unambiguous suffix "email" no longer resolves to "personal.email".
   * CQLPG-61 Equals empty is not same as equals star. Matching "*" means all records,
     matching "" means all records where that field is defined. This is a breaking change.
   * CQLPG-58 Performance: Convert id searches using '=' or '==' into primary key _id searches.
     This is a breaking change because it restricts the operators allowed for id and assumes
     that id is a UUID.
 * RMB-271 Also accept windows \r\n line endings in unit test.
 * RMB-256 Unit tests for PostgresClient.doGet.
 * RMB-257 Unit tests for PostgresClient.processResult.
 * RMB-289 private class TotaledResults: total needs to be Integer to afford null.
 * RMB-268 Remove finished MD creation code.
 * RMB-261 Skip all tests in PostgresClientIT on Windows.
 * RMB-262 Disable warning "Overriding managed version 3.5.1 for
   vertx-mysql-postgresql-client.
 * RMB-243 Add PostgresClient.execute(...) with SQL placeholders/parameters.
 * RMB-258 Drop IOException of ResourceUtils.resource2String.
 * RMB-230, RMB-254 PostgresClient: init moduleName, add getTenantId() and getSchemaName().

## 21.0.4 2018-10-04
 * RMB-266 Fix path parameters reversed when calling handler

## 21.0.3 2018-10-01
 * RMB-259 Windows compile failure - schema files.

## 21.0.2 2018-09-28
 * RMB-251 Add tests for missign query returns 500 Internal Error.
 * RMB-250 Fix fulltext search: stop words, trailing space and *.
 * RMB-249 Default to the 'simple' directory for fulltext searches.

## 21.0.1 2018-09-07
* RMB-245 Fix Invalid path for client generator

## 21.0.0 2018-09-06

* RMB-213 Commence doc directory, move upgrading notes, tidy for RMBv20+
* RMB-237 Do we need to bundle twitter.raml, github.raml, .. in fat jar?
* RMB-238 Extend PostgresClient.getById with POJO (change of API)
* RMB-239 Fix apidocs of RMB does not support RAML 1.0

## 20.0.0 2018-08-31

* RMB-221 Single quote SQL Injection in PostgresClient.saveBatch(table, list, handler)
* RMB-231 Errors in mod-inventory-storage when upgrading RMB from 19.1.5 to 19.3.1
* RMB-1 Specifying a RAML that only uses GET fails with RMB
* RMB-44 use generics for PostgresClient.get(...) and .join(...)
* RMB-109 RAML 1.0 support: use raml-for-jax-rs rather than obsolete raml-jaxrs-codegen
* RMB-149 Security update PostgreSQL 10.3 CVE-2018-1058 search_path
* RMB-174 mvn install leaves a postgres process running
* RMB-186 HTTP_ACCEPTED = 202, HTTP_OK = 200
* RMB-191 Reenable JUnit 5
* RMB-192 Add AdminAPI.postAdminImportSQL error reporting
* RMB-193 Add RestVerticle stacktrace error reporting
* RMB-195 Stacktrace logging on exception in AdminAPI
* RMB-202 Create small RMB example
* RMB-203 $ref should follow the JSON schema spec
* RMB-206 Add PostgresClient.getById
* RMB-207 Upgrade to maven-compiler-plugin 3.8.0
* RMB-225 PostgresClient: Replace "Object conn" by "Future<SQLConnection> conn"
* RMB-227 raml-cop rejects jobs.raml of RMB
* RMB-229 PostgresClientTransactionsIT hangs on old database
* RMB-234 Do NOT create tsvector indexes with lowercase/unaccent

## 19.3.1 2018-08-01

* Fix regression caused by RMB-176

## 19.3.0 2018-08-01

* RMB-176 Support index for multiple properties in declarative schema

## 19.2.0 2018-07-16

* RMB-184 new "fulltext" index type in schema.json
* RMB-183 add ability to set the result set distinct

## 19.1.5 2018-07-05

* RMB-179 net.sf.jsqlparser does not support "IS TRUE" and "a@>b"
* RMB-178 parseQuery throws StackOverflowError for deeply nested SQL query
* RMB-181 Rewrite getLastStartPos for reducing stack size

## 19.1.4 2018-07-02

* RMB-154 Investigate postgresql connection pool

## 19.1.3 2018-06-10

* RMB-167 consistently use runOnContext for DB operations
* RMB-172 add /admin api to drop , create indexes per module

## 19.1.2 2018-06-03

* RMB-168 PostgresClient makes a wrong callback

## 19.1.1 2018-05-23

* RMB-163 Fail to find API implementation due to subtle class loading order difference
* RMB-166 Reduce stack size for SQL queries

## 19.1.0 2018-04-24

* RMB-136: Default paging in storage modules prevents operations on entire contents
* RMB-138: SchemaDereferencer: Support name to name.schema resolution
* RMB-143: Version update: vertx 3.5.1, jackson 2.9.4
* RMB-144: Security update PostgreSQL 10.2
* RMB-146: Add support for faceting on fields in arrays
* RMB-148: Configure implementation specific paging limits per endpoint
* RMB-150: High number of facets seems to cause error
* RMB-151: AspectJ not running when recompiling
* RMB-152: `/_/tenant` DELETE: consider to use 4xx over 500 if tenant does not exist
* RMB-153: Add folio/util/StringUtil.urlencode(String)
* RMB-156: Allow saving base64 encoded data as a jsonarray
* RMB-157: Helper for error handling
* RMB-160: allow registering a custom deserializer to the static object mapper
* FOLIO-1202: improve README raml section
* FOLIO-1179: Fix dev URLs
* FOLIO-1187: Add lint-raml-cop.sh

## 19.0.0 2018-03-01

* RMB-140: Move to v19, 18.0.1 requires entry in pom

        <plugin>
          <groupId>org.codehaus.mojo</groupId>
          <artifactId>build-helper-maven-plugin</artifactId>
          <version>3.0.0</version>
          <executions>
            <execution>
              <id>add-raml-jaxrs-source</id>
              <phase>generate-sources</phase>
              <goals>
                <goal>add-source</goal>
              </goals>
              <configuration>
                <sources>
                  <source>${project.build.directory}/generated-sources/raml-jaxrs</source>
                </sources>
              </configuration>
            </execution>
          </executions>
        </plugin>

## 18.0.1 2018-02-28

* RMB-130: Use target/generated-sources for generated code
* RMB-135: Parse version from Module ID in `module_{from,to}`
* RMB-137: Move JsonPathParser.main() to JsonPathParserTest; use JUnit
* RMB-139: Add transaction support for get() and delete()

## 18.0.0 2018-02-19

* RMB-133: start, rollback and end transactions should use a handler of type `handler<asyncresult<object>>`
* RMB-132: Allow update() method to be called within a transaction.
* RMB-131: PostgresClient: asynchronous functions must not throw exceptions
* RMB-128: Upgrade to postgres v10

## 17.0.0 2017-01-17

* RMB-119: CQL's default relation "=" now uses "adj" relation for strings (was "all")
* CQLPG-30, CQLPG-31: Fix number handling in CQL queries
* RMB-124: Lift time limit of PostgresClientMultiVertxIT
* RMB-122: Setting caseSensitive= true, removeAccents = false in regular index declaration corrupts generated schema
* RMB-121: mod-configuration doesn't build with Postgres 10
* RMB-120: README should suggest to use UUID, not SERIAL
* RMB-119: CQL's default relation "=" should use "adj" for a string
* RMB-118: add a `/admin/db_maintenance` api
* RMB-115: jaxrs test cleanup results in compile failure
* RMB-114: Security update postgres-embedded:2.6
* RMB-113: Dereference raml-util if ramls directory does not exist
* RMB-112: Use logger rather than standard output/standard error
* RMB-104: optimize join views + add support for more than 2 tables
* RMB-98: Foreign key trigger created in public schema and not in tenant's schema

## 16.0.3 2017-12-19

* RMB-103: Schema dereferencing requires a ramls/ directory with the raml and schema files.
* RMB-106: Set Locale.US, use Logger, split into separate tests.
* importSQL tenant sensitive
* RMB-110: Lift time limit in PostgresClientIT.parallel(...)
* hack to handle simple text response for rmb http client
* ResourceUtil.asString(...): more documentation, more options
* CQLPG-29 "sortBy field" sorts by null (cql2pgjson:1.3.3)

## 16.0.2 2017-12-05

* RMB-94: "readonly" fields should be ignored
* MODINVSTOR-38: Fix number in string by updating cql2pgjson to 1.3.2

## 16.0.1 2017-12-01

* UICHKOUT-39: Update cql2pgjson to v1.3.1 fixing number handling

## 16.0.0 2017-11-29

* RMB-56 add SchemaDereferencer that replaces "$ref" in raml files
* RMB-82 additional / optimize counting of results via postgresClient
* RMB-84 fix facet mechanism to use new counting RMB-82
* RMB-88 optimize facet query
* RMB-91 Remove case insensitive support and use generic ->'' operator
* RMB-89 Performance checklist - various performance issues
* RMB-99 sql EXPLAIN returns very in-accurate counts on a view , OR queries
* RMB-77 fix documentation for declaration of json schema

## 15.0.2 2017-11-04

* RMB-64
* RMB-63
* RMB-66
* RMB-70
* RMB-71 move rmb to log4j with proper log patterns
* RMB-73
* RMB-77
* Support regular BTree indexes

## 15.0.0 2017-09-26

* RMB-61 move sql templates into rmb (break backwards compatibility for implementing modules using the sql scripts to generate a DB schema on tenant creation / update / delete)

## 14.0.0

* RMB-59 implement faceting framework (breaks backwards compatibility - postgresClient's get() now returns a org.folio.rest.persist.interfaces.Result object and not an Object[])
* RMB-51 add read only field usage
* RMB-57 Add upsert support to rmb db client
* RMB-52 Change name of expected metadata property (breaks backwards compatibility)
* RMB-60 Mark vertx-unit to compile scope (breaking change for modules that use the vertx-unit packaged in RMB)

## 13.1.0

* RMB-48 implement handling of meta-data section in RMB

## 13.0.3

* RMB-49 VertxUtils.getVertxWithExceptionHandler()
* FOLIO-749 add exception handler to each vertx

## 13.0.2

* FOLIO-727 Support multi-field queries in CQL2PgJSON (upgraded CQL2PgJSON for this functionality)

## 13.0.1

* RMB-46 RMB's http client caches requests by default

## 13.0.0

* RMB-42 Generated clients populate Authorization header with okapi tenant
* RMB-34 Validation response does not include content-type header
* FOLIO-693 document HTTP status codes
* DMOD-164 unit tests for Tenant API
* FOLIO-685 explore auto-generation of fakes (mocks) for module testing

## 12.1.4

* RMB-32 fix JSON encoding of String in UpdateSection
* RMB-40 submodule util with ResourceUtil.asString(...), IOUtil.toUTF8String(InputStream)
* RMB-36 fix PostgresClient.runSQLFile(...) when last sql statement does not end with ;
* RMB-31 Foreign key documentation and performance test
* RMB-38 PostgresClient on 2nd vertx throws RejectedExecutionException
* Support cross module join functionality - see MODUSERBL-7

## 12.1.3 2017-06-06

* v1.23 of embedded postgres fails to start on windows - upgrade to v2.2 RMB-25
* Split PostgresClient RMB-10
* getting module name from pom error RMB-28
* tenant api - unit tests and code cleanup RMB-3
* Provide UtilityClassTester
* includes trait added to raml-util
* raml-util updated
* Support passing List<String> to implementing function when query param is repeated in query string

## 12.1.2 2017-05-25

* When additionalProperties is set to false return a 422 RMB-16
* When additionalProperties is set to false, getting old db objects are ignored but count remains RMB-22

## 12.1.1 2017-05-24

* Requests to /admin/health require X-Okapi-Tenant RMB-24

## 12.1.0 2017-05-23

* HTTP Join functionality across modules
* Not passing x-okapi-tenant returns 400
* Criteria auto type detection for single quoted strings
* Criteria accepts schema for validation + tests
* Fix embedded-postgres cleanup causing unit test failure RMB-23

## 11.0.0 2017-05-11

* Date format of JSONs is saved as yyyy-MM-dd'T'HH:mm:ss.SSSZ
* HTTP Join functionality across modules

## 10.2.0 2017-05-04

* Allow indicating package to log in debug mode via command line: debug_log_package=[, a.b., a.b.c] RMB-13
* Preserve exceptions in reply handler RMB-15
* Cross module http join: many to many, one to many support

## 10.0.7 2017-05-02

* Non-snapshot version.

