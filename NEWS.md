## 32.2.4 2021-12-14

Bug fix:

 * [RMB-889](https://issues.folio.org/browse/RMB-889) Log4j 2.16.0 disabling JDNI and lookups (CVE-2021-44228)

## 32.2.3 2021-06-08

Bug fixes:

* [RMB-823](https://issues.folio.org/browse/RMB-823) Upgrade okapi-common to 4.8.0 fixing maven.indexdata.com url

## 32.2.2 2021-04-23

Bug fixes:

 * [RMB-823](https://issues.folio.org/browse/RMB-823) Update maven.indexdata.com URL to use https
 * [RMB-830](https://issues.folio.org/browse/RMB-830) foreign key non-alias sub-field query causes "missing FROM-clause" error

## 32.2.1 2021-04-21

Bug fixes:

 * [RMB-586](https://issues.folio.org/browse/RMB-586) == foreign key sub-field query causes "missing FROM-clause" error
 * [RMB-829](https://issues.folio.org/browse/RMB-829) Suppress "Table audit\_... NOT FOUND" in loadDbSchema()

Documentation updates:

 * [RMB-787](https://issues.folio.org/browse/RMB-787) Document guidance on API and Schema versioning for OptimisticLocking
 * [RMB-770](https://issues.folio.org/browse/RMB-770) Add personal data disclosure form

## 32.2.0 2021-02-24

New API features:

 * [RMB-741](https://issues.folio.org/browse/RMB-741) PostgresClient.withTransaction
 * [RMB-483](https://issues.folio.org/browse/RMB-483) TestContainers instead of postgresql-embedded
 * [RMB-601](https://issues.folio.org/browse/RMB-601) PgUtil futurisation for Vert.x 4
 * [RMB-806](https://issues.folio.org/browse/RMB-806) TenantClient: async utility

Other changes:

￼* [RMB-519](https://issues.folio.org/browse/RMB-519) Default Accept: */*, Content-Type: application/json
 * [RMB-768](https://issues.folio.org/browse/RMB-768) Default log4j2 logging should be line based patternlayout
￼* [RMB-786](https://issues.folio.org/browse/RMB-786) Change OL trigger notice level

Bugs fixed:

￼* [RMB-799](https://issues.folio.org/browse/RMB-799) Update Vert.x from 4.0.0 to 4.0.2
 * [RMB-788](https://issues.folio.org/browse/RMB-788) invalid id for GET /_/tenant/id hangs
￼* [RMB-803](https://issues.folio.org/browse/RMB-803) Fix URL encoding in BuildCQL preventing CQL injection

## 32.1.0 2021-01-05

 * [RMB-782](https://issues.folio.org/browse/RMB-782) Make postTenantSync public
 * [RMB-781](https://issues.folio.org/browse/RMB-781) Mention raml-util update for RMB upgrade notes
 * [RMB-642](https://issues.folio.org/browse/RMB-642) Remove JDBC driver, use vertx-pg-client for running migrations
 * [RMB-780](https://issues.folio.org/browse/RMB-780) NPE in logs after migrating to RMB v32.0.0

## 32.0.0 2020-12-21

New features (some of which are breaking changes):

 * [RMB-609](https://issues.folio.org/browse/RMB-609) Update to Vert.x 4.0.0
 * [RMB-655](https://issues.folio.org/browse/RMB-655) Add default metrics to RMB: Outgoing API calls
 * [RMB-669](https://issues.folio.org/browse/RMB-669) Add default metrics to RMB: incoming API calls
 * [RMB-720](https://issues.folio.org/browse/RMB-720) Replace DropWizard with Micrometer InfluxDB metrics options
 * [RMB-727](https://issues.folio.org/browse/RMB-727) Implement support for optimistic locking
 * [RMB-754](https://issues.folio.org/browse/RMB-754) Provide implementation for the async tenant API
 * [RMB-772](https://issues.folio.org/browse/RMB-772) Persist tenant operation
 * [RMB-759](https://issues.folio.org/browse/RMB-759) Consistent format for createdDate and updatedDate metadata
 * [RMB-389](https://issues.folio.org/browse/RMB-389) PgUtil.delete by CQL
 * Use okapi-common 4.5.0 (which is also using Vert.x 4.0.0)

Fixes:

 * [RMB-744](https://issues.folio.org/browse/RMB-744) Q2 to Q3 upgrade - "POST request for module_version /_/tenant failed with REVOKE ALL PRIVILEGES ON SCHEMA public FROM module_schema;"
 * [RMB-728](https://issues.folio.org/browse/RMB-728) Unable to use 'var' keyword when a module uses aspects plugin

## 31.1.0 2020-09-16

 * [RMB-699](https://issues.folio.org/browse/RMB-699) Fix Module name detection broken
 * [RMB-684](https://issues.folio.org/browse/RMB-684) Fix GET query returns no records when offset value > estimated totalRecords
 * [RMB-701](https://issues.folio.org/browse/RMB-701) Update to Vert.x 3.9.2
 * [RMB-700](https://issues.folio.org/browse/RMB-700) NPE when RestVerticle calls LogUtil.formatStatsLogMessage
 * [RMB-677](https://issues.folio.org/browse/RMB-677) Close PostgreSQL connection after invalid CQL failure
 * [RMB-553](https://issues.folio.org/browse/RMB-553) Add postSync to PgUtil
 * [RMB-697](https://issues.folio.org/browse/RMB-697) Explain Vert.x 4 futurisation
 * [RMB-698](https://issues.folio.org/browse/RMB-698) saveBatch and upsertBatch should create metadata
 * [RMB-703](https://issues.folio.org/browse/RMB-703) Full text search doesn't match URLs containing &
 * [RMB-721](https://issues.folio.org/browse/RMB-721) NPE in PomReader while PostgresClient.getInstance
 * [RMB-723](https://issues.folio.org/browse/RMB-723) Close database connection after dropping schema and role
 * [RMB-702](https://issues.folio.org/browse/RMB-702) {version} path variable no longer allowed in RAML
 * [RMB-704](https://issues.folio.org/browse/RMB-704) RestVerticle "singleField" is nullable
 * [RMB-705](https://issues.folio.org/browse/RMB-705) Refactor assertThrows lambda to have only one invocation throwing an exception
 * [RMB-706](https://issues.folio.org/browse/RMB-706) Critical code smell constant name, hardcoded password and ip address
 * [RMB-707](https://issues.folio.org/browse/RMB-707) Remove JwtUtils in favour of okapi-common's OkapiToken
 * [RMB-711](https://issues.folio.org/browse/RMB-711) Remove Java 8 and Maven 3.3 check
 * [RMB-712](https://issues.folio.org/browse/RMB-712) [Snyk] Update javax.validation:validation-api to 2.0.1.Final
 * [RMB-713](https://issues.folio.org/browse/RMB-713) [Snyk] Update org.aspectj:aspectjrt to 1.9.6
 * [RMB-715](https://issues.folio.org/browse/RMB-715) [Snyk] Update commons-cli from 1.3.1 to 1.4
 * [RMB-716](https://issues.folio.org/browse/RMB-716) [Snyk] Update javaparser-core from 3.3.0 to 3.16.1
 * [RMB-714](https://issues.folio.org/browse/RMB-714) Remove unused aws-lambda-java-core dependency
 * [RMB-725](https://issues.folio.org/browse/RMB-725) StringUtil.cqlEncode masking CQL characters preventing CQL injection	

## 31.0.0 2020-08-18

 * [RMB-328](https://issues.folio.org/browse/RMB-328) Make RMB OpenJDK 11 compliant
 * [RMB-694](https://issues.folio.org/browse/RMB-694) Upgrade foreign key of a sub-field like "fieldName": "foo.bar"
 * [RMB-693](https://issues.folio.org/browse/RMB-693) Close prepared statements in PostgresClient stream get
 * [RMB-686](https://issues.folio.org/browse/RMB-686) Fix documentation about postTenant error handling
 * [RMB-687](https://issues.folio.org/browse/RMB-687) ResponseException for TenantAPI, fix wrong postTenant HTTP status code
 * Drop obsolete rmb\_internal\_index
 * [RMB-675](https://issues.folio.org/browse/RMB-675) Move pg\_trgm from wrong to public schema, don't drop & recreate

## 30.2.4 2020-07-10

 * [RMB-673](https://issues.folio.org/browse/RMB-673) totalRecords returns exact hit count for limit=0
 * [RMB-670](https://issues.folio.org/browse/RMB-670) Fix RestVerticle requestId logging, replace log4j 1.2 MDC by log4j 2 ThreadContext

## 30.2.3 2020-07-08

 * [RMB-671](https://issues.folio.org/browse/RMB-671) Reinstall PostgreSQL extensions on upgrade, drop pg\_catalog.pg\_trgm

## 30.2.2 2020-07-02

 * [RMB-657](https://issues.folio.org/browse/RMB-657) Default 60s connectionReleaseDelay prevents streaming large data
 * [RMB-656](https://issues.folio.org/browse/RMB-656) NPE at startup when an OPTIONS endpoint is defined in RAML
 * [RMB-662](https://issues.folio.org/browse/RMB-662) Remove parseDatabaseSchemaString and org.json:json
 * [RMB-660](https://issues.folio.org/browse/RMB-660) com.thoughtworks.xstream vulnerability (CVE-2017-7957)
 * [RMB-661](https://issues.folio.org/browse/RMB-661) Update log4j to 2.13.3 fixing SMTPS host mismatch (CVE-2020-9488)

## 30.2.1 2020-06-23

RMB 30.0 and 30.1 are discontinued. Only RMB >= 30.2 will receive bug and security fixes.

 * [RMB-653](https://issues.folio.org/browse/RMB-653) Urldecode path parameters like /perms/users/12%2334
 * [RMB-649](https://issues.folio.org/browse/RMB-649) Remove duplicate foreign key constraints for unchanged tables

## 30.2.0 2020-06-19

Note error message regression: [RMB-652](https://issues.folio.org/browse/RMB-652)
"may not be null" changed to "must not be null" (hibernate-validator)

 * [RMB-648](https://issues.folio.org/browse/RMB-648) org.hibernate:hibernate-validator vulnerability
 * [RMB-649](https://issues.folio.org/browse/RMB-649) Remove duplicate foreign key constraints for unchanged tables
 * [RMB-650](https://issues.folio.org/browse/RMB-650) Client generator should support Date query parameter

## 30.1.0 2020-06-15

 * [RMB-645](https://issues.folio.org/browse/RMB-645) Use where-only clause for the "count query"
 * [RMB-643](https://issues.folio.org/browse/RMB-643) Support generating wrapped native types
 * [RMB-640](https://issues.folio.org/browse/RMB-640) Sortby title and limit=0 gives zero hits
 * [RMB-639](https://issues.folio.org/browse/RMB-639) Unexpected breaking changes streamGet and getWithOptimizedSql
 * [RMB-638](https://issues.folio.org/browse/RMB-638) Revert PgUtil.streamGet parameter reordering
 * [RMB-636](https://issues.folio.org/browse/RMB-636) Support PATCH requests
 * [RMB-634](https://issues.folio.org/browse/RMB-634) PostgresClient does not provide access to db connection configs
 * [RMB-626](https://issues.folio.org/browse/RMB-626) Remove sample, sample2
 * [RMB-615](https://issues.folio.org/browse/RMB-615) Cancel queries that take longer than limit
 * [RMB-555](https://issues.folio.org/browse/RMB-555) Upgrade creates duplicate foreign key constraints
 * [RMB-495](https://issues.folio.org/browse/RMB-495) Date types of query parameters do not work correctly

## 30.0.2 2020-06-05

 * [RMB-632](https://issues.folio.org/browse/RMB-632) Tenant upgrade fails on foreignKey targetPath
 * [RMB-634](https://issues.folio.org/browse/RMB-634) PostgresClient does not provide access to db connection configs
 * [RMB-637](https://issues.folio.org/browse/RMB-637) Run ANALYZE after index (re-)creation

## 30.0.1 2020-05-30

 * [RMB-627](https://issues.folio.org/browse/RMB-627) Generate valid SQL for index with "stringType" false
 * [RMB-525](https://issues.folio.org/browse/RMB-525) Removes drools validation
 * [RMB-625](https://issues.folio.org/browse/RMB-625) Remove PostgresRunner; not in use anywhere
 * [RMB-624](https://issues.folio.org/browse/RMB-624) GenerateRunner/SchemaDereferencer fails with
   InvocationTargetException/DecodeException "Failed to decode"
 * Upgrade to Vert.x 3.9.1.
   [Release Notes](https://github.com/vert-x3/wiki/wiki/3.9.1-Release-Notes) and
   [Deprecations and breaking changes](https://github.com/vert-x3/wiki/wiki/3.9.1-Deprecations-and-breaking-changes)
 * [RMB-629](https://issues.folio.org/browse/RMB-629) Fix null pointer exception in deserializeRow
 * Various fixes and clean up to upgrading notes

## 30.0.0 2020-05-15

 See [upgrade notes](doc/upgrading.md#version-300)

 * [RMB-552](https://issues.folio.org/browse/RMB-552) smart index recreation on module upgrade
 * [RMB-583](https://issues.folio.org/browse/RMB-583) move f_unaccent and concat* from public to schema, fix search_path
 * [RMB-164](https://issues.folio.org/browse/RMB-164) Incorrect moduleVersion and rmbVersion in rmb_internal table
 * [RMB-491](https://issues.folio.org/browse/RMB-491) Migrate deprecated/removed Future calls in preparation for
   vert.x4 migration
 * [RMB-575](https://issues.folio.org/browse/RMB-575) Add "sqlExpressionQuery" parameter
 * [RMB-587](https://issues.folio.org/browse/RMB-587) Do not use public schema, remove it from search_path
 * [RMB-588](https://issues.folio.org/browse/RMB-588) investigate wrong value passed from "module_from" to ${version}
 * [RMB-594](https://issues.folio.org/browse/RMB-594) Random and changing order of GenerateRunner class generation
 * [RMB-565](https://issues.folio.org/browse/RMB-565) Review if all CQL to SQL generation code is using masking
 * [RMB-590](https://issues.folio.org/browse/RMB-590) Persist schema.json in rmb_internal
 * [RMB-591](https://issues.folio.org/browse/RMB-591) Remove or harmonize 'optimizedSql' execution path
 * [RMB-580](https://issues.folio.org/browse/RMB-580) Unit tests in Jenkins fail which seem related to streamGet
 * [RMB-605](https://issues.folio.org/browse/RMB-605) Combining characters (diacritics, umlauts) not found
 * [RMB-606](https://issues.folio.org/browse/RMB-606) HridSettingsIncreaseMaxValueMigrationTest has stopped working
 * [RMB-607](https://issues.folio.org/browse/RMB-607) `${myuniversity}_${mymodule}.gin_trgm_ops`
 * [RMB-612](https://issues.folio.org/browse/RMB-612) PostgresClientIT can not be executed in isolation
 * [RMB-246](https://issues.folio.org/browse/RMB-246) migrate to reactive postgres client (vertx-pg-client)
 * [RMB-600](https://issues.folio.org/browse/RMB-600) PostgresClient.upsertBatch
 * [RMB-619](https://issues.folio.org/browse/RMB-619) Update Vert.x to 3.9.0
 * [RMB-622](https://issues.folio.org/browse/RMB-622) PgExceptionUtil.getMessage: Compose a message of all
   PgException fields.
 * [RMB-569](https://issues.folio.org/browse/RMB-569) Drop fromModuleVersion and tOps from table section, use
   schema.json comparison instead
 * [RMB-610](https://issues.folio.org/browse/RMB-610) Remove JobAPI code
 * [RMB-616](https://issues.folio.org/browse/RMB-616) Update readme to specify permissionsRequired
 * [RMB-620](https://issues.folio.org/browse/RMB-620) Configure idle timeout with connectionReleaseDelay

## 29.3.0 2020-02-27

 * [RMB-499](https://issues.folio.org/browse/RMB-499) add "normalizeDigits" function
 * [RMB-500](https://issues.folio.org/browse/RMB-500) PostgresClient.streamGet with total hits and error handling
 * [RMB-549](https://issues.folio.org/browse/RMB-549) Mismatch lower() for compound index
 * [RMB-559](https://issues.folio.org/browse/RMB-559) Add stream get utility that produces HTTP result
 * [RMB-560](https://issues.folio.org/browse/RMB-560) Upgrade vert.x from 3.8.4 to 3.8.5 fixing CompositeFuture
 * [RMB-563](https://issues.folio.org/browse/RMB-563) SQL injection in PostgresClient.update by id
 * [RMB-562](https://issues.folio.org/browse/RMB-562) Switch back to upstream vertx-mysql-postgresql-client
 * [RMB-568](https://issues.folio.org/browse/RMB-568) Add JSON object array helper functions

## 29.2.0 2020-01-06

 * [RMB-498](https://issues.folio.org/browse/RMB-498) Truncate b-tree string for 2712 index row size
 * [RMB-533](https://issues.folio.org/browse/RMB-533) Performance: Fix lower/f_unaccent usage by checking all 5
   index types
 * [RMB-532](https://issues.folio.org/browse/RMB-532) Criterion get does not select id (only jsonb)
 * [RMB-536](https://issues.folio.org/browse/RMB-536) Missing f_unaccent for compound fields full text index
 * [RMB-537](https://issues.folio.org/browse/RMB-537) f_unaccent single quote fullText tsquery sql injection
 * [RMB-540](https://issues.folio.org/browse/RMB-540) Unique index must not truncate for 2712 max index byte size
 * [RMB-542](https://issues.folio.org/browse/RMB-542) Implement streamGet with CQLWrapper as parameter
 * [RMB-541](https://issues.folio.org/browse/RMB-541) contributors and identifiers search perf regression with
   RMB 29.1.3/29.1.4

## 29.1.0 2019-12-02

 * [RMB-529](https://issues.folio.org/browse/RMB-529) Upgrade Vert.x 3.8.4 fixing Netty HTTP request smuggling
 * [RMB-471](https://issues.folio.org/browse/RMB-471) Fix ddlgen/Index.java StringBuilder code smells
 * [RMB-486](https://issues.folio.org/browse/RMB-486) Use indexed id fields in foreign tables, such as
   `holdingsRecords.permanentLocationId==abc*` from mod-inventory-storage
 * [RMB-487](https://issues.folio.org/browse/RMB-487) support also field[*] notation
 * [RMB-404](https://issues.folio.org/browse/RMB-404) Use org.folio.okapi.common.SemVer rather than copying Apache
   Maven code
 * [RMB-506](https://issues.folio.org/browse/RMB-506) Estimate hit counts
 * [RMB-522](https://issues.folio.org/browse/RMB-522) TenantLoading in sequence - rather than parallel
 * [RMB-523](https://issues.folio.org/browse/RMB-523) TenantLoading Retry with 422 for PUT/POST attempt
 * [RMB-524](https://issues.folio.org/browse/RMB-524) TenantLoading does not work on Windows (path problem)
 * [RMB-528](https://issues.folio.org/browse/RMB-528) schema.json's fromModuleVersion messy and buggy

## 29.0.0 2019-11-20

 * Update jackson to 2.10.1
 * [RMB-518](https://issues.folio.org/browse/RMB-518) Extract index2sqlText and index2sqlJson for reuse
 * [RMB-516](https://issues.folio.org/browse/RMB-516) Rename Cql2SqlUtil.hasCqlWildCardd to hasCqlWildCard
 * [RMB-514](https://issues.folio.org/browse/RMB-514) Update to aspectj 1.9.4 and aspectj-maven-plugin 1.11
 * [RMB-511](https://issues.folio.org/browse/RMB-511) Use log4js MDC class, rather than slf4j
 * [RMB-510](https://issues.folio.org/browse/RMB-510) Breaking change: POST /_/tenant requires JSON body with module_to
 * [RMB-509](https://issues.folio.org/browse/RMB-509) POST /_/tenant returns invalid JSON: "[ ]"
 * [RMB-508](https://issues.folio.org/browse/RMB-508) Integration tests for /_/tenant with X-Okapi-Request-Id
 * [RMB-507](https://issues.folio.org/browse/RMB-507) TenantAPI POST NullPointerException if tenant already exists
 * [RMB-497](https://issues.folio.org/browse/RMB-497) Replace JSQL parser by using SqlSelect from CQL2PgJSON

## 28.1.0 2019-11-06

 * [RMB-504](https://issues.folio.org/browse/RMB-504) Jackson-* version 2.10.0, fixes jackson-databind security
 * [RMB-474](https://issues.folio.org/browse/RMB-474) Support 'field[*].subfield' syntax in multiFieldNames
 * Mention Vert.x 3.8 Future/Premise changes in upgrading.md
 * Fix upgrading wording for fullText defaultDictionary

## 28.0.0 2019-10-30

 * [RMB-433](https://issues.folio.org/browse/RMB-433) Fix saveBatch must not overwrite existing id
 * [RMB-492](https://issues.folio.org/browse/RMB-492) Remove unnecessary JSQLParserException error log if query
 * [RMB-493](https://issues.folio.org/browse/RMB-493) Fix CQLWrapper.addWrapper not left associative as it should be
   was parsed successfully
 * [RMB-496](https://issues.folio.org/browse/RMB-496) connectionReleaseDelay: Use 1 minute as default, add config

## 27.1.2 2019-10-23

 * [RMB-485](https://issues.folio.org/browse/RMB-485): RMB modules leak PostgreSQLConnection objects
 * [RMB-476](https://issues.folio.org/browse/RMB-476): PgUtil.post: Return after-trigger value (INSERT ... RETURNING jsonb)
 * [RMB-482](https://issues.folio.org/browse/RMB-482): Dropwizard metrics disabling
 * [RMB-427](https://issues.folio.org/browse/RMB-427): StatsTracker not being synchronized can fail PostgresClient operations when run in parallel
 * [RMB-469](https://issues.folio.org/browse/RMB-469): "multiFieldNames": "firstName , lastName" fails on spaces around the comma
 * [RMB-470](https://issues.folio.org/browse/RMB-470): "multiFieldNames" fail on nested fields (subfields) like proxy.personal.firstName
 * [RMB-462](https://issues.folio.org/browse/RMB-462): fullText / defaultDictionary not used in queries

## 27.1.1 2019-09-25

 * [RMB-479](https://issues.folio.org/browse/RMB-479) mvn install leaves embedded postgres running
 * [RMB-478](https://issues.folio.org/browse/RMB-478) RMB echoes all headers

## 25.0.2 2019-09-25

 * [RMB-478](https://issues.folio.org/browse/RMB-478) RMB echoes all headers

## 27.1.0 2019-09-24

See also [27.1 upgrading instructions](doc/upgrading.md#version-271).
 * [RMB-444](https://issues.folio.org/browse/RMB-444) Documentation: Name of generated .java files (resource, model)
 * [RMB-385](https://issues.folio.org/browse/RMB-385) add 'queryIndexName' to schema.json and allow compound indexes
 * [RMB-421](https://issues.folio.org/browse/RMB-421) 422 JSON error message for PgUtil
 * [RMB-457](https://issues.folio.org/browse/RMB-457) option to enable: analyze explain queries
 * [RMB-466](https://issues.folio.org/browse/RMB-466) drop CQL2PgJSON view constructor
 * [RMB-464](https://issues.folio.org/browse/RMB-464) Upgrade to Vert.x 3.8.1
 * [RMB-468](https://issues.folio.org/browse/RMB-468) Foreign key field index
 * [RMB-477](https://issues.folio.org/browse/RMB-477) jackson-databind 2.9.10 fix CVE-2019-16335, CVE-2019-14540

## 27.0.0 2019-08-26

Breaking changes:
 * [RMB-451](https://issues.folio.org/browse/RMB-451) add "targetPath" to disambiguate multi-table joins
 * [RMB-452](https://issues.folio.org/browse/RMB-452) make tableAlias and targetTableAlias explicit properties
 * [RMB-318](https://issues.folio.org/browse/RMB-318) Database config: Drop deprecated dot (db.port) variables, use underscore (DB_PORT)
 * [RMB-460](https://issues.folio.org/browse/RMB-460) Add 'snippetPath' to 'scripts' in schema.json
 * [RMB-445](https://issues.folio.org/browse/RMB-445) rename HttpStatus.HTTP_VALIDATION_ERROR to HttpStatus.HTTP_UNPROCESSABLE_ENTITY for 422

Bug fixes, new features:
 * [RMB-200](https://issues.folio.org/browse/RMB-200) Single quote SQL Injection in PostgresClient.update(table, updateSection, ...)
 * [RMB-459](https://issues.folio.org/browse/RMB-459) Populate metadata for reference data/sample data
 * [RMB-460](https://issues.folio.org/browse/RMB-460) Add 'snippetPath' to 'scripts' in schema.json

## 26.4.0 2019-08-12

 * [RMB-438](https://issues.folio.org/browse/RMB-438) contributor search returns no results when name
   contains "-"
 * [RMB-432](https://issues.folio.org/browse/RMB-432) fulltext: word splitting and punctuation removal

## 26.3.0 2019-08-07

 * [RMB-443](https://issues.folio.org/browse/RMB-443) HttpStatus.java: Explain FOLIO's 400 and 422 status code usage
 * [RMB-442](https://issues.folio.org/browse/RMB-442) Fix security vulnerabilities reported in
   jackson-databind >= 2.0.0, < 2.9.9.2
 * [RMB-440](https://issues.folio.org/browse/RMB-440) Skip logging GIN index missing if b-tree index can be used
 * [RMB-439](https://issues.folio.org/browse/RMB-439) Remove "-" token for full search to unblock CIRCSTORE-138
 * [RMB-437](https://issues.folio.org/browse/RMB-437) Fix queries with facet fail
 * [RMB-436](https://issues.folio.org/browse/RMB-436) Log both CQL and SQL within the missing index line
 * [RMB-435](https://issues.folio.org/browse/RMB-435) CVE-2019-12814 jackson-databind security vulnerability
 * [RMB-411](https://issues.folio.org/browse/RMB-411) Implement poLine.id = * style cql for subquery
 * [RMB-395](https://issues.folio.org/browse/RMB-395) Aupport nested sub queries across multiple tables
 * [RMB-430](https://issues.folio.org/browse/RMB-430) Audit table PK (id) unique constraint violation on
   concurrent queries

## 26.2.0 2019-07-11

 * [RMB-417](https://issues.folio.org/browse/RMB-417) Optimize search for array relation modifiers
 * [RMB-422](https://issues.folio.org/browse/RMB-422) Fix Invalid SQL for array modifiers
 * [RMB-426](https://issues.folio.org/browse/RMB-426) Consider also arraySubfield as relation modifier (with or
   without value)
 * [RMB-429](https://issues.folio.org/browse/RMB-429) Fix fulltext mismatch WRT accents. Note that this affects
   schema generation, so tenant data should be re-generated to ensure
   good performance.

## 26.1.0 2019-07-04

 * [RMB-419](https://issues.folio.org/browse/RMB-419) Fix syntax error at or near "." because table name is reserved word
 * #460 Bug fix: Reject foreign table processing if current table is unknown
 * #462 Bug fix: Do auditingSnippet null tests separately for declare and
   statement

## 26.0.0 2019-07-03

 * [RMB-380](https://issues.folio.org/browse/RMB-380) [RMB-416](https://issues.folio.org/browse/RMB-416) [RMB-418](https://issues.folio.org/browse/RMB-418) CQL extension: Implement /@ modifier for searching
   array-of-objects fields.
 * [RMB-387](https://issues.folio.org/browse/RMB-387) [RMB-391](https://issues.folio.org/browse/RMB-391) CQL extension: Implement foreign key joins (two tables).
 * [RMB-397](https://issues.folio.org/browse/RMB-397) Bug fix: Regression with $ref paths
 * [RMB-400](https://issues.folio.org/browse/RMB-400) Bug fix: PgUtil should return 400 (not 500) on invalid UUID id
 * [RMB-401](https://issues.folio.org/browse/RMB-401) Bug fix: PgUtil.post: Object or POJO type for entity
   parameter of respond201WithApplicationJson
 * [RMB-402](https://issues.folio.org/browse/RMB-402) [RMB-414](https://issues.folio.org/browse/RMB-414): Major change: Restructured audit (history) table
 * [RMB-405](https://issues.folio.org/browse/RMB-405) Bug fix: PostgresClient.save: Create random UUID if id is missing

## 25.0.0 2019-06-07

There are several breaking changes, see [upgrading instructions](doc/upgrading.md#version-25).

 * [RMB-199](https://issues.folio.org/browse/RMB-199) Fixed Single quote SQL Injection in
   `PostgresClient.delete(table, pojo, handler)`
 * [RMB-376](https://issues.folio.org/browse/RMB-376) Merge CQL2PG into RMB. Project cql2pgjson-java is now a
   subject part of RMB. This should not impact users of RMB.
 * [RMB-379](https://issues.folio.org/browse/RMB-379) Remove `Critiera.setValue` and replace it with `Criteria.setVal`
   to fix wrong Criteria value masking results in SQL Injection. This is
   major change.
 * [RMB-125](https://issues.folio.org/browse/RMB-125) Fix no way to indicate a removal of the metadata trigger in
   the schema.json
 * [RMB-277](https://issues.folio.org/browse/RMB-277) Always 'id' for primary key name, drop configuration option.
   This is a major change.
 * [RMB-346](https://issues.folio.org/browse/RMB-346) Drop the `populateJsonWithId` option. This is a major change.
 * [RMB-347](https://issues.folio.org/browse/RMB-347) Remove `gen_random_uuid()` as it fails in replication
   environments. The `generateId` option is removed from schema.json.
   This is major change.
 * [RMB-375](https://issues.folio.org/browse/RMB-375) Fix AnnotationGrabberTest not executed
 * [RMB-377](https://issues.folio.org/browse/RMB-377) jackson-databind 2.9.9: Block one more gadget type (CVE-2019-12086)
 * [RMB-378](https://issues.folio.org/browse/RMB-378) Update jersey to 2.28, fixing security issues
 * [RMB-383](https://issues.folio.org/browse/RMB-383) `PgUtil.deleteById`: make it return 400 on foreign key violation
 * [RMB-384](https://issues.folio.org/browse/RMB-384) Fix Generated models might have "readOnly" properties which
   are not in schema.
 * [RMB-392](https://issues.folio.org/browse/RMB-392) Move CQL2PgJSON.java from `org.z3950.zing.cql.cql2pgjson` to
   `org.folio.cql2pgjson` package . This is major change.
 * [RMB-396](https://issues.folio.org/browse/RMB-396) mask ) ] } in regexp to prevent SQL injection

## 24.0.0 2019-04-25

 * [RMB-368](https://issues.folio.org/browse/RMB-368) Update RMB for non-schema usage and CQLPG-90
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
 * [RMB-357](https://issues.folio.org/browse/RMB-357) Increase code coverage of PostgresClient
 * [RMB-352](https://issues.folio.org/browse/RMB-352) Increase code coverage of PgUtil

## 23.12.0 2019-04-05

 * [RMB-350](https://issues.folio.org/browse/RMB-350) PgUtilIT not tested
 * [RMB-331](https://issues.folio.org/browse/RMB-331) Tenant upgrade failed if older version has no db schema
 * [RMB-335](https://issues.folio.org/browse/RMB-335) Signal stream closed (prematurely)
 * [RMB-343](https://issues.folio.org/browse/RMB-343) Get rid of " WARNING Problem parsing x-okapi-token header" message
 * [RMB-345](https://issues.folio.org/browse/RMB-345) Document TenantLoading and proper behavior of CRUD services
 * [RMB-349](https://issues.folio.org/browse/RMB-349) Do not spawn postgres-runner when using -Dmaven.test.skip=true
 * [RMB-351](https://issues.folio.org/browse/RMB-351) Enable Junit4 + Junit5 and make it work in Eclipse
 * [RMB-354](https://issues.folio.org/browse/RMB-354) Add PostgresClient.saveBatch variant with SQLConnection parameter
 * [RMB-355](https://issues.folio.org/browse/RMB-355) Facets broken in RMB 22 (and later)

## 23.11.0 2019-03-14

 * [RMB-342](https://issues.folio.org/browse/RMB-342) TenantLoading / empty list for empty directory
 * [RMB-341](https://issues.folio.org/browse/RMB-341) Log CQL and generated SQL WHERE clause together

## 23.10.0 2019-03-12

 * [RMB-339](https://issues.folio.org/browse/RMB-339)/RMB-340 More facilities for TenantLoading
 * [RMB-334](https://issues.folio.org/browse/RMB-334) Add option to generate toString, hashCode and equals
   methods in generated classes
 * Enable JUnit5 in Eclipse
 * Minimum Java version runtime check

## 23.9.0 2019-03-01

 * [RMB-338](https://issues.folio.org/browse/RMB-338) TenantLoading: allow property of JSON Object to be set
 * Generated HTTP client now sets X-Okapi-Url header
 * TenantLoading: handle null TenantAttributes

## 23.8.0 2019-02-27

 * [RMB-337](https://issues.folio.org/browse/RMB-337) Extend TenangLoading API

## 23.7.0 2019-02-25

 * [RMB-330](https://issues.folio.org/browse/RMB-330) Gin index lost lower and f_unaccent functions
 * [RMB-329](https://issues.folio.org/browse/RMB-329) TenantLoading API

## 23.6.0 2019-02-18

 * [RMB-332](https://issues.folio.org/browse/RMB-332) Move BooksDemoAPI impl to test
 * Update cql2pgjson from 3.0.3 to 3.1.0 [CQLPG-76](https://issues.folio.org/browse/CQLPG-76) CQLPG-86

## 23.5.0 2019-01-29

 * [RMB-324](https://issues.folio.org/browse/RMB-324) Fix No Content-Type for most errors returned by RMB
 * [RMB-325](https://issues.folio.org/browse/RMB-325) Make /_/jsonSchemas to return schemas from subfolders
 * [RMB-326](https://issues.folio.org/browse/RMB-326) Fix Upload: complete state set before end-of-stream
 * Document headers streamed_id and complete

## 23.4.0 2019-01-16

 * [RMB-304](https://issues.folio.org/browse/RMB-304) PostgresClient.get(txConn, sql, params, replyHandler) for SELECT in a transaction
 * [RMB-305](https://issues.folio.org/browse/RMB-305) Add support for DISTINCT ON
 * [RMB-310](https://issues.folio.org/browse/RMB-310) XML handling different when going to RAML 1.0 / RMB 20+
 * [RMB-312](https://issues.folio.org/browse/RMB-312) Integration tests fails: too many open files
 * [RMB-313](https://issues.folio.org/browse/RMB-313) NullPointerException PostgresClientIT.tearDownClass on Windows
 * [RMB-315](https://issues.folio.org/browse/RMB-315) Fix security vulnerabilities in jackson-databind >= 2.9.0, < 2.9.8
 * [RMB-322](https://issues.folio.org/browse/RMB-322) Add offset and limit getter in CQLWrapper

## 23.3.0 2018-12-14

 * [RMB-162](https://issues.folio.org/browse/RMB-162) Simplify PostgresClient.getInstance
 * [RMB-185](https://issues.folio.org/browse/RMB-185) Nicer constructor for the CQLWrapper
 * [RMB-288](https://issues.folio.org/browse/RMB-288) StorageHelper post put deleteById getById
 * [RMB-293](https://issues.folio.org/browse/RMB-293) executeTransParamSyntaxError: IllegalStateException: Test
          already completed
 * [RMB-308](https://issues.folio.org/browse/RMB-308) enum values not matched
 * [RMB-311](https://issues.folio.org/browse/RMB-311) Validation of URI parameters
 * Update to cql2pgjson 3.0.3

## 23.2.0 2018-11-29

 * [RMB-296](https://issues.folio.org/browse/RMB-296) Tenant init for loading reference data
 * [RMB-299](https://issues.folio.org/browse/RMB-299) simplify embedded postgres start
 * [RMB-300](https://issues.folio.org/browse/RMB-300) Optional leading slash for ResourceUtil::asString
 * [RMB-303](https://issues.folio.org/browse/RMB-303) Remove org.junit.Assert dependency from UtilityClassTester
 * [RMB-306](https://issues.folio.org/browse/RMB-306) Add support for application/vnd.api+json content type for POST requests
 * [RMB-307](https://issues.folio.org/browse/RMB-307) Type check for query parameters broken

## 23.1.0 2018-11-08
 * [RMB-297](https://issues.folio.org/browse/RMB-297) Several connection.close() missing - totalCount calculation hangs after 5-12 errors.
 * [RMB-282](https://issues.folio.org/browse/RMB-282) Use description fields in RAML JSON schemas.
 * [RMB-290](https://issues.folio.org/browse/RMB-290) Document why environment variables with dots/periods are deprecated.
 * [RMB-291](https://issues.folio.org/browse/RMB-291) Exclude android, fix jzlib compression bug.
 * [RMB-292](https://issues.folio.org/browse/RMB-292) Enable compression of HTTP traffic in RestVerticle.
 * [RMB-294](https://issues.folio.org/browse/RMB-294) Broken maven-surefire-plugin.
 * [RMB-295](https://issues.folio.org/browse/RMB-295) Fix broken link to schema.json.example.json.

## 23.0.0 2018-10-18
 * [RMB-284](https://issues.folio.org/browse/RMB-284) id="foo\*" search results in QueryValidationException: CQL: Invalid UUID foo\*.
 * [RMB-18](https://issues.folio.org/browse/RMB-18) The "local apidocs" for "admin" and "jobs" has extra "/v1/" in path.
 * [RMB-275](https://issues.folio.org/browse/RMB-275) Windows backslash: Illegal character in opaque part at index 7: file:C:\Users\...
 * [RMB-279](https://issues.folio.org/browse/RMB-279) Fix false positive password vulnerability warning (sonarqube/sonarlint).
 * [RMB-280](https://issues.folio.org/browse/RMB-280) Provide a POM snippet to configure local apidocs.
 * [RMB-281](https://issues.folio.org/browse/RMB-281) Update vert.x libraries to fix security vulnerabilities (CVE-2018-12537)
 * [RMB-255](https://issues.folio.org/browse/RMB-255) Add streaming support to RMB.

## 22.1.0 2018-10-16
 * [RMB-265](https://issues.folio.org/browse/RMB-265) Allow recursion in JSON schema references (loops).
 * [RMB-274](https://issues.folio.org/browse/RMB-274) Implement PostgresClient::save(sqlConnection, table, id, entity, replyHandler).

## 22.0.1 2018-10-14
 * [RMB-273](https://issues.folio.org/browse/RMB-273) CQL2PG v3.0.1: Fix validating of array field names.

## 22.0.0 2018-10-13
 * [RMB-272](https://issues.folio.org/browse/RMB-272) Update cql2pg-json to version 3.0.0:
   * [CQLPG-63](https://issues.folio.org/browse/CQLPG-63) Drop field name to full field path resolution. This is a breaking change,
     using the unambiguous suffix "email" no longer resolves to "personal.email".
   * [CQLPG-61](https://issues.folio.org/browse/CQLPG-61) Equals empty is not same as equals star. Matching "*" means all records,
     matching "" means all records where that field is defined. This is a breaking change.
   * [CQLPG-58](https://issues.folio.org/browse/CQLPG-58) Performance: Convert id searches using '=' or '==' into primary key _id searches.
     This is a breaking change because it restricts the operators allowed for id and assumes
     that id is a UUID.
 * [RMB-271](https://issues.folio.org/browse/RMB-271) Also accept windows \r\n line endings in unit test.
 * [RMB-256](https://issues.folio.org/browse/RMB-256) Unit tests for PostgresClient.doGet.
 * [RMB-257](https://issues.folio.org/browse/RMB-257) Unit tests for PostgresClient.processResult.
 * [RMB-289](https://issues.folio.org/browse/RMB-289) private class TotaledResults: total needs to be Integer to afford null.
 * [RMB-268](https://issues.folio.org/browse/RMB-268) Remove finished MD creation code.
 * [RMB-261](https://issues.folio.org/browse/RMB-261) Skip all tests in PostgresClientIT on Windows.
 * [RMB-262](https://issues.folio.org/browse/RMB-262) Disable warning "Overriding managed version 3.5.1 for
   vertx-mysql-postgresql-client.
 * [RMB-243](https://issues.folio.org/browse/RMB-243) Add PostgresClient.execute(...) with SQL placeholders/parameters.
 * [RMB-258](https://issues.folio.org/browse/RMB-258) Drop IOException of ResourceUtils.resource2String.
 * [RMB-230](https://issues.folio.org/browse/RMB-230), [RMB-254](https://issues.folio.org/browse/RMB-254) PostgresClient: init moduleName, add getTenantId() and getSchemaName().

## 21.0.4 2018-10-04
 * [RMB-266](https://issues.folio.org/browse/RMB-266) Fix path parameters reversed when calling handler

## 21.0.3 2018-10-01
 * [RMB-259](https://issues.folio.org/browse/RMB-259) Windows compile failure - schema files.

## 21.0.2 2018-09-28
 * [RMB-251](https://issues.folio.org/browse/RMB-251) Add tests for missign query returns 500 Internal Error.
 * [RMB-250](https://issues.folio.org/browse/RMB-250) Fix fulltext search: stop words, trailing space and *.
 * [RMB-249](https://issues.folio.org/browse/RMB-249) Default to the 'simple' directory for fulltext searches.

## 21.0.1 2018-09-07
 * [RMB-245](https://issues.folio.org/browse/RMB-245) Fix Invalid path for client generator

## 21.0.0 2018-09-06

 * [RMB-213](https://issues.folio.org/browse/RMB-213) Commence doc directory, move upgrading notes, tidy for RMBv20+
 * [RMB-237](https://issues.folio.org/browse/RMB-237) Do we need to bundle twitter.raml, github.raml, .. in fat jar?
 * [RMB-238](https://issues.folio.org/browse/RMB-238) Extend PostgresClient.getById with POJO (change of API)
 * [RMB-239](https://issues.folio.org/browse/RMB-239) Fix apidocs of RMB does not support RAML 1.0

## 20.0.0 2018-08-31

 * [RMB-221](https://issues.folio.org/browse/RMB-221) Single quote SQL Injection in PostgresClient.saveBatch(table, list, handler)
 * [RMB-231](https://issues.folio.org/browse/RMB-231) Errors in mod-inventory-storage when upgrading RMB from 19.1.5 to 19.3.1
 * [RMB-1](https://issues.folio.org/browse/RMB-1) Specifying a RAML that only uses GET fails with RMB
 * [RMB-44](https://issues.folio.org/browse/RMB-44) use generics for PostgresClient.get(...) and .join(...)
 * [RMB-109](https://issues.folio.org/browse/RMB-109) RAML 1.0 support: use raml-for-jax-rs rather than obsolete raml-jaxrs-codegen
 * [RMB-149](https://issues.folio.org/browse/RMB-149) Security update PostgreSQL 10.3 CVE-2018-1058 search_path
 * [RMB-174](https://issues.folio.org/browse/RMB-174) mvn install leaves a postgres process running
 * [RMB-186](https://issues.folio.org/browse/RMB-186) HTTP_ACCEPTED = 202, HTTP_OK = 200
 * [RMB-191](https://issues.folio.org/browse/RMB-191) Reenable JUnit 5
 * [RMB-192](https://issues.folio.org/browse/RMB-192) Add AdminAPI.postAdminImportSQL error reporting
 * [RMB-193](https://issues.folio.org/browse/RMB-193) Add RestVerticle stacktrace error reporting
 * [RMB-195](https://issues.folio.org/browse/RMB-195) Stacktrace logging on exception in AdminAPI
 * [RMB-202](https://issues.folio.org/browse/RMB-202) Create small RMB example
 * [RMB-203](https://issues.folio.org/browse/RMB-203) $ref should follow the JSON schema spec
 * [RMB-206](https://issues.folio.org/browse/RMB-206) Add PostgresClient.getById
 * [RMB-207](https://issues.folio.org/browse/RMB-207) Upgrade to maven-compiler-plugin 3.8.0
 * [RMB-225](https://issues.folio.org/browse/RMB-225) PostgresClient: Replace "Object conn" by "Future<SQLConnection> conn"
 * [RMB-227](https://issues.folio.org/browse/RMB-227) raml-cop rejects jobs.raml of RMB
 * [RMB-229](https://issues.folio.org/browse/RMB-229) PostgresClientTransactionsIT hangs on old database
 * [RMB-234](https://issues.folio.org/browse/RMB-234) Do NOT create tsvector indexes with lowercase/unaccent

## 19.3.1 2018-08-01

* Fix regression caused by [RMB-176](https://issues.folio.org/browse/RMB-176)

## 19.3.0 2018-08-01

* [RMB-176](https://issues.folio.org/browse/RMB-176) Support index for multiple properties in declarative schema

## 19.2.0 2018-07-16

* [RMB-184](https://issues.folio.org/browse/RMB-184) new "fulltext" index type in schema.json
* [RMB-183](https://issues.folio.org/browse/RMB-183) add ability to set the result set distinct

## 19.1.5 2018-07-05

* [RMB-179](https://issues.folio.org/browse/RMB-179) net.sf.jsqlparser does not support "IS TRUE" and "a@>b"
* [RMB-178](https://issues.folio.org/browse/RMB-178) parseQuery throws StackOverflowError for deeply nested SQL query
* [RMB-181](https://issues.folio.org/browse/RMB-181) Rewrite getLastStartPos for reducing stack size

## 19.1.4 2018-07-02

* [RMB-154](https://issues.folio.org/browse/RMB-154) Investigate postgresql connection pool

## 19.1.3 2018-06-10

* [RMB-167](https://issues.folio.org/browse/RMB-167) consistently use runOnContext for DB operations
* [RMB-172](https://issues.folio.org/browse/RMB-172) add /admin api to drop , create indexes per module

## 19.1.2 2018-06-03

* [RMB-168](https://issues.folio.org/browse/RMB-168) PostgresClient makes a wrong callback

## 19.1.1 2018-05-23

* [RMB-163](https://issues.folio.org/browse/RMB-163) Fail to find API implementation due to subtle class loading order difference
* [RMB-166](https://issues.folio.org/browse/RMB-166) Reduce stack size for SQL queries

## 19.1.0 2018-04-24

* [RMB-136](https://issues.folio.org/browse/RMB-136): Default paging in storage modules prevents operations on entire contents
* [RMB-138](https://issues.folio.org/browse/RMB-138): SchemaDereferencer: Support name to name.schema resolution
* [RMB-143](https://issues.folio.org/browse/RMB-143): Version update: vertx 3.5.1, jackson 2.9.4
* [RMB-144](https://issues.folio.org/browse/RMB-144): Security update PostgreSQL 10.2
* [RMB-146](https://issues.folio.org/browse/RMB-146): Add support for faceting on fields in arrays
* [RMB-148](https://issues.folio.org/browse/RMB-148): Configure implementation specific paging limits per endpoint
* [RMB-150](https://issues.folio.org/browse/RMB-150): High number of facets seems to cause error
* [RMB-151](https://issues.folio.org/browse/RMB-151): AspectJ not running when recompiling
* [RMB-152](https://issues.folio.org/browse/RMB-152): `/_/tenant` DELETE: consider to use 4xx over 500 if tenant does not exist
* [RMB-153](https://issues.folio.org/browse/RMB-153): Add folio/util/StringUtil.urlencode(String)
* [RMB-156](https://issues.folio.org/browse/RMB-156): Allow saving base64 encoded data as a jsonarray
* [RMB-157](https://issues.folio.org/browse/RMB-157): Helper for error handling
* [RMB-160](https://issues.folio.org/browse/RMB-160): allow registering a custom deserializer to the static object mapper
* FOLIO-1202: improve README raml section
* FOLIO-1179: Fix dev URLs
* FOLIO-1187: Add lint-raml-cop.sh

## 19.0.0 2018-03-01

* [RMB-140](https://issues.folio.org/browse/RMB-140): Move to v19, 18.0.1 requires entry in pom

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

* [RMB-130](https://issues.folio.org/browse/RMB-130): Use target/generated-sources for generated code
* [RMB-135](https://issues.folio.org/browse/RMB-135): Parse version from Module ID in `module_{from,to}`
* [RMB-137](https://issues.folio.org/browse/RMB-137): Move JsonPathParser.main() to JsonPathParserTest; use JUnit
* [RMB-139](https://issues.folio.org/browse/RMB-139): Add transaction support for get() and delete()

## 18.0.0 2018-02-19

* [RMB-133](https://issues.folio.org/browse/RMB-133): start, rollback and end transactions should use a handler of type `handler<asyncresult<object>>`
* [RMB-132](https://issues.folio.org/browse/RMB-132): Allow update() method to be called within a transaction.
* [RMB-131](https://issues.folio.org/browse/RMB-131): PostgresClient: asynchronous functions must not throw exceptions
* [RMB-128](https://issues.folio.org/browse/RMB-128): Upgrade to postgres v10

## 17.0.0 2017-01-17

* [RMB-119](https://issues.folio.org/browse/RMB-119): CQL's default relation "=" now uses "adj" relation for strings (was "all")
* CQLPG-30, CQLPG-31: Fix number handling in CQL queries
* [RMB-124](https://issues.folio.org/browse/RMB-124): Lift time limit of PostgresClientMultiVertxIT
* [RMB-122](https://issues.folio.org/browse/RMB-122): Setting caseSensitive= true, removeAccents = false in regular index declaration corrupts generated schema
* [RMB-121](https://issues.folio.org/browse/RMB-121): mod-configuration doesn't build with Postgres 10
* [RMB-120](https://issues.folio.org/browse/RMB-120): README should suggest to use UUID, not SERIAL
* [RMB-119](https://issues.folio.org/browse/RMB-119): CQL's default relation "=" should use "adj" for a string
* [RMB-118](https://issues.folio.org/browse/RMB-118): add a `/admin/db_maintenance` api
* [RMB-115](https://issues.folio.org/browse/RMB-115): jaxrs test cleanup results in compile failure
* [RMB-114](https://issues.folio.org/browse/RMB-114): Security update postgres-embedded:2.6
* [RMB-113](https://issues.folio.org/browse/RMB-113): Dereference raml-util if ramls directory does not exist
* [RMB-112](https://issues.folio.org/browse/RMB-112): Use logger rather than standard output/standard error
* [RMB-104](https://issues.folio.org/browse/RMB-104): optimize join views + add support for more than 2 tables
* [RMB-98](https://issues.folio.org/browse/RMB-98): Foreign key trigger created in public schema and not in tenant's schema

## 16.0.3 2017-12-19

* [RMB-103](https://issues.folio.org/browse/RMB-103): Schema dereferencing requires a ramls/ directory with the raml and schema files.
* [RMB-106](https://issues.folio.org/browse/RMB-106): Set Locale.US, use Logger, split into separate tests.
* importSQL tenant sensitive
* [RMB-110](https://issues.folio.org/browse/RMB-110): Lift time limit in PostgresClientIT.parallel(...)
* hack to handle simple text response for rmb http client
* ResourceUtil.asString(...): more documentation, more options
* [CQLPG-29](https://issues.folio.org/browse/CQLPG-29) "sortBy field" sorts by null (cql2pgjson:1.3.3)

## 16.0.2 2017-12-05

* [RMB-94](https://issues.folio.org/browse/RMB-94): "readonly" fields should be ignored
* MODINVSTOR-38: Fix number in string by updating cql2pgjson to 1.3.2

## 16.0.1 2017-12-01

* UICHKOUT-39: Update cql2pgjson to v1.3.1 fixing number handling

## 16.0.0 2017-11-29

* [RMB-56](https://issues.folio.org/browse/RMB-56) add SchemaDereferencer that replaces "$ref" in raml files
* [RMB-82](https://issues.folio.org/browse/RMB-82) additional / optimize counting of results via postgresClient
* [RMB-84](https://issues.folio.org/browse/RMB-84) fix facet mechanism to use new counting [RMB-82](https://issues.folio.org/browse/RMB-82)
* [RMB-88](https://issues.folio.org/browse/RMB-88) optimize facet query
* [RMB-91](https://issues.folio.org/browse/RMB-91) Remove case insensitive support and use generic ->'' operator
* [RMB-89](https://issues.folio.org/browse/RMB-89) Performance checklist - various performance issues
* [RMB-99](https://issues.folio.org/browse/RMB-99) sql EXPLAIN returns very in-accurate counts on a view , OR queries
* [RMB-77](https://issues.folio.org/browse/RMB-77) fix documentation for declaration of json schema

## 15.0.2 2017-11-04

* [RMB-64](https://issues.folio.org/browse/RMB-64)
* [RMB-63](https://issues.folio.org/browse/RMB-63)
* [RMB-66](https://issues.folio.org/browse/RMB-66)
* [RMB-70](https://issues.folio.org/browse/RMB-70)
* [RMB-71](https://issues.folio.org/browse/RMB-71) move rmb to log4j with proper log patterns
* [RMB-73](https://issues.folio.org/browse/RMB-73)
* [RMB-77](https://issues.folio.org/browse/RMB-77)
* Support regular BTree indexes

## 15.0.0 2017-09-26

* [RMB-61](https://issues.folio.org/browse/RMB-61) move sql templates into rmb (break backwards compatibility for implementing modules using the sql scripts to generate a DB schema on tenant creation / update / delete)

## 14.0.0

* [RMB-59](https://issues.folio.org/browse/RMB-59) implement faceting framework (breaks backwards compatibility - postgresClient's get() now returns a org.folio.rest.persist.interfaces.Result object and not an Object[])
* [RMB-51](https://issues.folio.org/browse/RMB-51) add read only field usage
* [RMB-57](https://issues.folio.org/browse/RMB-57) Add upsert support to rmb db client
* [RMB-52](https://issues.folio.org/browse/RMB-52) Change name of expected metadata property (breaks backwards compatibility)
* [RMB-60](https://issues.folio.org/browse/RMB-60) Mark vertx-unit to compile scope (breaking change for modules that use the vertx-unit packaged in RMB)

## 13.1.0

* [RMB-48](https://issues.folio.org/browse/RMB-48) implement handling of meta-data section in RMB

## 13.0.3

* [RMB-49](https://issues.folio.org/browse/RMB-49) VertxUtils.getVertxWithExceptionHandler()
* [FOLIO-749](https://issues.folio.org/browse/FOLIO-749) add exception handler to each vertx

## 13.0.2

* [FOLIO-727](https://issues.folio.org/browse/FOLIO-727) Support multi-field queries in CQL2PgJSON (upgraded CQL2PgJSON for this functionality)

## 13.0.1

* [RMB-46](https://issues.folio.org/browse/RMB-46) RMB's http client caches requests by default

## 13.0.0

* [RMB-42](https://issues.folio.org/browse/RMB-42) Generated clients populate Authorization header with okapi tenant
* [RMB-34](https://issues.folio.org/browse/RMB-34) Validation response does not include content-type header
* [FOLIO-693](https://issues.folio.org/browse/FOLIO-693) document HTTP status codes
* [DMOD-164](https://issues.folio.org/browse/DMOD-164) unit tests for Tenant API
* [FOLIO-685](https://issues.folio.org/browse/FOLIO-685) explore auto-generation of fakes (mocks) for module testing

## 12.1.4

* [RMB-32](https://issues.folio.org/browse/RMB-32) fix JSON encoding of String in UpdateSection
* [RMB-40](https://issues.folio.org/browse/RMB-40) submodule util with ResourceUtil.asString(...), IOUtil.toUTF8String(InputStream)
* [RMB-36](https://issues.folio.org/browse/RMB-36) fix PostgresClient.runSQLFile(...) when last sql statement does not end with ;
* [RMB-31](https://issues.folio.org/browse/RMB-31) Foreign key documentation and performance test
* [RMB-38](https://issues.folio.org/browse/RMB-38) PostgresClient on 2nd vertx throws RejectedExecutionException
* Support cross module join functionality - see MODUSERBL-7

## 12.1.3 2017-06-06

* v1.23 of embedded postgres fails to start on windows - upgrade to v2.2 [RMB-25](https://issues.folio.org/browse/RMB-25)
* Split PostgresClient [RMB-10](https://issues.folio.org/browse/RMB-10)
* getting module name from pom error [RMB-28](https://issues.folio.org/browse/RMB-28)
* tenant api - unit tests and code cleanup [RMB-3](https://issues.folio.org/browse/RMB-3)
* Provide UtilityClassTester
* includes trait added to raml-util
* raml-util updated
* Support passing List<String> to implementing function when query param is repeated in query string

## 12.1.2 2017-05-25

* When additionalProperties is set to false return a 422 [RMB-16](https://issues.folio.org/browse/RMB-16)
* When additionalProperties is set to false, getting old db objects are ignored but count remains [RMB-22](https://issues.folio.org/browse/RMB-22)

## 12.1.1 2017-05-24

* Requests to /admin/health require X-Okapi-Tenant [RMB-24](https://issues.folio.org/browse/RMB-24)

## 12.1.0 2017-05-23

* HTTP Join functionality across modules
* Not passing x-okapi-tenant returns 400
* Criteria auto type detection for single quoted strings
* Criteria accepts schema for validation + tests
* Fix embedded-postgres cleanup causing unit test failure [RMB-23](https://issues.folio.org/browse/RMB-23)

## 11.0.0 2017-05-11

* Date format of JSONs is saved as yyyy-MM-dd'T'HH:mm:ss.SSSZ
* HTTP Join functionality across modules

## 10.2.0 2017-05-04

* Allow indicating package to log in debug mode via command line: debug_log_package=[, a.b., a.b.c] [RMB-13](https://issues.folio.org/browse/RMB-13)
* Preserve exceptions in reply handler [RMB-15](https://issues.folio.org/browse/RMB-15)
* Cross module http join: many to many, one to many support

## 10.0.7 2017-05-02

* Non-snapshot version.

