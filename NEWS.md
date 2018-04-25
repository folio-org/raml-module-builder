## 19.1.0 2018-04-24
* RMB-136: Default paging in storage modules prevents operations on entire contents
* RMB-138: SchemaDereferencer: Support name to name.schema resolution
* RMB-143: Version update: vertx 3.5.1, jackson 2.9.4 
* RMB-144: Security update PostgreSQL 10.2 
* RMB-146: Add support for faceting on fields in arrays
* RMB-148: Configure implementation specific paging limits per endpoint
* RMB-150: High number of facets seems to cause error
* RMB-151: AspectJ not running when recompiling
* RMB-152: /_/tenant DELETE: consider to use 4xx over 500 if tenant does not exist
* RMB-153: Add folio/util/StringUtil.urlencode(String)
* RMB-156: Allow saving base64 econded data as a jsonarray
* RMB-157: Helper for error handling
* RMB-160: allow registering a custom deserializater to the static object mapper
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
* RMB-135: Parse version from Module ID in ´module_{from,to}´
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
