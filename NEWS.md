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
