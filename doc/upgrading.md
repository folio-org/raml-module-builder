# RMB upgrading notes

These are notes to assist upgrading to newer versions.
See the [NEWS](../NEWS.md) summary of changes for each version.

<!-- ../../okapi/doc/md2toc -l 2 -h 3 upgrading.md -->
* [Version 35.0](#version-350)
* [Version 34.0](#version-340)
* [Version 33.2](#version-332)
* [Version 33.1](#version-331)
* [Version 33.0](#version-330)
* [Version 32.0](#version-320)
* [Version 31.0](#version-310)
* [Version 30.2](#version-302)
* [Version 30.0](#version-300)
* [Version 29.5](#version-295)
* [Version 29.2](#version-292)
* [Version 29](#version-29)
* [Version 28](#version-28)
* [Version 27.1](#version-271)
* [Version 27](#version-27)
* [Version 26](#version-26)
* [Version 25](#version-25)
* [Version 20](#version-20)

## Version 35.0

### [RMB-932](https://issues.folio.org/browse/RMB-932) Broken empty string matching: uuidfield == ""

Modules that have been using a workaround for this bug may need to remove the workaround.

### [RMB-927](https://issues.folio.org/browse/RMB-927) http: Do not join response headers

Multiple HTTP response headers with the same key, for example "Set-Cookie", are no longer joined.

### [RMB-945](https://issues.folio.org/browse/RMB-945] Vert.x 4.3.4

There's a breaking change in RowDesc in vertx-pg-client 4.3.4:

RMB <= 35.0.0 used with Vert.x >= 4.3.4 fails with this error:
`java.lang.NoSuchMethodError: 'void io.vertx.sqlclient.impl.RowDesc.<init>(io.vertx.sqlclient.desc.ColumnDescriptor[])'

RMB >= 35.0.1 used with Vert.x <= 4.3.3 fails with this error:
`java.lang.NoSuchMethodError: 'void io.vertx.sqlclient.impl.RowDesc.<init>(java.util.List, java.util.List)'

## Version 34.0

#### [RMB-856](https://issues.folio.org/browse/RMB-856)

RMB starts with default value of MaxFormAttributeSize of 8192, rather
than 32768. Modules that rely on higher value, such as mod-login-saml,
should set proper value in InitAPI hook.

```
   @Override
   public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
     RestVerticle.getHttpServerOptions().setMaxFormAttributeSize(64 * 1024);
     ..

```

#### [RMB-815](https://issues.folio.org/browse/RMB-815)


`UtilityClassTester` no longer part of RMB. Use `UtilityClassTester`
from `okapi-testing`. It has same API. Just import
`org.folio.okapi.testing.UtilityClassTester` and extend `pom.xml` with:

```
   <dependency>
     <groupId>org.folio.okapi</groupId>
     <artifactId>okapi-testing</artifactId>
     <version>4.14.1</version>
     <scope>test</scope>
   </dependency>
```

#### [RMB-883](https://issues.folio.org/browse/RMB-883)

TenantLoading methods `addJsonIdContent` and `addJsonIdBasename`
are removed.

#### [RMB-885](https://issues.folio.org/browse/RMB-885)

The `PostgresClient.streamGet` method without `PostgresClientStreamResult` parameter has been removed because it doesn't return totalCount and consumes too much memory. Use one of the other `PostgresClient.streamGet` methods with `PostgresClientStreamResult`.

`PostgresClient.get` methods with `String where` or `String filter` parameter have been removed to reduce SQL injection issues. Use `PostgresClient.get` methods with `CQLWrapper` or `Criterion` parameter instead.

#### [RMB-759](https://issues.folio.org/browse/RMB-759)

The format of metadata.createdDate and metadata.updatedDate has changed.

Old:

```
"metadata": {
  "createdDate": "2020-10-19T09:31:31.529",
  "updatedDate": "2020-10-19T09:31:31.529+00:00",
  "createdByUserId": "ba6baf95-bf14-4020-b44c-0cad269fb5c9",
  "updatedByUserId": "ba6baf95-bf14-4020-b44c-0cad269fb5c9"
}
```

New:

```
"metadata": {
  "createdDate": "2020-10-19T09:31:31.529Z",
  "updatedDate": "2020-10-19T09:31:31.529Z",
  "createdByUserId": "ba6baf95-bf14-4020-b44c-0cad269fb5c9",
  "updatedByUserId": "ba6baf95-bf14-4020-b44c-0cad269fb5c9"
}
```

This may cause some unit tests to fail, use java.time.Instant and compare Instants to be format agnostic.

#### [RMB-944](https://issues.folio.org/browse/RMB-944)

Unit tests that pass X-Okapi-Tenant header into TenantTool.tenantId must
use a case insensitive map. Replace `Map.of("x-okapi-tenant", "foo")`
with `new CaseInsensitiveMap<>(Map.of("x-okapi-tenant", "foo"))` after
`import org.apache.commons.collections4.map.CaseInsensitiveMap`.
This avoids failures caused by fall-back tenant id `folio_shared`.

## Version 33.2

#### [RMB-718](https://issues.folio.org/browse/RMB-718), [FOLIO-3351](https://issues.folio.org/browse/FOLIO-3351)

If the module intends to adopt the new totalRecords query parameter as part of this upgrade
then update the submodule that sources https://github.com/folio-org/raml to the latest version.

This is an optional step and NOT required to upgrade to 33.2, it is only necessary for using the new parameter.

This removes the language trait and adds totalRecords to the pageable trait:

* https://github.com/folio-org/raml/pull/141/files
* https://github.com/folio-org/raml/pull/140/files

Therefore you need to change the parameters of your methods that implement the RAML generated interfaces -
remove the lang parameter, and add the `String totalRecords` parameter before the `int offset` parameter.
The compile will fail unless this is done.

Examples:

Replace

`public void getMyitems(String query, int offset, int limit, String lang, Map<String, String> okapiHeaders,`

by

`public void getMyitems(String query, String totalRecords, int offset, int limit, Map<String, String> okapiHeaders,`

Replace

`public void postMyitems(String lang, Myitem entity, Map<String, String> okapiHeaders,`

by

`public void postMyitems(Myitem entity, Map<String, String> okapiHeaders,`

## Version 33.1

#### [RMB-862](https://issues.folio.org/browse/RMB-862), [RMB-874](https://issues.folio.org/browse/RMB-874) Upgrade to Vert.x 4.1.2/4.1.4

RMB 33.1.0 requires Vert.x 4.1.2 and okapi-common 4.8.2.
RMB >= 33.1.1 requires Vert.x >= 4.1.3 and okapi-common >= 4.9.0.

Module should use `vertx-stack-depchain`:

```
  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>io.vertx</groupId>
        <artifactId>vertx-stack-depchain</artifactId>
        <version>4.1.4</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>
```

The [FOLIO fork of the vertx-sql-client and vertx-pg-client](https://github.com/folio-org/vertx-sql-client/releases)
is no longer needed because our fix has been merged upstream for
Vert.x >= 4.1.1.

Therefore _remove_ these dependencies from the pom.xml:

```
      <dependency>
        <groupId>io.vertx</groupId>
        <artifactId>vertx-sql-client</artifactId>
        <version>${vertx.version}-FOLIO</version>
      </dependency>
      <dependency>
        <groupId>io.vertx</groupId>
        <artifactId>vertx-pg-client</artifactId>
        <version>${vertx.version}-FOLIO</version>
      </dependency>
```

## Version 33.0

Module should use the vertx, netty, jackson and tcnative dependencies from `vertx-stack-depchain` to avoid
old version with security vulnerabilities: Either remove the explicit vertx, netty, jackson and tcnative
dependencies from the pom.xml or use the
[versions that vertx-stack-depchain` ships with](https://github.com/vert-x3/vertx-dependencies/blob/4.1.2/pom.xml),
for example:

```
    <dependency>
      <groupId>io.vertx</groupId>
      <artifactId>vertx-codegen</artifactId>
    </dependency>
    <dependency>
      <groupId>io.netty</groupId>
      <artifactId>netty-common</artifactId>
    </dependency>
    <dependency>
      <groupId>io.netty</groupId>
      <artifactId>netty-tcnative-boringssl-static</artifactId>
    </dependency>
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-core</artifactId>
    </dependency>
```

#### [RMB-717](https://issues.folio.org/browse/RMB-717) Deprecate HttpClientInterface, HttpModuleClient2, HttpClientMock2

All classes in org.folio.rest.tools.client are deprecated. Instead
use the generated client provided by RMB or use WebClient directly.
Tests can create HTTP servers with Vertx.createHttpServer or use other
mocking facility.

#### [RMB-789](https://issues.folio.org/browse/RMB-789) Remove support of Embedded PostgreSQL Server

Removed support for embedded Postgres for testing. Testing is now with
[Testcontainers](https://testcontainers.org/).

The following calls have been removed:

  * `PostgresClient.setIsEmbedded`
  * `PostgresClient.setEmbeddedPort`
  * `PostgresClient.getEmbeddedPort`

Enable testing with postgres by calling
`PostgresClient.setPostgresTester(new PostgresTesterContainer())`
before any calls to PostgresClient or the Verticle. It is also
possible to provide your own by implemeting the `PostgresTester` interface.

If [DB\_\* environment variable database configuration](../README.md#environment-variables)
(or JSON DB config) is provided, the PostgresTester instance is
*not* used for testing. This allows testing to be performed on local
Postgres instance. See [RMB-826](https://issues.folio.org/browse/RMB-826).

`PostgresClient.stopEmbeddedPostgres` replaced with
`PostgresClient.stopPostgresTester`. The invocation is usually *not* needed
and should be removed because PostgresClient and Testcontainers core will automatically
close and remove the container.

`PostgresClient.startEmbeddedPostgres` replaced with
`PostgresClient.startPostgresTester`. It is usually not necessary to
invoke  this as it is automatically called by PostgresClient when an
instance is created.

Command-line option `embed_postgres=true` is no longer supported.

Remove `<groupId>ru.yandex.qatools.embed</groupId>`
`<artifactId>postgresql-embedded</artifactId>` from pom.xml.

#### Remove loading db conf from url

Removed for security. Use
[DB\_\* environment variables](../README.md#environment-variables)
instead. See discussion of [RMB-855](https://issues.folio.org/browse/RMB-855).

#### [RMB-785](https://issues.folio.org/browse/RMB-785) domain-models-maven-plugin

In pom.xml replace the exec-maven-plugin sections that call
`<mainClass>org.folio.rest.tools.GenerateRunner</mainClass>` by :

```xml
      <plugin>
        <groupId>org.folio</groupId>
        <artifactId>domain-models-maven-plugin</artifactId>
        <version>${raml-module-builder-version}</version>
        <executions>
          <execution>
            <id>generate_interfaces</id>
            <goals>
              <goal>java</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
```

Replace `<mainClass>org.folio.rest.tools.ClientGenerator</mainClass>` by
a call to the same plugin. It must be used in a separate project depending
on the artifact with generated interfaces.

```xml
      <plugin>
        <groupId>org.folio</groupId>
        <artifactId>domain-models-maven-plugin</artifactId>
        <version>${raml-module-builder-version}</version>
        <dependencies>
          <dependency>
            <groupId>org.folio</groupId>
            <artifactId>mod-my-server</artifactId>
            <version>${project.parent.version}</version>
          </dependency>
        </dependencies>
        <executions>
          <execution>
            <id>generate_interfaces</id>
            <goals>
              <goal>java</goal>
            </goals>
            <configuration>
              <generateClients>true</generateClients>
            </configuration>
          </execution>
        </executions>
      </plugin>

```

If you need to set system properties for `domain-models-maven-plugin` run
`properties-maven-plugin` with goal `set-system-properties` before:
https://www.mojohaus.org/properties-maven-plugin/usage.html#set-system-properties

Add FOLIO Maven repository for plugins after existing `<repositories>` section:
```xml
  <pluginRepositories>
    <pluginRepository>
      <id>folio-nexus</id>
      <name>FOLIO Maven repository</name>
      <url>https://repository.folio.org/repository/maven-folio</url>
    </pluginRepository>
  </pluginRepositories>
```

Remove `domain-models-interface-extensions` dependency from pom.xml.

Replace `PomReader.INSTANCE.getModuleName()` by `ModuleName.getModuleName()`.

Replace `PomReader.INSTANCE.getVersion()` by `ModuleName.getModuleVersion()`.

Replace `PomReader.INSTANCE.getRmbVersion()` by `RmbVersion.getRmbVersion()`.

Replace any joda class usage by a java.time class usage. RMB no longer ships with
joda-time that is deprecated because the replacement java.time has been in JDK core
since Java 8.

Add commons-lang:commons-lang:2.6 dependency to pom.xml if commons-lang is used.
RMB no longer ships with commons-lang.

Add org.assertj test dependency to pom.xml if Assertj is used. RMB no longer ships
with Assertj.

## Version 32.0

* [RMB-609](https://issues.folio.org/browse/RMB-609) Upgrade to Vert.x 4:
  * Replace deprecated `io.vertx.core.logging.Logger` by
    `org.apache.logging.log4j.Logger`, for example
    `private static final Logger LOGGER = LogManager.getLogger(Foo.class)`
    (using `getLogger()` without argument requires Multi-Release for
    stack walking, see below).
  * Replace `HttpClient` `.getAbs`, `.postAbs`, `.putAbs`, `.deleteAbs` by
    `WebClient.requestAbs(HttpMethod method, ...)`.
  * Replace `Future.setHandler` by `Future.onComplete`.
  * `Future` has been split into `Future` and `Promise`, see
    [Futurisation in Vert.x 4](futurisation.md).
  * The handlers of the autogenerated clients (generated by ClientGenerator) should be migrated from Handler<HttpClientResponse> to Handler<AsyncResult<HttpResponse<Buffer>>>
* [OKAPI-943](https://issues.folio.org/browse/OKAPI-943) When calling `.any`, `.all` or `.join`
  replace `io.vertx.core.CompositeFuture` by `org.folio.okapi.common.GenericCompositeFuture`,
  replace raw type by actual type and remove `@SuppressWarnings("rawtypes")`.
* [RMB-728](https://issues.folio.org/browse/RMB-728) In `pom.xml` update `aspectj-maven-plugin`
  `<configuration>` with `<complianceLevel>11</complianceLevel>`.
* Remove `setWorker(true)` when starting RestVerticle in tests or production code, learn why at
  [RMB RestVerticle](https://github.com/folio-org/raml-module-builder#restverticle) and
  [MODINVSTOR-635](https://issues.folio.org/browse/MODINVSTOR-635).
* Tenant API changed - refer to
  [RAML](https://github.com/folio-org/raml/blob/tenant_v2_0/ramls/tenant.raml).
  Tenant interface 2.0 is supported by Okapi 4.5.0 and later. RMB 32, thus, will
*not* work with an earlier version of Okapi.
  If module includes the shared raml as a Git sub module, it should be
  updated as well.
  See issues [FOLIO-2908](https://issues.folio.org/browse/FOLIO-2908)
   and [FOLIO-2877](https://issues.folio.org/browse/FOLIO-2877)
  The API for purge, upgrade, init is single end-point. Client code
  (mostly testing code) must be able to handle both 201 with a Location
  and 204 No content if testing via HTTP. However, for API testing RMB
  32.1.0 provides a simpler call: `TenantAPI.postTenantSync`.
  Update the module descriptor - usually `descriptors/ModuleDescriptor-template.json` - to
  tenant interface version 2. Replace the old `_tenant` "provides" with

```json
    "provides" : [ {
       "id" : "_tenant",
       "version" : "2.0",
       "interfaceType" : "system",
       "handlers" : [ {
         "methods" : [ "POST" ],
         "pathPattern" : "/_/tenant"
       }, {
         "methods" : [ "GET", "DELETE" ],
         "pathPattern" : "/_/tenant/{id}"
       } ]
    } ]
```
* Optional support for [optimistic locking](https://github.com/folio-org/raml-module-builder#optimistic-locking).
Please refer to [RMB-719](https://issues.folio.org/browse/RMB-719) and [RMB-727](https://issues.folio.org/browse/RMB-727)

## Version 31.0

* [RMB-738](https://issues.folio.org/browse/RMB-738) Since RMB 30.2.9 and 31.1.3:
  Upgrade to Vert.x 3.9.4
* [RMB-740](https://issues.folio.org/browse/RMB-740) Since RMB 30.2.9 and 31.1.3:
  Use FOLIO fork of vertx-sql-client and vertx-pg-client,
  [example pom.xml](https://github.com/folio-org/raml-module-builder/commit/1481635d291fc6191366aeb276c8e23fad038655):
```
  <properties>
    <vertx.version>3.9.4</vertx.version>
  </properties>
  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>io.vertx</groupId>
        <artifactId>vertx-stack-depchain</artifactId>
        <version>${vertx.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
      <dependency>
        <groupId>io.vertx</groupId>
        <artifactId>vertx-sql-client</artifactId>
        <version>${vertx.version}-FOLIO</version>
      </dependency>
      <dependency>
        <groupId>io.vertx</groupId>
        <artifactId>vertx-pg-client</artifactId>
        <version>${vertx.version}-FOLIO</version>
      </dependency>
    </dependencies>
  </dependencyManagement>
```
* [RMB-328](https://issues.folio.org/browse/RMB-328) Update to OpenJDK 11.
  In most cases no code changes are necessary. A few files needs updating
  ([mod-inventory-storage example](https://github.com/folio-org/mod-inventory-storage/pull/485/files)):
  * `pom.xml`: For `maven-compiler-plugin` update `version` to `3.8.1` and
    use `<release>11</release>` instead
    of `source` and `target` elements.
  * `pom.xml`: Update aspectj version to `1.9.6`. Update `aspectj-maven-plugin` with
    groupId `com.nickwongdev` and version `1.12.6` and set `<complianceLevel>11</complianceLevel>`
    in the `<configuration>` section.
  * `Jenkinsfile`: Add `buildNode = 'jenkins-agent-java11'`
  * `Dockerfile` (if present): change `folioci/alpine-jre-openjdk8:latest`
    to `folioci/alpine-jre-openjdk11:latest`.
  * `docker/docker-entrypoint.sh`: remove if present.
  * For `pom.xml`, plugin `maven-shade-plugin`, add
   `<Multi-Release>true</Multi-Release>` to section `manifestEntries`. see
   [this](https://github.com/folio-org/okapi/pull/968) example.
    Also, after `version` element, add section as shown below to avoid warnings
    about format specifiers during startup. See
    [this](https://github.com/folio-org/okapi/pull/964/commits/f4b5b33dfed59e2d64340a9a18a5cc479957afcf)
    example.

```xml
        <configuration>
          <filters>
            <filter>
              <artifact>*:*</artifact>
              <excludes>
                <exclude>**/Log4j2Plugins.dat</exclude>
              </excludes>
            </filter>
          </filters>
        </configuration>
```
* A colon has been added to the timezone to comply with RFC 3339,
  `2017-07-27T10:23:43.000+0000` becomes `2017-07-27T10:23:43.000+00:00`.

## Version 30.2

* [RMB-652](https://issues.folio.org/browse/RMB-652) error message
  "may not be null" changed to "must not be null" (hibernate-validator)
* [RMB-693](https://issues.folio.org/browse/RMB-693) If using
  PostgresClient#selectStream always call RowStream#close.
* [RMB-702](https://issues.folio.org/browse/RMB-702) Rename {version} path variable in RAML files.

## Version 30.0

* [RMB-246](https://issues.folio.org/browse/RMB-246) Switch to
    [vertx-pg-client](https://vertx.io/docs/vertx-pg-client/java/).
  * All functions that previously returned `UpdateResult` now return
    `RowSet<Row>`. From that result, the number of rows affected by
    SQL was `getUpdated()`, it is now `rowCount()`.
  * All functions that previously returned `ResultSet` now return
    `RowSet<Row>`. From that result, the number of rows affected by
    SQL is `rowCount()`. The `size()` method returns number of rows
    returned. An iterator to go through rows is obtained by calling
    `iterator()`.
  * `PostgresClient.selectSingle` returns `Row` rather than `JsonArray`.
  * PostgreSQL `JSONB` type was previously represented by Java `String`,
    now by Java `JsonObject` (return type and parameter type).
  * PostgreSQL `UUID` type (used for `id` and foreign keys columns) was
    previously represented by Java `String`, now by Java `UUID`
    (return type and parameter type).
  * In prepared/parameterized queries replace '?' signs by '$1', '$2' and so on
    and the parameters argument type `JsonArray` by `Tuple`.
  * Class `SQLConnection` is now provided by RMB. The same class name
    was used for the SQL client. `io.vertx.ext.sql.SQLConnection` ->
    `org.folio.rest.persist.SQLConnection`.
  * `PostgresClient.getClient()` is no longer public. If you need a
    connection, use `PostgresClient.startTx()`. For modules that wish to use
    vertx-pg-client directly, `PostgresClient.getConnection`  is offered -
    it returns `PgConnection` from the pool that is managed by `PostgresClient`.
  * Replace `exception.getMessage` by `PgExceptionUtil.getMessage(exception)`
    to mimic the old `GenericDatabaseException.getMessage(e)` because the
    new `PgException.getMessage(e)` returns the message field only, no SQL error code,
    no detail.
  * `PgExceptionFacade.getTable` removed.
  * `PgExceptionFacade.getIndex` removed.
  * `PgExceptionFacade.selectStream` without SQLConnection has been
     removed. Streams must be executed within a transaction.
  * `PostgresClient.mutate` removed (deprecated since Oct 2018).
* [RMB-619](https://issues.folio.org/browse/RMB-619)
  [Deprecation due to upgrading to Vert.x 3.9](https://github.com/vert-x3/wiki/wiki/3.9.0-Deprecations-and-breaking-changes):
  * Replace `Verticle#start(Future<Void>)` and `Verticle#stop(Future<Void>)` by
    `Verticle#start(Promise<Void>)` and `Verticle#stop(Promise<Void>)`
  * Replace `Future.setHandler(ar -> …)` by `Future.onComplete(ar -> …)`
* Vert.x 4 will split `Future` into `Future` and `Promise`, see
  [futurisation.md](./futurisation.md) for details and deprecations.
* [RMB-624](https://issues.folio.org/browse/RMB-624) Fix invalid RAML sample
  JSON files, otherwise GenerateRunner/SchemaDereferencer will fail with
  InvocationTargetException/DecodeException "Failed to decode".
  Hint: Use `for i in *; do jq empty $i || echo $i; done` to list invalid JSONs.
* Update Vert.x to 3.9.1, consider using
  [Vert.x Bill of Materials (Maven BOM)](https://github.com/vert-x3/vertx-stack#bills-of-materials-maven-bom)
  (content: [vertx-dependencies pom.xml](https://github.com/vert-x3/vertx-dependencies/blob/3.9.1/pom.xml#L49-L54))
  for io.vertx:\*, com.fasterxml.jackson.core:\* and com.fasterxml.jackson.dataformat:\* dependencies.
  Remove jackson-databind dependency from pom.xml if module doesn't use it directly.

## Version 29.5

* [RMB-587](https://issues.folio.org/browse/RMB-587) Write access to `public` schema has been removed, it is no longer in `search_path` and should be used for Postgres extensions only, prepend `public.` when calling their methods and operators. Example: RMB replaces `gin_trgm_ops` by `public.gin_trgm_ops` in indexes that RMB maintains.
* [RMB-588](https://issues.folio.org/browse/RMB-588) Consider removing
  `<directory>src/main/resources</directory> <filtering>true</filtering>`
  from pom.xml (or use a more specific directory tree). For details see [FOLIO-2548](https://issues.folio.org/browse/FOLIO-2548).
* [RMB-594](https://issues.folio.org/browse/RMB-594) RMB no longer uses a random order, but a fixed order to generate the .java classes from the .raml and .json files. This may consistently fail the build if two classes with the same name are generated. To prevent the second from overwriting the first, set a different class name using "javaType": "org.folio.rest.jaxrs.model.Bar". For example for link-field "folio:isVirtual" annotations for mod-graphql.

## Version 29.2

* [RMB-532](https://issues.folio.org/browse/RMB-532) Use PostgresClient.get without the unused and deprecated setId parameter.

## Version 29

* [RMB-529](https://issues.folio.org/browse/RMB-529) The update to Vert.x 3.8.4 deprecates several elements,
  including Json#mapper, Json#prettyMapper and Json#decodeValue:
  [3.8.2 Deprecations](https://github.com/vert-x3/wiki/wiki/3.8.2-Deprecations-and-breaking-changes),
  [3.8.3 Deprecations](https://github.com/vert-x3/wiki/wiki/3.8.3-Deprecations-and-breaking-changes)
* [RMB-510](https://issues.folio.org/browse/RMB-510) Tenant POST `/_/tenant` must include a body. Client that
  used an empty request must now include a body with at least:
  `{"module_to":"module-id"}`. This means that for modules that are using
  `org.folio.rest.client.TenantClient` that previously passed `null` as for
  `org.folio.rest.jaxrs.model.TenantAttributes` must now create
  `TenantAttributes` with `moduleTo` property.
* [RMB-497](https://issues.folio.org/browse/RMB-497) removed PostgresClient.join methods. It also deprecates all
  PostgresClient.get methods taking where-clause strings. Module users
  should use Criterion or CQLWrapper instead to construct queries:

    * Remove Criterion.selects2String, Criterion.from2String
    * Remove Criteria.setJoinON, Criteria.isJoinON
    * CQLWrapper.toString no longer returns leading blank/space

* [RMB-514](https://issues.folio.org/browse/RMB-514) Update aspectj-maven-plugin to version 11 and
  its aspectjrt and aspectjtools versions to 1.9.4 in pom.xml ([example](https://github.com/folio-org/raml-module-builder/blob/a77786a4b118a523b753a26d2f10aa51e276a4da/sample2/pom.xml#L180-L221))
  to avoid "bad version" warnings.

## Version 28

* [RMB-485](https://issues.folio.org/browse/RMB-485) changed the postgresql driver. Namespace prefix change
   `com.github.mauricio.async.db.postgresql` to
   `com.github.jasync.sql.db.postgresql`

    * `org.folio.rest.persist.PgExceptionFacade.getFields` returns
      `Map<Character.String>` rather than `Map<Object,String>`

    * `org.folio.rest.persist.PgExceptionUtil.getBadRequestFields` returns
      `Map<Character,String>` rather than `Map<Object,String>`

* [RMB-462](https://issues.folio.org/browse/RMB-462) removed support for `fullText defaultDictionary` property (`simple`, `english`, `german`, etc.) in `schema.json`

## Version 27.1

* Remove each foreign key field index and each primary key field `id` index and uniqueIndex, by setting `"tOps": "DELETE"` in schema.json. These btree indexes are created automatically.
* Each fullTextIndex that was created with a dictionary different than 'simple' (for example using `to_tsvector('english', jsonb->>'foo')`)
  needs to be dropped. Then RMB will recreate the index with 'simple'.
  [Example](https://github.com/folio-org/mod-circulation-storage/blob/a8cbed7d32861ec92295a67f93335780e4034e7b/src/main/resources/templates/db_scripts/schema.json):
```
  "scripts": [
    {
      "run": "before",
      "fromModuleVersion": "10.0.1",
      "snippet": "DROP INDEX IF EXISTS loan_userid_idx_ft;"
    }
  ]
```
* Breaking change due to [upgrading to Vert.x 3.8.1](https://github.com/vert-x3/wiki/wiki/3.8.0-Deprecations-and-breaking-changes#blocking-tasks)
    * old: `vertx.executeBlocking(future  -> …, result -> …);`
    * new: `vertx.executeBlocking(promise -> …, result -> …);`
    * old: `vertx.executeBlocking((Future <String> future ) -> …, result -> …);`
    * new: `vertx.executeBlocking((Promise<String> promise) -> …, result -> …);`
* Deprecation changes due to [upgrading to Vert.x 3.8.1](https://github.com/vert-x3/wiki/wiki/3.8.0-Deprecations-and-breaking-changes#future-creation-and-completion)
    * old: `Future <String> fut     = Future.future();   … return fut;`
    * new: `Promise<String> promise = Promise.promise(); … return promise.future();`
    * old: AbstractVerticle `start(Future <Void> fut)`
    * new: AbstractVerticle `start(Promise<Void> fut)`
    * old: `future.compose(res -> …, anotherFuture);`
    * new: `future.compose(res -> { …; return anotherFuture; });`
* Deprecation changes due to [upgrading to Vert.x 3.8.1 > 3.6.2](https://github.com/vert-x3/wiki/wiki/3.6.2-Deprecations)
    * old: `HttpClient` request/response methods with `Handler<HttpClientResponse>`
    * new: `WebClient` request/response methods with `Handler<AsyncResult<HttpClientResponse>>`

## Version 27

* Update the raml-util subdirectory to the latest raml1.0 commit of https://github.com/folio-org/raml to make metadata.createdByUserId optional:
    * Hint: `cd ramls/raml-util; git fetch; git checkout 69f6074; cd ../..; git add ramls/raml-util`
    * Note that pom.xml may revert any submodule update that hasn't been staged or committed!
      (See [notes](https://dev.folio.org/guides/developer-setup/#update-git-submodules).)

## Version 26

* The audit (history) table changed to a new incompatible schema. New audit configuration options are required in schema.json. See [README.md](../README.md#the-post-tenant-api) for details.
* PostgresClient constructor now, by default, starts Embedded Postgres.
  This means that if you have unit tests that do not start Embedded Postgres
  explicitly, you might need to terminate Embedded Postgres by calling
  `PostgresClient.stopEmbeddedPostgres`

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
