
# Raml-Module-Builder

Copyright (C) 2016-2019 The Open Library Foundation

This software is distributed under the terms of the Apache License, Version 2.0.
See the file ["LICENSE"](LICENSE) for more information.

<!-- ../okapi/doc/md2toc -l 2 -h 3 README.md -->
* [Introduction](#introduction)
* [Upgrading](#upgrading)
* [Overview](#overview)
* [The basics](#the-basics)
    * [Implement the interfaces](#implement-the-interfaces)
    * [Set up your pom.xml](#set-up-your-pomxml)
    * [Build and run](#build-and-run)
* [Get started with a sample working module](#get-started-with-a-sample-working-module)
* [Command-line options](#command-line-options)
* [Environment Variables](#environment-variables)
* [Local development server](#local-development-server)
* [Creating a new module](#creating-a-new-module)
    * [Step 1: Create new project directory layout](#step-1-create-new-project-directory-layout)
    * [Step 2: Include the jars in your project pom.xml](#step-2-include-the-jars-in-your-project-pomxml)
    * [Step 3: Add the plugins to your pom.xml](#step-3-add-the-plugins-to-your-pomxml)
    * [Step 4: Build your project](#step-4-build-your-project)
    * [Step 5: Implement the generated interfaces](#step-5-implement-the-generated-interfaces)
    * [Step 6: Design the RAML files](#step-6-design-the-raml-files)
* [Adding an init() implementation](#adding-an-init-implementation)
* [Adding code to run periodically](#adding-code-to-run-periodically)
* [Adding a hook to run immediately after verticle deployment](#adding-a-hook-to-run-immediately-after-verticle-deployment)
* [Adding a shutdown hook](#adding-a-shutdown-hook)
* [Implementing file uploads](#implementing-file-uploads)
* [Implement chunked bulk download](#implement-chunked-bulk-download)
* [PostgreSQL integration](#postgresql-integration)
    * [Credentials](#credentials)
    * [Securing DB Configuration file](#securing-db-configuration-file)
    * [Foreign keys constraint](#foreign-keys-constraint)
* [CQL (Contextual Query Language)](#cql-contextual-query-language)
    * [CQL2PgJSON: CQL to PostgreSQL JSON converter](#cql2pgjson-cql-to-postgresql-json-converter)
    * [CQL2PgJSON: Usage](#cql2pgjson-usage)
    * [CQL2PgJSON: id](#cql2pgjson-id)
    * [CQL: Relations](#cql-relations)
    * [CQL: Modifiers](#cql-modifiers)
    * [CQL: Matching, comparing and sorting numbers](#cql-matching-comparing-and-sorting-numbers)
    * [CQL: Matching full text](#cql-matching-full-text)
    * [CQL: Matching all records](#cql-matching-all-records)
    * [CQL: Matching undefined or empty values](#cql-matching-undefined-or-empty-values)
    * [CQL: Matching array elements](#cql-matching-array-elements)
    * [CQL: @-relation modifiers for array searches](#cql--relation-modifiers-for-array-searches)
    * [CQL2PgJSON: Foreign key cross table index queries](#cql2pgjson-foreign-key-cross-table-index-queries)
    * [CQL2PgJSON: Foreign key tableAlias and targetTableAlias](#cql2pgjson-foreign-key-tablealias-and-targettablealias)
    * [CQL2PgJSON: Exceptions](#cql2pgjson-exceptions)
    * [CQL2PgJSON: Unit tests](#cql2pgjson-unit-tests)
* [Tenant API](#tenant-api)
* [RAMLs API](#ramls-api)
* [JSON Schemas API](#json-schemas-api)
* [Query Syntax](#query-syntax)
* [Metadata](#metadata)
* [Facet Support](#facet-support)
* [JSON Schema fields](#json-schema-fields)
* [Overriding RAML (traits) / query parameters](#overriding-raml-traits--query-parameters)
* [Drools integration](#drools-integration)
* [Messages](#messages)
* [Documentation of the APIs](#documentation-of-the-apis)
* [Logging](#logging)
* [Monitoring](#monitoring)
* [Overriding Out of The Box RMB APIs](#overriding-out-of-the-box-rmb-apis)
* [Client Generator](#client-generator)
* [Querying multiple modules via HTTP](#querying-multiple-modules-via-http)
* [A Little More on Validation](#a-little-more-on-validation)
* [Advanced Features](#advanced-features)
* [Additional Tools](#additional-tools)
* [Some REST examples](#some-rest-examples)
* [Additional information](#additional-information)

## Introduction

This documentation includes information about the Raml-Module-Builder (RMB) framework
and examples of how to use it.

The goal of the project is to abstract away as much boilerplate functionality as
possible and allow a developer to focus on implementing business functions. In
other words: **simplify the process of developing a micro service module**.
The framework is RAML driven, meaning a developer / analyst declares APIs that the
'to be developed' module is to expose (via RAML files) and declares the objects
to be used and exposed by the APIs (via JSON schemas). Once the schemas and RAML
files are in place, the framework generates code and offers a number of tools
to help implement the module.
Note that this framework is both a build and a run-time library.


The framework consists of a number of tools:

- `domain-models-api-interfaces` -- project exposes tools that receive as input
  these RAML files and these JSON schemas, and generates java POJOs and java
  interfaces.

- `domain-models-api-aspects` -- project exposes tools that enforce strict
  adherence to the RAML declaration to any API call by exposing validation
  functionality.

    - for example: a RAML file may indicate that a specific parameter is
      mandatory or that a query parameter value must be a specific regex pattern.
      The aspects project handles this type of validation for developers so that it
      does not need to be re-developed over and over. More on validation
      [below](https://github.com/folio-org/raml-module-builder#a-little-more-on-validation).

- `domain-models-runtime` -- project exposes a run-time library which should be
  used to run a module. It is Vert.x based. When a developer implements the
  interfaces generated by the interfaces project, the run-time library should be
  included in the developed project and run. The run-time library will
  automatically map URLs to the correct implemented function so that developers
  only need to implement APIs, and so all the wiring, validation,
  parameter / header / body parsing, logging (every request is logged in an
  apache like format) is handled by the framework. Its goal is to abstract
  away all boilerplate functionality and allow a module implementation to focus
  on implementing business functions.

    - The runtime framework also exposes hooks that allow developers to
      implement one-time jobs, scheduled tasks, etc.

    - Provides tooling (Postgres client, etc.) for developers
      to use while developing their module.

    - Runtime library runs a Vert.x verticle.

- `rules` -- Basic Drools functionality allowing module developers to create
  validation rules via `*.drl` files for objects (JSON schemas).

## Upgrading

See separate [upgrading notes](doc/upgrading.md).

Note: This version of the README is for RMB v20+ version.
If still using older versions, then see the [branch b19](https://github.com/folio-org/raml-module-builder/tree/b19) README.

## Overview

Follow the [Introduction](#introduction) section above to generally understand
the RMB framework.
Review the separate [Okapi Guide and Reference](https://github.com/folio-org/okapi/blob/master/doc/guide.md).
Scan the [Basics](#the-basics) section below for a high-level overview of RMB. Then follow the
[Get started with a sample working module](#get-started-with-a-sample-working-module)
section which demonstrates an already constructed example.
When that is understood, then move on to the section
[Creating a new module](#creating-a-new-module) to get your project started.

Note that actually building this RAML Module Builder framework is not required.
(Some of the images below are out-of-date.) The already published RMB artifacts will
be [incorporated](#step-2-include-the-jars-in-your-project-pomxml) into your project from the repository.

## The basics

![](images/build.png)
![](images/generate.png)
![](images/what.png)

### Implement the interfaces

For example, note the validation annotations generated based on the constraints in the RAML.

![](images/interface_example.png)

- When implementing the interfaces, you must add the @Validate
  annotation to enforce the annotated constraints declared by the interface.

- Note that a Bib entity was passed as a parameter. The runtime framework
  transforms the JSON passed in the body into the correct POJO.


### Set up your pom.xml

- Add the `exec-maven-plugin`. This will generate the POJOs and interfaces based on
  the RAML files.

- Add the `aspectj-maven-plugin`. This is required if you
  would like the runtime framework to validate all URLs.

- Add the `maven-shade-plugin`, indicating the main class to
  run as `RestLauncher` and main verticle as `RestVerticle`. This will create a
  runnable jar with the runtime's `RestVerticle` serving as the main class.

- Add the `maven-resources-plugin`. This will copy
  your RAML files to the /apidocs directory where they will be made visible
  online (html view) by the runtime framework.

These are further explained below.

### Build and run

Do `mvn clean install` ... and run :)

The runtime framework will route URLs in your RAML to the correct method
implementation. It will validate (if `@Validate` was used), log, and expose
various tools.

Notice that no web server was configured or even referenced in the implementing
module - this is all handled by the runtime framework.

Some sample projects:

- https://github.com/folio-org/mod-configuration
- https://github.com/folio-org/mod-notes

and other [modules](https://dev.folio.org/source-code/#server-side) (not all do use the RMB).


## Get started with a sample working module

The [mod-notify](https://github.com/folio-org/mod-notify)
is a full example which uses the RMB. Clone it, and then investigate:

```
$ git clone --recursive https://github.com/folio-org/mod-notify.git
$ cd mod-notify
$ mvn clean install
```

- Its RAMLs and JSON schemas can be found in the `ramls` directory.
These are also displayed as local [API documentation](#documentation-of-the-apis).

- Open the pom.xml file - notice the jars in the `dependencies` section as well as the `plugins` section. The `ramls` directory is declared in the pom.xml and passed to the interface and POJO generating tool via a maven exec plugin. The tool generates source files into the `target/generated-sources/raml-jaxrs` directory. The generated interfaces are implemented within the project in the `org.folio.rest.impl` package.

- Investigate the `src/main/java/org/folio/rest/impl/NotificationsResourceImpl.java` class. Notice that there is a function representing each endpoint that is declared in the RAML file. The appropriate parameters (as described in the RAML) are passed as parameters to these functions so that no parameter parsing is needed by the developer. Notice that the class contains all the code for the entire module. All handling of URLs, validations, objects, etc. is all either in the RMB jars, or generated for this module by the RMB at build time.

- **IMPORTANT NOTE:** Every interface implementation - by any module -
  must reside in package `org.folio.rest.impl`. This is the package that is
  scanned at runtime by the runtime framework, to find the needed runtime
  implementations of the generated interfaces.

Now run the module in standalone mode:

```
$ java -jar target/mod-notify-fat.jar embed_postgres=true
```

Now send some requests using '[curl](https://curl.haxx.se)' or '[httpie](https://httpie.org)'

At this stage there is not much that can be queried, so stop that quick demonstration now.
After explaining general command-line options, etc.
we will get your local development server running and populated with test data.

## Command-line options

- `-Dhttp.port=8080` (Optional -- defaults to 8081)

- `-Ddebug_log_package=*` (Optional -- Set log level to debug for all packages.
Or use `org.folio.rest.*` for all classes within a specific package,
or `org.folio.rest.RestVerticle` for a specific class.)

- `embed_postgres=true` (Optional -- enforces starting an embedded postgreSQL, defaults to false)

- `db_connection=[path]` (Optional -- path to an external JSON config file with
  connection parameters to a PostgreSQL DB)

  - for example Postgres: `{"host":"localhost", "port":5432, "maxPoolSize":50,
    "username":"postgres","password":"mysecretpassword", "database":"postgres",
    "charset":"windows-1252", "queryTimeout" : 10000}`

- `drools_dir=[path]` (Optional -- path to an external drools file. By default,
  `*.drl` files in the `resources/rules` directory are loaded)

- Other module-specific arguments can be passed via the command line in the format key=value. These will be accessible to implementing modules via `RestVerticle.MODULE_SPECIFIC_ARGS` map.

- Optional JVM arguments can be passed before the `-jar` argument, e.g.
`-XX:+HeapDumpOnOutOfMemoryError`
`-XX:+PrintGCDetails`
`-XX:+PrintGCTimeStamps`
`-Xloggc:C:\Git\circulation\gc.log`

## Environment Variables

RMB implementing modules expect a set of environment variables to be passed in at module startup. The environment variables expected by RMB modules are:

 - DB_HOST
 - DB_PORT
 - DB_USERNAME
 - DB_PASSWORD
 - DB_DATABASE
 - DB_QUERYTIMEOUT
 - DB_CHARSET
 - DB_MAXPOOLSIZE

Environment variables with periods/dots in their names are deprecated in RMB because a period is [not POSIX compliant](http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap08.html) and therefore some shells, notably, the BusyBox /bin/sh included in Alpine Linux, strip them (reference: [warning in OpenJDK docs](https://hub.docker.com/_/openjdk/)).

See the [Environment Variables](https://github.com/folio-org/okapi/blob/master/doc/guide.md#environment-variables) section of the Okapi Guide for more information on how to deploy environment variables to RMB modules via Okapi.

## Local development server

To get going quickly with running a local instance of Okapi, adding a tenant and some test data,
and deploying some modules, see
[Running a local FOLIO system](https://dev.folio.org/guides/run-local-folio/).

## Creating a new module

### Step 1: Create new project directory layout

Create the new project using the [normal layout](https://dev.folio.org/guides/commence-a-module/) of files, and basic POM file.

Add the `/ramls` directory, the area for the RAML, schemas, and examples files.
For a maven subproject the directory may be at the parent project only.

To get a quick start, copy the "ramls" directory and POM file from
[mod-notify](https://github.com/folio-org/mod-notify).
(At [Step 6](#step-6-design-the-raml-files) below, these will be replaced to suit your project's needs.)

Adjust the POM file to match your project, e.g. artifactID, version, etc.

### Step 2: Include the jars in your project pom.xml

```xml
  <repositories>
    <repository>
      <id>folio-nexus</id>
      <name>FOLIO Maven repository</name>
      <url>https://repository.folio.org/repository/maven-folio</url>
    </repository>
  </repositories>
  <dependencies>
    <dependency>
      <groupId>org.folio</groupId>
      <artifactId>domain-models-runtime</artifactId>
      <version>20.0.0</version>
    </dependency>
    ...
    ...
  </dependencies>
```

### Step 3: Add the plugins to your pom.xml

Four plugins need to be declared in the POM file:

- The `exec-maven-plugin` which will generate the POJOs and interfaces based on
  the RAML files.

- The `aspectj-maven-plugin` which will pre-compile your code with validation aspects
  provided by the framework - remember the `@Validate` annotation. The
  validation supplied by the framework verifies that headers are passed
  correctly, parameters are of the correct type and contain the correct content
  as indicated by the RAML file.

- The `maven-shade-plugin` which will generate a fat-jar runnable jar.
  The important thing to
  notice is the main class that will be run when running your module. Notice the
  `Main-class` and `Main-Verticle` in the shade plugin configuration.

- The `maven-resources-plugin` which will copy the RAML files into a directory
  under `/apidocs` so that the runtime framework can pick it up and display html
  documentation based on the RAML files.

Add `ramlfiles_path` property indicating the location of the RAML directory.

```xml
  <properties>
    <ramlfiles_path>${basedir}/ramls</ramlfiles_path>
  </properties>
```

Compare the POM with other FOLIO RMB-based modules.

### Step 4: Build your project

Do `mvn clean install`

This should:

- Create java interfaces for each added RAML file.

- Each interface will contain functions to be implemented (each function represents
  an API endpoint declared in the RAML).

- The parameters within each function interface will be annotated with
  validation annotations that were declared in the RAML. So, if a trait was
  marked as mandatory, it will be marked as @NOT_NULL. This is not something that
  needs to be handled by the implementer. This is handled by the framework,
  which handles validation.

- POJOs -- The JSON schemas will be generated into java objects.

- All generated code can be found in the `org.folio.rest.jaxrs` package in the
  `target/generated-sources/raml-jaxrs/` directory.

### Step 5: Implement the generated interfaces

Implement the interfaces associated with the RAML files you created. An
interface is generated for every root endpoint in the RAML files.
For example an
`org.folio.rest.jaxrs.resource.Ebooks` interface will be generated.
Note that the `org.folio.rest.jaxrs.resource` will be the package for every
generated interface.

The implementations must go into the `org.folio.rest.impl` package because RMB's
[RestVerticle](https://github.com/folio-org/raml-module-builder/blob/master/domain-models-runtime/src/main/java/org/folio/rest/RestVerticle.java)
scans this package for a class that implements the required interface.  The class can
have any name.
RMB then uses reflection to invoke the constructor and the method.

See [mod-notify's org.folio.rest.impl package](https://github.com/folio-org/mod-notify/tree/master/src/main/java/org/folio/rest/impl)
for example implementations.

### Step 6: Design the RAML files

It is beneficial at this stage to take some time to design and prepare the RAML files for the project.
Investigate the other FOLIO modules for guidance.
The [mod-notify](https://github.com/folio-org/mod-notify) is an exemplar.

Remove the temporary copy of the "ramls" directory from Step 1, and replace with your own.

Add the shared suite of [RAML utility](https://github.com/folio-org/raml) files
as the "raml-util" directory inside your "ramls" directory:
```
git submodule add https://github.com/folio-org/raml ramls/raml-util
```
The "raml1.0" branch is the current and default branch.

Create JSON schemas indicating the objects exposed by the module.
Use the `description` field alongside the `type` field to explain the content and
usage and to add documentation.

The GenerateRunner automatically dereferences the schema files and places them into the
`target/classes/ramls/` directory. It scans the `${basedir}/ramls/` directory including
subdirectories, if not found then `${basedir}/../ramls/` supporting maven submodules with
common ramls directory.

The documentation of HTTP response codes
is in [HttpStatus.java](util/src/main/java/org/folio/HttpStatus.java)

Use the collection/collection-item pattern provided by the
[collection resource type](https://github.com/folio-org/raml/tree/raml1.0/rtypes) explained
in the [RAML 200 tutorial](https://raml.org/developers/raml-200-tutorial#resource-types).

The RMB does do some validation of RAML files at compile-time.
There are some useful tools to assist with command-line validation,
and some can be integrated with text editors, e.g.
[raml-cop](https://github.com/thebinarypenguin/raml-cop).

See the guide to [Use raml-cop to assess RAML, schema, and examples](https://dev.folio.org/guides/raml-cop/)
and the [Primer for RAML and JSON Schema](https://dev.folio.org/start/primer-raml/) quick-start document.

RAML-aware text editors are very helpful, such as
[api-workbench](https://github.com/mulesoft/api-workbench) for Atom.

Remember that the POM configuration enables viewing your RAML and interacting
with your application via the local [API documentation](#documentation-of-the-apis).

## Adding an init() implementation

It is possible to add custom code that will run once before the application is deployed
(e.g. to init a DB, create a cache, create static variables, etc.) by implementing
the `InitAPIs` interface. You must implement the
`init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> resultHandler)`. Only one implementation per module is supported.
Currently the implementation should sit in the
`org.folio.rest.impl` package in the implementing project. The implementation
will run during verticle deployment. The verticle will not complete deployment
until the init() completes. The init() function can do anything basically, but
it must call back the Handler. For example:

```java
public class InitAPIs implements InitAPI {

  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> resultHandler){
    try {
      sayHello();
      resultHandler.handle(io.vertx.core.Future.succeededFuture(true));
    } catch (Exception e) {
      e.printStackTrace();
      resultHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
    }
  }
}
```


## Adding code to run periodically

It is possible to add custom code that will run periodically. For example,
to ongoingly check status of something in the system and act upon that.
Need to implement the PeriodicAPI interface:

```java
public interface PeriodicAPI {
  /** this implementation should return the delay in which to run the function */
  public long runEvery();
  /** this is the implementation that will be run every runEvery() milliseconds*/
  public void run(Vertx vertx, Context context);

}
```

For example:

```java
public class PeriodicAPIImpl implements PeriodicAPI {


  @Override
  public long runEvery() {
    return 45000;
  }

  @Override
  public void run(Vertx vertx, Context context) {
    try {
      InitAPIs.amIMaster(vertx, context, v-> {
        if(v.failed()){
          //TODO - what should be done here?
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
```

There can be multiple implementations of the periodic hook, all will be called by the RMB framework.


## Adding a hook to run immediately after verticle deployment

It is possible to add custom code that will be run immediately after the verticle running the module is deployed.

```java
public interface PostDeployVerticle {

  /** this implementation will be run immediately after the verticle is initially deployed. Failure does not stop
   * deployment success. The implementing function MUST call the resultHandler to pass back
   * control to the verticle, like so: resultHandler.handle(io.vertx.core.Future.succeededFuture(true));
   * if not, this function will hang the verticle during deployment */
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> resultHandler);

}
```

An implementation example:

```java
public class InitConfigService implements PostDeployVerticle {

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {

    System.out.println("Getting secret key to decode DB password.");
    /** hard code the secret key for now - in production env - change this to read from a secure place */
    String secretKey = "b2%2BS%2BX4F/NFys/0jMaEG1A";
    int port = context.config().getInteger("http.port");
    AdminClient ac = new AdminClient("http://localhost:" + port, null);
    ac.postSetAESKey(secretKey, reply -> {
      if(reply.statusCode() == 204){
        handler.handle(io.vertx.core.Future.succeededFuture(true));
      }
      else{
        handler.handle(io.vertx.core.Future.failedFuture(reply.statusCode() + ", " + reply.statusMessage()));
      }
    });
  }

}
```

## Adding a shutdown hook

It is possible to add custom code that will run just before the verticle is
undeployed and the JVM stopped. This will occur on graceful shutdowns, but can
not be guaranteed to run if the JVM is forcefully shutdown.

The interface to implement:

```java
public interface ShutdownAPI {

  public void shutdown(Vertx vertx, Context context, Handler<AsyncResult<Void>> handler);

}
```

An implementation example:

```java
public class ShutdownImpl implements ShutdownAPI {

  @Override
  public void shutdown(Vertx vertx, Context context, Handler<AsyncResult<Void>> handler) {
    try {
      AuditLogger.getInstance().publish(new LogRecord(Level.INFO, "closing audit logger"));
      AuditLogger.getInstance().close();
      handler.handle(io.vertx.core.Future.succeededFuture());
    }
    catch (Exception e) {
      e.printStackTrace();
      handler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
    }
  }
}
```



Note that when implementing the generated interfaces it is possible to add a constructor to the implementing class. This constructor will be called for every API call. This is another way you can implement custom code that will run per request.


## Implementing file uploads

The RMB supports several methods to upload files and data. The implementing module can use the `multipart/form-data` header or the `application/octet-stream` header to indicate that the HTTP request is an upload content request.

#### File uploads Option 1

A multipart RAML declaration may look something like this:

```raml
/uploadmultipart:
    description: Uploads a file
    post:
      description: |
          Uploads a file
      body:
        multipart/form-data:
          formParameters:
            file:
              description: The file to be uploaded
              required: true
              type: file
```

The body content would look something like this:

```sh
------WebKitFormBoundaryNKJKWHABrxY1AdmG
Content-Disposition: form-data; name="config.json"; filename="kv_configuration.sample"
Content-Type: application/octet-stream

<file content 1>

------WebKitFormBoundaryNKJKWHABrxY1AdmG
Content-Disposition: form-data; name="sample.drl"; filename="Sample.drl"
Content-Type: application/octet-stream

<file content 2>

------WebKitFormBoundaryNKJKWHABrxY1AdmG
```

There will be a `MimeMultipart` parameter passed into the generated interfaces. An implementing
module can access its content in the following manner:

```sh
int parts = entity.getCount();
for (int i = 0; i < parts; i++) {
        BodyPart part = entity.getBodyPart(i);
        Object o = part.getContent();
}
```

where each section in the body (separated by the boundary) is a "part".

An octet/stream can look something like this:

```raml
 /uploadOctet:
    description: Uploads a file
    post:
      description: |
          Uploads a file
      body:
        application/octet-stream:
```

The interfaces generated from the above will contain a parameter of type `java.io.InputStream`
representing the uploaded file.


#### File uploads Option 2

The RMB allows for content to be streamed to a specific implemented interface.
For example, to upload a large file without having to save it all in memory:

 - Mark the function to handle the upload with the `org.folio.rest.annotations.Stream` annotation `@Stream`.
 - Declare the RAML as receiving `application/octet-stream` (see Option 1 above)

The RMB will then call the function every time a chunk of data is received.
This means that a new Object is instantiated by the RMB for each chunk of
data, and the function of that object is called with the partial data included in a `java.io.InputStream` object.

For each invocation, RMB adds header `streamed_id` which will be unique
for the current stream. For the last invocation, header `complete` is supplied
to indicate "end-of-stream".

As of RMB 23.12.0 and later, if an HTTP client prematurely closes the upload
before complete, the handler will be called with `streamed_abort`.

## Implement chunked bulk download

RMB supports bulk downloads of chunks using [CQL](#cql-contextual-query-language) ordered by primary key id (since version 25).

* 1st CQL query: `cql.allRecords=1 sortBy id`
* 2nd CQL query: `id > [last id from 1st CQL query] sortBy id`
* 3rd CQL query: `id > [last id from 2nd CQL query] sortBy id`
* ...

The chunk size is set using the API's limit parameter, for example `limit=10000`
for chunks of 10000 records each.

## PostgreSQL integration

The PostgreSQL connection parameters locations are searched in this order:

- [DB_* environment variables](#environment-variables)
- Configuration file, defaults to `resources/postgres-conf.json` but can be set via [command-line options](#command-line-options)
- Embedded PostgreSQL using [default credentials](#credentials)

By default an embedded PostgreSQL is included in the runtime, but it is only run if neither DB_* environment variables
nor a postgres configuration file are present. To start an embedded PostgreSQL using connection parameters from the
environment variables or the configuration file add `embed_postgres=true` to the command line
(`java -jar mod-notify-fat.jar embed_postgres=true`). Use PostgresClient.setEmbeddedPort(int) to overwrite the port.

The runtime framework exposes a PostgreSQL async client which offers CRUD
operations in an ORM type fashion.
https://github.com/folio-org/raml-module-builder/blob/master/domain-models-runtime/src/main/java/org/folio/rest/persist/PostgresClient.java

**Important Note:** The PostgreSQL client currently implemented assumes
JSONB tables in PostgreSQL. This is not mandatory and developers can work with
regular PostgreSQL tables but will need to implement their own data access
layer.

**Important Note:** For performance reasons the Postgres client will return accurate counts for result sets with less than 50,000 results. Queries with over 50,000 results will return an estimated count.

**Important Note:** The embedded Postgres can not run as root.

**Important Note:** The embedded Postgres relies on the `en_US.UTF-8` (*nix) / `american_usa` (win) locale. If this locale is not installed the Postgres will not start up properly.

**Important Note:** Currently we only support Postgres version 10. We cannot use version 11 because of reduced platform support of postgresql-embedded ([postgresql-embedded supported versions](https://github.com/yandex-qatools/postgresql-embedded/commit/15685611972bacd8ba61dd7f11d4dbdcb3ba8dc1), [PostgreSQL Database Download](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads)).

The PostgresClient expects tables in the following format:

```sql
create table <schema>.<table_name> (
  id UUID PRIMARY KEY,
  jsonb JSONB NOT NULL
);
```

This means that all the fields in the JSON schema (representing the JSON object) **are** the "jsonb" (column) in the Postgres table.

#### Saving binary data

As indicated, the PostgresClient is jsonb oriented. If there is a need to store data in binary form, this can be done in the following manner (only id based upsert is currently supported):
```
byte[] data = ......;
JsonArray jsonArray = new JsonArray().add(data);
client.upsert(TABLE_NAME, id, jsonArray, false, replyHandler -> {
.....
});
```
### Credentials

When running in embedded mode, credentials are read from `resources/postgres-conf.json`. If a file is not found, then the following configuration will be used by default:

```
port: 6000
host: 127.0.0.1
username: username
password: password
database: postgres
```

### Securing DB Configuration file

As previously mentioned, the Postgres Client supplied by the RMB looks for a file called `postgres-conf.json`. However, leaving a file which contains the DB password to a superuser in plain text on the server is not a good idea. It is possible to encrypt the password in the file. The encryption should be an AES encryption (symmetric block cipher). This encryption is done with a secret key.

Meaning: password in plain text + secret key = encrypted password

The RMB comes with an AES class that supports generating secret keys, encrypting and decrypting them, https://github.com/folio-org/raml-module-builder/blob/master/domain-models-runtime/src/main/java/org/folio/rest/security/AES.java

Note that the use of this class is optional.

To work with an encrypted password the RMB exposes an API that can be used to set the secret key (stored only in memory). When creating the DB connection the RMB will check to see if the secret key has been set. If the secret key has been set, the RMB will decrypt the password with the secret key, and use the decrypted password to connect to the DB. Otherwise it will assume an un-encrypted password, and will connect using that password as-is.
A module can also set the secret key via the static method `AES.setSecretKey(mykey)`

The needed steps are:

 -  Generate a key
 -  Encrypt a password
 -  Include that password in the config file
 -  Either call `AES.setSecretKey(mykey)` or the `admin/set_AES_key` API (to load the secret key into memory)

A good way for a module to set the secret key is by using the post deployment hook interface in the RMB.

```java
public class InitConfigService implements PostDeployVerticle {
  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
    System.out.println("Getting secret key to decode DB password.");
    //** hard code the secret key  - in production env - read from a secure place *//
    String secretKey = "b2%2BS%2BX4F/NFys/0jMaEG1A";
    int port = context.config().getInteger("http.port");
    AdminClient ac = new AdminClient("http://localhost:" + port, null);
    ac.postSetAESKey(secretKey, reply -> {
      if(reply.statusCode() == 204){
        handler.handle(io.vertx.core.Future.succeededFuture(true));
      }
      else{
        handler.handle(io.vertx.core.Future.failedFuture(reply.statusCode() + ", " + reply.statusMessage()));
      }
    });
    handler.handle(io.vertx.core.Future.succeededFuture(true));
  }
}
```

### Foreign keys constraint

Use `foreignKeys` in schema.json of the Tenant API to automatically create the following columns and triggers.

PostgreSQL does not directly support a foreign key constraint (referential integrity) of a field inside the JSONB. Therefore an additional column with the foreign key constraint, and a trigger to keep it in sync with the value inside the JSONB, are created.

Example:

```sql
CREATE TABLE item (
  id UUID PRIMARY KEY,
  jsonb JSONB NOT NULL,
  permanentLoanTypeId UUID REFERENCES loan_type,
  temporaryLoanTypeId UUID REFERENCES loan_type
);
CREATE OR REPLACE FUNCTION update_item_references()
RETURNS TRIGGER AS $$
BEGIN
  NEW.permanentLoanTypeId = NEW.jsonb->>'permanentLoanTypeId';
  NEW.temporaryLoanTypeId = NEW.jsonb->>'temporaryLoanTypeId';
  RETURN NEW;
END;
$$ language 'plpgsql';
CREATE TRIGGER update_item_references
  BEFORE INSERT OR UPDATE ON item
  FOR EACH ROW EXECUTE PROCEDURE update_item_references();
```

The overhead of this trigger and foreign key constraint reduces the number of UPDATE transactions per second on this table by about 10% (when tested against an external stand alone Postgres database).  See
https://github.com/folio-org/raml-module-builder/blob/master/domain-models-runtime/src/test/java/org/folio/rest/persist/ForeignKeyPerformanceIT.java
for the performance test.  Doing the foreign key check manually by sending additional SELECT queries takes much more time than 10%.

See also [foreign key CQL support](#cql2pgjson-foreign-key-cross-table-index-queries).

## CQL (Contextual Query Language)

Further [CQL](https://dev.folio.org/reference/glossary/#cql) information.

### CQL2PgJSON: CQL to PostgreSQL JSON converter

The source code is at [./cql2pgjson](cql2pgjson) and [./cql2pgjson-cli](cql2pgjson-cli)

### CQL2PgJSON: Usage

Invoke like this:

    // users.user_data is a JSONB field in the users table.
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("users.user_data");
    String cql = "name=Miller";
    String where = cql2pgJson.cql2pgJson(cql);
    String sql = "select * from users where " + where;
    // select * from users
    // where CAST(users.user_data->'name' AS text)
    //       ~ '(^|[[:punct:]]|[[:space:]])Miller($|[[:punct:]]|[[:space:]])'

Or use `toSql(String cql)` to get the `ORDER BY` clause separately:

    CQL2PgJSON cql2pgJson = new CQL2PgJSON("users.user_data");
    String cql = "name=Miller";
    SqlSelect sqlSelect = cql2pgJson.toSql(cql);
    String sql = "select * from users where " + sqlSelect.getWhere()
                               + " order by " + sqlSelect.getOrderBy();


Setting server choice indexes is possible, the next example searches `name=Miller or email=Miller`:

    CQL2PgJSON cql2pgJson = new CQL2PgJSON("users.user_data", Arrays.asList("name", "email"));
    String cql = "Miller";
    String where = cql2pgJson.cql2pgJson(cql);
    String sql = "select * from users where " + where;

Searching across multiple JSONB fields works like this. The _first_ json field specified
in the constructor will be applied to any query arguments that aren't prefixed with the appropriate
field name:

    // Instantiation
    CQL2PgJSON cql2pgJson = new CQL2PgJSON(Arrays.asList("users.user_data","users.group_data"));

    // Query processing
    where = cql2pgJson.cql2pgJson( "users.user_data.name=Miller" );
    where = cql2pgJson.cql2pgJson( "users.group_data.name==Students" );
    where = cql2pgJson.cql2pgJson( "name=Miller" ); // implies users.user_data

### CQL2PgJSON: id

The UUID field id is not searched in the JSON but in the table's primary key field. PostgreSQL automatically
creates an index for the primary key.

`=`, `==`, `<>`, `>`, `>=`, `<`, and `<=` relations are supported for comparison with a valid UUID.

`=`, `==`, and `<>` relations allow `*` for right truncation.

Modifiers are forbidden.

### CQL: Relations

Only these relations have been implemented yet:

* `=` (this is `==` for number matching and `adj` for a string matching.
       Examples 1: `height =/number 3.4` Example 2: `title = Potter`)
* `==` (exact match, for example `barcode == 883746123` or exact substring match `title == "Harry Potter*"`;
        `==/number` matches any form: 3.4 = 3.400 = 0.34e1)
* `all` (each word of the query string exists somewhere, `title all "Potter Harry"` matches "Harry X. Potter")
* `any` (any word of the query string exists somewhere, `title any "Potter Foo"` matches "Harry Potter")
* `adj` (substring phrase match: all words of the query string exist consecutively in that order, there may be any
          whitespace and punctuation in between, `title adj "Harry Potter"` matches "Harry - . - Potter")
* `>` `>=` `<` `<=` `<>` (comparison for both strings and numbers)

Note to mask the CQL special characters by prepending a backslash: * ? ^ " \

Use quotes if the search string contains a space, for example `title = "Harry Potter"`.

### CQL: Modifiers

Matching modifiers: Only `masked` is implemented, not `unmasked`, `regexp`,
`honorWhitespace`, `substring`.

Word begin and word end in JSON is only detected at whitespace and punctuation characters
from the ASCII charset, not from other Unicode charsets.

### CQL: Matching, comparing and sorting numbers

Add the /number modifier to enable number matching, comparing and sorting, for example `age ==/number 18`,
`age >=/number 21` and `sortBy age/number`.

3.4, 3.400, and 0.34e1 match each other when using `==/number`, and 2 is smaller than 19
(in contrast to string comparison where "2" > "19").

This requires that the value has been stored as a JSONB number (`{"age": 19}`)
and not as a JSONB string (`{"age": "19"}`).

### CQL: Matching full text

See [PostgreSQL's tsvector full text parser documentation](https://www.postgresql.org/docs/current/textsearch-parsers.html)
how word splitting works when using a full text index. Some notable consequences:

CQL `field adj "bar"` matches `bar`, `bar-baz`, `foo-bar-baz`.

CQL `field adj "bar baz"` matches `bar baz`, `bar-baz`, `foo-bar-baz`, `foo-bar baz`, `bar-baz-foo`.

CQL `field adj "bar-baz"` matches `bar-baz`, but neither `bar baz` nor `foo-bar-baz` nor `foo-bar baz` nor `bar-baz-foo`.

`foo/bar/baz` is a single word, while `foo//bar//baz`, `foo///bar///baz`, `foo////bar////baz`, etc.
are split into the three words `foo`, `/bar`, and `/baz` (always reduced to a single slash).

### CQL: Matching all records

A search matching all records in the target index can be executed with a
`cql.allRecords=1` query. `cql.allRecords=1` can be used alone or as part of
a more complex query, for example
`cql.allRecords=1 NOT name=Smith sortBy name/sort.ascending`

* `cql.allRecords=1 NOT name=Smith` matches all records where name does not contain Smith
   as a word or where name is not defined.
* `name="" NOT name=Smith` matches all records where name is defined but does not contain
   Smith as a word.
* For performance reasons, searching for `*` in any fulltext field will match all records as well.

### CQL: Matching undefined or empty values

A relation does not match if the value on the left-hand side is undefined. (but see the fulltext
`*` case above).
A negation (using NOT) of a relation matches if the value on the left-hand side is
not defined or if it is defined but doesn't match.

* `name=""` matches all records where name is defined.
* `cql.allRecords=1 NOT name=""` matches all records where name is not defined.
* `name==""` matches all records where name is defined and empty.
* `cql.allRecords=1 NOT name==""` matches all records where name is defined and not empty or
   where name is not defined.
* `name="" NOT name==""` matches all records where name is defined and not empty.

### CQL: Matching array elements

For matching the elements of an array use these queries (assuming that lang is either an array or not defined, and assuming
an array element value does not contain double quotes):
* `lang ==/respectAccents []` for matching records where lang is defined and an empty array
* `cql.allRecords=1 NOT lang <>/respectAccents []` for matching records where lang is not defined or an empty array
* `lang =/respectCase/respectAccents \"en\"` for matching records where lang is defined and contains the value en
* `cql.allRecords=1 NOT lang =/respectCase/respectAccents \"en\"` for matching records where lang does not
  contain the value en (including records where lang is not defined)
* `lang = "" NOT lang =/respectCase/respectAccents \"en\"` for matching records where lang is defined and
  and does not contain the value en
* `lang = ""` for matching records where lang is defined
* `cql.allRecords=1 NOT lang = ""` for matching records where lang is not defined
* `identifiers == "*\"value\": \"6316800312\", \"identifierTypeId\": \"8261054f-be78-422d-bd51-4ed9f33c3422\"*"`
  (note to use `==` and not `=`) for matching the ISBN 6316800312 using ISBN's identifierTypeId where each element of
  the identifiers array is a JSON object with the two keys value and identifierTypeId, for example

      "identifiers": [ {
        "value": "(OCoLC)968777846", "identifierTypeId": "7e591197-f335-4afb-bc6d-a6d76ca3bace"
      }, {
        "value": "6316800312", "identifierTypeId": "8261054f-be78-422d-bd51-4ed9f33c3422"
      } ]

To avoid the complicated syntax all ISBN values or all values can be extracted and used to create a view or an index:

    SELECT COALESCE(jsonb_agg(value), '[]')
       FROM jsonb_to_recordset(jsonb->'identifiers')
         AS y(key text, value text)
       WHERE key='8261054f-be78-422d-bd51-4ed9f33c3422'

    SELECT COALESCE(jsonb_agg(value), '[]')
      FROM jsonb_to_recordset(jsonb->'identifiers')
        AS x(key text, value text)
      WHERE value IS NOT NULL

### CQL: @-relation modifiers for array searches

RMB 26 or later supports array searches with relation modifiers, that
are particular suited for structures like:

    "property" : [
      {
        "type1" : "value1",
        "type2" : "value2",
        "subfield": "value"
      },
      ...
    ]

An example of this kind of structure is `contributors ` (property) from
mod-inventory-storage . `contributorTypeId` is the type of contributor
(type1).

With CQL you can limit searches to `property1` with regular match in
`subfield`, with type1=value2 with

    property =/@type1=value1 value

Observe that the relation modifier is preceeded with the @-character to
avoid clash with other CQL relation modifiers.

The type1, type2 and subfield must all be defined in schema.json, because
the JSON schema is not known. And also because relation modifiers are
unfortunately lower-cased by cqljava. To match value1 against the
property contents of type1, full-text match is used.

Multiple relation modifiers with value are ANDed together. So

    property =/@type1=value1/@type2=value2 value

will only give a hit if both type1 has value1 AND type2 has value2.

It is also possible to specify relation modifiers without value. This
essentially is a way to override what subfield to search. In this case
the right hand side term is matched. Multiple relation modifiers
are OR'ed together. For example:

    property =/@type1 value

And to match any of the sub properties type1, type2, you could use:

    property =/@type1/@type2 value

In schema.json two new properties, `arraySubfield` and `arrayModifiers`,
specifies the subfield and the list of modifiers respectively.
This can be applied to `ginIndex` and `fullTextIndex`.
schema.json example:

    {
      "fieldName": "property",
      "tOps": "ADD",
      "caseSensitive": false,
      "removeAccents": true,
      "arraySubfield": "subfield",
      "arrayModifiers": ["type1", "type2"]
    }

For the identifiers example we could define things in schema.json with:

    {
      "fieldName": "identifiers",
      "tOps": "ADD",
      "arraySubfield": "value",
      "arrayModifiers": ["identifierTypeId"]
    }

This will allow you to perform searches, such as:

    identifiers = /@identifierTypeId=7e591197-f335-4afb-bc6d-a6d76ca3bace 6316800312

### CQL2PgJSON: Foreign key cross table index queries

CQL2PgJSON supports cross table joins via subquery based on foreign keys.
This allows arbitrary depth relationships in both child-to-parent and parent-to-child direction.

Example relationship: item → holdings_record → instance

Join conditions of this example:
* item.holdingsRecordId = holdings_record.id
* holdings_record.instanceId = instance.id

The field in the child table points to the primary key `id` field of the parent table; the parent table is also called the target table.

* Precede the index you want to search with the table name in camelCase, e.g. `instance.title = "bee"`.
* There is no change with child table fields, use them in the regular way without table name prefix.
* The target table index field must have an index declared in the schema.json file.
* For a multi-table join use `targetPath` instead of `fieldName` and put the list of field names into the `targetPath` array.
* Use `= *` to check whether a join record exists. This runs a cross index join with no further restriction, e.g. `instance.id = *`.
* The schema for the above example:
```
{
  "tables": [
    {
      "tableName": "instance",
      "index": [
        {
          "fieldName": "title",
          "tOps": "ADD",
          "caseSensitive": false,
          "removeAccents": true
        }
      ]
    },
    {
      "tableName": "holdings_record",
      "foreignKeys": [
        {
          "fieldName": "instanceId",
          "targetTable":      "instance",
          "targetTableAlias": "instance",
          "tableAlias": "holdingsRecord",
          "tOps": "ADD"
        }
      ]
    },
    {
      "tableName": "item",
      "foreignKeys": [
        {
          "fieldName": "holdingsRecordId",
          "targetTable":      "holdings_record",
          "targetTableAlias": "holdingsRecord",
          "tableAlias": "item",
          "tOps": "ADD"
        },
        {
          "targetPath": ["holdingsRecordId", "instanceId"],
          "targetTable":      "instance",
          "targetTableAlias": "instance",
          "tableAlias": "item"
        }
      ]
    }
  ]
}
```

### CQL2PgJSON: Foreign key tableAlias and targetTableAlias

The property `targetTableAlias` enables that parent table name in CQL queries against the current child table.

The property `tableAlias` enables that child table name in CQL queries against the target/parent table.

If any of these two properties is missing, then that respective foreign key join syntax is disabled.

The name may be different from the table name (`tableName`, `targetTable`). One use case is to change to camelCase, e.g.
`"targetTable": "holdings_record"` and `"targetTableAlias": "holdingsRecord"`. Another use case is
to resolve ambiguity when two foreign keys point to the same target table, example:
```
    {
      "tableName": "item",
      "foreignKeys": [
        {
          "fieldName": "permanentLoanTypeId",
          "tableAlias": "itemWithPermanentLoanType",
          "targetTable": "loan_type",
          "targetTableAlias": "loanType",
          "tOps": "ADD"
        },
        {
          "fieldName": "temporaryLoanTypeId",
          "tableAlias": "itemWithTemporaryLoanType",
          "targetTable": "loan_type",
          "targetTableAlias": "temporaryLoanType",
          "tOps": "ADD"
        }
      ]
    }
```
Running CQL `loanType.name == "Can circulate"` against the item endpoint returns all items where the item's permanentLoanTypeId points to a loan_type where the loan_type's name equals "Can circulate".

Running CQL `temporaryLoanType.name == "Can circulate"` against the item endpoint returns all items where the item's temporaryLoanTypeId points to a loan_type where the loan_type's name equals "Can circulate".

Running CQL `itemWithPermanentLoanType.status == "In transit"` against the loan_type endpoint returns all loan_types where there exists an item that has this loan_type as a permanentLoanType and where the item's status equals "In transit".

Running CQL `itemWithTemporaryLoanType.status == "In transit"` against the loan_type endpoint returns all loan_types where there exists an item that has this loan_type as a temporaryLoanType and where the item's status equals "In transit".

### CQL2PgJSON: Exceptions

All locally produced Exceptions are derived from a single parent so they can be caught collectively
or individually. Methods that load a JSON data object model pass in the identity of the model as a
resource file name, and may also throw a native `java.io.IOException`.

    CQL2PgJSONException
      ├── FieldException
      ├── SchemaException
      ├── ServerChoiceIndexesException
      ├── CQLFeatureUnsupportedException
      └── QueryValidationException
            └── QueryAmbiguousException

### CQL2PgJSON: Unit tests

To run the unit tests in your IDE, the Unicode input files must have been produced by running maven.
In Eclipse you may use "Run as ... Maven Build" for doing so.

## Tenant API

The Postgres Client support in the RMB is schema specific, meaning that it expects every tenant to be represented by its own schema. The RMB exposes three APIs to facilitate the creation of schemas per tenant (a type of provisioning for the tenant). Post, Delete, and 'check existence' of a tenant schema. Note that the use of this API is optional.

The RAML defining the API:

   https://github.com/folio-org/raml/blob/raml1.0/ramls/tenant.raml

By default RMB includes an implementation of the Tenant API which assumes Postgres being present. Implementation in
 [TenantAPI.java](https://github.com/folio-org/raml-module-builder/blob/master/domain-models-runtime/src/main/java/org/folio/rest/impl/TenantAPI.java) file. You might want to extend/override this because:

1. You want to not call it at all (your module is not using Postgres).
2. You want to provide further Tenant control, such as loading reference and/or sample data.

#### Extending the Tenant Init

In order to implement your tenant API, extend `TenantAPI` class:

```java
package org.folio.rest.impl;
import javax.ws.rs.core.Response;
import org.folio.rest.jaxrs.model.TenantAttributes;

public class MyTenantAPI extends TenantAPI {
 @Override
  public void postTenant(TenantAttributes ta, Map<String, String> headers,
    Handler<AsyncResult<Response>> hndlr, Context cntxt) {

    ..
    }
  @Override
  public void getTenant(Map<String, String> map, Handler<AsyncResult<Response>> hndlr, Context cntxt) {
    ..
  }
  ..
}

```

If you wish to call the Post Tenant API (with Postgres) then just call the corresponding super-class, e.g.:
```java
@Override
public void postTenant(TenantAttributes ta, Map<String, String> headers,
  Handler<AsyncResult<Response>> hndlr, Context cntxt) {
  super.postTenant(ta, headers, hndlr, cntxt);
}
```
(not much point in that though - it would be the same as not defining it at all).

If you wish to load data for your module, that should be done after the DB has been successfully initialized,
e.g. do something like:
```
public void postTenant(TenantAttributes ta, Map<String, String> headers,
  super.postTenant(ta, headers, res -> {
    if (res.failed()) {
      hndlr.handle(res);
      return;
    }
    // load data here
    hndlr.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse
      .respond201WithApplicationJson("")));
  }, cntxt);
}
```

There is no right way to load data, but consider that data load will be both happening for first time tenant
usage of the module and during an upgrade process. Your data loading should be idempotent. If files are stored
as resources and as JSON files, you can use the TenantLoading utility.

```java
import org.folio.rest.tools.utils.TenantLoading;

public void postTenant(TenantAttributes ta, Map<String, String> headers,
  super.postTenant(ta, headers, res -> {
    if (res.failed()) {
      hndlr.handle(res);
      return;
    }
    TenantLoading tl = new TenantLoading();
    // two sets of reference data files
    // resources ref-data/data1 and ref-data/data2 .. loaded to
    // okapi-url/instances and okapi-url/items respectively
    tl.withKey("loadReference").withLead("ref-data")
      .withIdContent().
      .add("data1", "instances")
      .add("data2", "items");
    tl.perform(ta, headers, vertx, res1 -> {
      if (res1.failed()) {
        hndlr.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse
          .respond500WithTextPlain(res1.cause().getLocalizedMessage())));
        return;
      }
      hndlr.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse
        .respond201WithApplicationJson("")));
    });
  }, cntxt);
}
```

If data is already in resources, then fine. If not, for example, if in root of
project in project, then copy it with maven-resource-plugin. For example, to
copy `reference-data` to `ref-data` in resources:

```xml
<execution>
  <id>copy-reference-data</id>
  <phase>process-resources</phase>
  <goals>
    <goal>copy-resources</goal>
  </goals>
  <configuration>
    <outputDirectory>${basedir}/target/classes/ref-data</outputDirectory>
    <resources>
      <resource>
        <directory>${basedir}/reference-data</directory>
        <filtering>true</filtering>
      </resource>
    </resources>
  </configuration>
</execution>
```


#### The Post Tenant API

The Postgres based Tenant API implementation will look for a file at `/resources/templates/db_scripts/`
called **schema.json**

The file contains an array of tables and views to create for a tenant on registration (tenant api post)

An example can be found here:

 - https://github.com/folio-org/raml-module-builder/blob/master/domain-models-runtime/src/main/resources/templates/db_scripts/examples/schema.json.example.json

Entries in the json file to be aware of:

For each **table**:

1. `tableName` - name of the table that will be generated - this is the table that should be referenced from the code
2. `generateId` - No longer supported.  This functionality is not stable in Pgpool-II see https://www.pgpool.net/docs/latest/en/html/restrictions.html.  The solution is to generate a UUID in java in the same manner as https://github.com/folio-org/raml-module-builder/blob/v23.11.0/domain-models-runtime/src/main/java/org/folio/rest/persist/PgUtil.java#L358
3. `fromModuleVersion` - this field indicates the version in which the table was created / updated in. When a tenant update is requested - only versions older than the indicated version will generate the declared table. This ensures that if a module upgrades from an older version, the needed tables will be generated for it, however, subsequent upgrades from versions equal or later than the version indicated for the table will not re-generate the table.
    * Note that this is enforced for all tables, views, indexes, FK, triggers, etc. (via the `IF NOT EXISTS` sql Postgres statement)
4. `mode` - should be used only to indicate `delete`
5. `withMetadata` - will generate the needed triggers to populate the metadata section in the json on update / insert
6. `likeIndex` - indicate which fields in the json will be queried using the `LIKE`. Needed for fields that will be faceted on.
    * `fieldName` the field name in the json for which to create the index
    * the `tOps` indicates the table operation - ADD means to create this index, DELETE indicates this index should be removed
    * the `caseSensitive` allows you to create case insensitive indexes (boolean true / false), if you have a string field that may have different casings and you want the value to be unique no matter the case. Defaults to false.
    *  `removeAccents` - normalize accents or leave accented chars as is. Defaults to true.
    * the `whereClause` allows you to create partial indexes, for example:  "whereClause": "WHERE (jsonb->>'enabled')::boolean = true"
    * `stringType` - defaults to true - if this is set to false than the assumption is that the field is not of type text therefore ignoring the removeAccents and caseSensitive parameters.
    * `arrayModifiers` - specifies array relation modifiers supported for some index. The modifiers must exactly match the name of the property in the JSON object within the array.
    * `arraySubfield` - is the key of the object that is used for the primary term when array relation modifiers are in use. This is typically also defined when `arrayModifiers` are also defined.
7. `ginIndex` - generate an inverted index on the JSON using the `gin_trgm_ops` extension. Allows for regex queries to run in an optimal manner (similar to a simple search engine). Note that the generated index is large and does not support the equality operator (=). See the `likeIndex` for available options (except that partial indexes are not supported, i.e. using `whereClause`). The `removeAccents` is set to true and is not case sensitive.
8. `uniqueIndex` - create a unique index on a field in the JSON
    * the `tOps` indicates the table operation - ADD means to create this index, DELETE indicates this index should be removed
    * the `whereClause` allows you to create partial indexes, for example:  "whereClause": "WHERE (jsonb->>'enabled')::boolean = true"
    * See additional options in the likeIndex section above
9. `index` - create a btree index on a field in the JSON
    * the `tOps` indicates the table operation - ADD means to create this index, DELETE indicates this index should be removed
    * the `whereClause` allows you to create partial indexes, for example:  "whereClause": "WHERE (jsonb->>'enabled')::boolean = true"
    * See additional options in the likeIndex section above
10. `fullTextIndex` - create a full text index using the tsvector features of postgres.
    * `removeAccents` can be used, the default `caseSensitive: false` cannot be changed because tsvector always converts to lower case.
    * See [CQL: Matching full text](#cql-matching-full-text) to learn how word splitting works.
    * The `tOps` is optional (like for all indexes), and defaults to ADDing the index.
    * `whereClause` and `stringType` work as for `likeIndex` above.
11. `withAuditing` - Creates an auditing table and a trigger that populates the audit table with the history of the table record whenever an insert, update, or delete occurs. `"withAuditing": true` for enabled, `false` or undefined for disabled.
    * `auditingTableName` The name of the audit table.
    * `auditingFieldName` The field (JSON property) in the audit record that contains the copy of the original record.
    * `"withAuditing": true` automatically creates the auditing table; an entry of the audit table in the "tables" section of schema.json is optional, for example to create indexes.
    * The `auditingSnippet` section allows some customizations to the auditing function with custom SQL in the declare section and the body (for either insert / update / delete).
    * The audit table jsonb column has three fields: `$auditingFieldName` contains the original record (jsonb from the original table), `id` contains a new unique id, `operation` contains `I`, `U`, `D` for insert, update, delete, and `createdDate` contains the time when the audit record was created.
12. `foreignKeys` - adds / removes foreign keys (trigger populating data in a column based on a field in the JSON and creating a FK constraint)
13. `customSnippetPath` - a relative path to a file with custom SQL commands for this specific table
14. `deleteFields` / `addFields` - delete (or add with a default value), a field at the specified path for all JSON entries in the table
15. `populateJsonWithId` - This schema.json entry and the disable option is no longer supported. The primary key is always copied into `jsonb->'id'` on each insert and update.
16. `pkColumnName` - No longer supported. The name of the primary key column is always `id` and is copied into `jsonb->'id'` in each insert and update. The method PostgresClient.setIdField(String) no longer exists.

The **views** section is a bit more self explanatory, as it indicates a viewName and the two tables (and a column per table) to join by. In addition to that, you can indicate the join type between the two tables. For example:
```
  "views": [
    {
      "viewName": "items_mt_view",
      "join": [
        {
          "table": {
            "tableName": "item",
            "joinOnField": "materialTypeId"
          },
          "joinTable": {
            "tableName": "material_type",
            "joinOnField": "id",
            "jsonFieldAlias": "mt_jsonb"
          }
        }
      ]
    }
  ]
```
Behind the scenes this will produce the following statement which will be run as part of the schema creation:

    CREATE OR REPLACE VIEW ${tenantid}_${module_name}.items_mt_view AS
      SELECT u.id, u.jsonb as jsonb, g.jsonb as mt_jsonb
      FROM ${tenantid}_${module_name}.item u
      JOIN ${tenantid}_${module_name}.material_type g
        ON lower(f_unaccent(g.jsonb->>'id')) = lower(f_unaccent(u.jsonb->>'materialTypeId'))

Notice the `lower(f_unaccent(` functions, currently, by default, all string fields will be wrapped in these functions (will change in the future).

A three table join would look something like this:

```
    {
      "viewName": "instance_holding_item_view",
      "join": [
        {
          "table": {
            "tableName": "instance",
            "joinOnField": "id"
          },
          "joinTable": {
            "tableName": "holdings_record",
            "joinOnField": "instanceId",
            "jsonFieldAlias": "ho_jsonb"
          }
        },
        {
          "table": {
            "tableName": "holdings_record",
            "joinOnField": "id",
            "jsonFieldAlias": "ho2_jsonb"
          },
          "joinTable": {
            "tableName": "item",
            "joinOnField": "holdingsRecordId",
            "jsonFieldAlias": "it_jsonb"
          }
        }
      ]
    }
```

The **script** section allows a module to run custom SQLs before table / view creation/updates and after all tables/views have been created/updated.

The fields in the **script** section include:

1. `run` - either `before` or `after` the tables / views are generated
2. `snippet` - the SQL to run
3. `snippetPath` - relative path to a file with SQL script to run. If `snippetPath` is set then `snippet` field will be ignored.
4. `fromModuleVersion` - same as `fromModuleVersion` for table


The tables / views will be generated in the schema named tenantid_modulename

The x-okapi-tenant header passed in to the API call will be used to get the tenant id.
The value used for the module name is the artifactId found in the pom.xml (the parent artifactId is used if one is found).

#### Important information
Right now all indexes on string fields in the jsonb should be declared as case in-sensitive and lower cased. This is how the [CQL to Postgres converter](#cql-contextual-query-language) generates SQL queries, so in order for the indexes generated to be used during query time, the indexes must be declared in a similar manner
```
  {
    "fieldName": "title",
    "tOps": "ADD",
    "caseSensitive": false,
    "removeAccents": true
  }
```

Behind the scenes, the CQL to Postgres query converter will generate regex queries for `=` queries.
For example: `?query=fieldA=ABC` will generate an SQL regex query, which will require a gin index to perform on large tables.

The converter will generate LIKE queries for `==` queries. For example `?query=fieldA==ABC` will generate an SQL LIKE query that will use a btree index (if it exists). For queries that only look up specific ids, etc... the preferred approach would be to query with two equals `==` and hence, declare a regular btree (index).


##### Posting information

Posting a new tenant can optionally include a body. The body should contain a JSON conforming to the https://github.com/folio-org/raml/blob/master/schemas/moduleInfo.schema schema. The `module_to` entry is mandatory if a body is included in the request, indicating the version module for this tenant. The `module_from` entry is optional and indicates an upgrade for the tenant to a new module version.

##### Encrypting Tenant passwords

As of now (this may change in the future), securing a tenant's connection to the database via an encrypted password can be accomplished in the following way:

 - Set the secret key (as described in the Securing DB Configuration file section)

  The PASSWORD will be replaced with the following:
  encrypt(tenant id with secret key) = **new tenant's password**
  The **new tenant's password** will replace the default PASSWORD value (which is the tenantid_modulename)
  The RMB Postgres client will use the secret key and the passed in tenant id to calculate the tenant's password when DB connections are needed for that tenant. Note that if you use the tenant API and set the secret key - the decrypting of the password will be done by the Postgres Client for each tenant connection.


The RMB comes with a TenantClient to facilitate calling the API via URL.
To post a tenant via the client:

```java
TenantClient tClient = null;
tClient = new TenantClient("http://localhost:" + port, "mytenantid", "sometoken");
tClient.post( response -> {
  response.bodyHandler( body -> {
    System.out.println(body.toString());
    async.complete();
  });
});
```

#### The Delete Tenant API

When this API is called RMB will basically drop the schema for the tenant (CASCADE) as well as drop the user


**Some Postgres Client examples**


Examples:

Saving a POJO within a transaction:

```java
PoLine poline = new PoLine();

...

postgresClient.save(beginTx, TABLE_NAME_POLINE, poline , reply -> {...
```
Remember to call beginTx and endTx

Querying for similar POJOs in the DB (with or without additional criteria):

```java
Criterion c = new Criterion(new Criteria().addField("id").setJSONB(false).setOperation("=").setValue("'"+entryId+"'"));

postgresClient.get(TABLE_NAME_POLINE, PoLine.class, c,
              reply -> {...
```

The `Criteria` object which generates `where` clauses can also receive a JSON Schema so that it can cast values to the correct type within the `where` clause.

```java
Criteria idCrit = new Criteria("ramls/schemas/userdata.json");
```

## RAMLs API

The RAMLs API is a multiple interface which affords RMB modules to expose their RAML files in a machine readable way. To enable the interface the module must add the following to the provides array of its module descriptor:

```JSON
{
  "id": "_ramls",
  "version": "1.0",
  "interfaceType" : "multiple",
  "handlers" : [
    {
      "methods" : [ "GET" ],
      "pathPattern" : "/_/ramls"
    }
  ]
}
```

The interface has a single GET endpoint with an optional query parameter path. Without the path query parameter the response will be an application/json array of the available RAMLs. This will be the immediate RAMLs the module provides. If the query parameter path is provided it will return the RAML at the path if exists. The RAML will have HTTP resolvable references. These references are either to JSON Schemas or RAMLs the module provides or shared JSON Schemas and RAMLs. The shared JSON Schemas and RAMLs are included in each module via a git submodule under the path `raml_util`. These paths are resolvable using the path query parameter.

The RAML defining the API:

https://github.com/folio-org/raml/blob/eda76de6db681076212e20c7f988c3913764b9b0/ramls/ramls.raml

## JSON Schemas API

The JSON Schemas API is a multiple interface which affords RMB modules to expose their JSON Schema files in a machine readable way. To enable the interface the module must add the following to the provides array of its module descriptor:

```JSON
{
  "id": "_jsonSchemas",
  "version": "1.0",
  "interfaceType" : "multiple",
  "handlers" : [
    {
      "methods" : [ "GET" ],
      "pathPattern" : "/_/jsonSchemas"
    }
  ]
}
```

The interface has a single GET endpoint with an optional query parameter path.
Without the path query parameter the response will be an "application/json" array of the available JSON Schemas. By default this will be JSON Schemas that are stored in the root of ramls directory of the module. Returned list of schemas can be customized in modules pom.xml file.
Add schema_paths system property to "exec-maven-plugin" in pom.xml running the
`<mainClass>org.folio.rest.tools.GenerateRunner</mainClass>`
specify comma-separated list of directories that should be searched for schema files. To search directory recursively specify
directory in the form of glob expression (e.g. "raml-util/**")
 For example:
```
<systemProperty>
  <key>schema_paths</key>
  <value>schemas/**,raml-util/**</value>
</systemProperty>
```
If the query parameter path is provided it will return the JSON Schema at the path if exists. The JSON Schema will have HTTP resolvable references. These references are either to JSON Schemas or RAMLs the module provides or shared JSON Schemas and RAMLs. The shared JSON Schemas and RAMLs are included in each module via a git submodule under the path `raml_util`. These paths are resolvable using the path query parameter.

The RAML defining the API:

https://github.com/folio-org/raml/blob/eda76de6db681076212e20c7f988c3913764b9b0/ramls/jsonSchemas.raml

## Query Syntax

The RMB can receive parameters of different types. Modules can declare a query parameter and receive it as a string parameter in the generated API functions.

The RMB exposes an easy way to query, using [CQL (Contextual Query Language)](#cql-contextual-query-language).
This enables a seamless integration from the query parameters to a prepared "where" clause to query with.

```java
//create object on table.field
CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablename.jsonb");
//cql wrapper based on table.field and the cql query
CQLWrapper cql = new CQLWrapper(cql2pgJson, query);
//query the db with the cql wrapper object
PostgresClient.getInstance(context.owner(), tenantId).get(CONFIG_COLLECTION, Config.class,
          cql, true,
```

The CQLWrapper can also get an offset and limit:

```java
new CQLWrapper(cql2pgJson, query).setLimit(new Limit(limit)).setOffset(new Offset(offset));
```

A CQL querying example:

```sh
http://localhost:<port>/configurations/entries?query=scope.institution_id=aaa%20sortBy%20enabled
```

## Metadata

RMB is aware of the [metadata.schema](https://github.com/folio-org/raml/blob/raml1.0/schemas/metadata.schema). When a request (POST / PUT) comes into an RMB module, RMB will check if the passed-in JSON's schema declares a reference to the metadata schema. If so, RMB will populate the JSON with a metadata section with the current user and the current time. RMB will set both update and create values to the same date/time and to the same user, as accepting this information from the request may be unreliable. The module should persist the creation date and the created by values after the initial POST. For an example of this using SQL triggers see [metadata.ftl](https://github.com/folio-org/raml-module-builder/blob/master/domain-models-runtime/src/main/resources/templates/db_scripts/metadata.ftl). Add [withMetadata to the schema.json](https://github.com/folio-org/raml-module-builder#the-post-tenant-api) to create that trigger.

## Facet Support

RMB also allows easy faceting of result sets. The grouping / faceting is done in the database.
To add faceting to your API.
1. Add the [faceting RAML trait](https://github.com/folio-org/raml/blob/master/traits/facets.raml) to your RAML and reference it from the endpoint (using the is:[])
    - facet query parameter format: `facets=a.b.c` or `facets=a.b.c:10` (they are repeating). For example `?facets=active&facets=personal.lastName`
2. Add the [resultInfo.schema](https://github.com/folio-org/raml/blob/master/schemas/resultInfo.schema) to your RAML and reference it within your collection schemas.
For example:
```
 "type": "object",
  "properties": {
    "items": {
      "id": "items",
      "type": "array",
      "items": {
        "type": "object",
        "$ref" : "item.json"
      }
    },
    "resultInfo": {
      "type": "object",
      "$ref": "raml-util/schemas/resultInfo.schema"
    }
```
3. When building your module, an additional parameter will be added to the generated interfaces of the faceted endpoints. `List<String> facets`. You can simply convert this list into a List of Facet objects using the RMB tool as follows: `List<FacetField> facetList = FacetManager.convertFacetStrings2FacetFields(facets, "jsonb");` and pass the `facetList` returned to the `postgresClient`'s `get()` methods.

You can set the amount of results to facet on by calling (defaults to 10,000) `FacetManager.setCalculateOnFirst(20000);`
Note that higher numbers will potentially affect performance.

4. Faceting on array fields can be done in the following manner:
`personal.secondaryAddress[].postalCode`
`personal.secondaryAddress[].address[].postalCode`

NOTE: Creating an index on potential facet fields may be required so that performance is not greatly hindered

## JSON Schema fields

It is possible to indicate that a field in the JSON is a readonly field when declaring the schema. `"readonly": true`. From example:
```
    "resultInfo": {
      "$ref": "raml-util/schemas/resultInfo.schema",
      "readonly" : true
    }
```
A `readonly` field is not allowed to be passed in as part of the request. A request that contains data for a field that was declared as `readonly` will have its read-only fields removed from the passed in data by RMB (the data will be passed into the implementing functions without the read-only fields)

This is part of a framework exposed by RMB which allows creating a field and associating a validation constraint on that field.

To add a custom field, add a system property (in the configuration) to the plugin definition (in the pom.xml) running the `<mainClass>org.folio.rest.tools.GenerateRunner</mainClass>`

for example:
```
<systemProperty>
    <key>jsonschema.customfield</key>
    <value>{"fieldname" : "readonly" , "fieldvalue": true , "annotation" : "javax.validation.constraints.Null"}</value>
</systemProperty>
```

the `jsonschema.customfield` key can contain multiple JSON values (delimited by a `;`). Each JSON indicates a field name + a field value to match against - and a validation annotation to apply. So, getting back to the readonly field, the example above indicates that a field in the JSON schema that has been tagged with the `readonly` field can not contain data when passed in as part of the request.
A list of available annotations:
https://docs.oracle.com/javaee/7/api/javax/validation/constraints/package-summary.html

To customize generation of java classes, add a system property to plugin definition running `<mainClass>org.folio.rest.tools.GenerateRunner</mainClass>`.
Properties that start with `jsonschema2pojo.config` will be passed to underlying library that generates java classes.
Incomplete list of available properties:
- jsonschema2pojo.config.includeHashcodeAndEquals - adds hashCode and equals methods
- jsonschema2pojo.config.includeToString - adds toString method
- jsonschema2pojo.config.serializable - makes classes serializable

For more available properties see:
 https://joelittlejohn.github.io/jsonschema2pojo/site/1.0.0/generate-mojo.html
 https://github.com/mulesoft-labs/raml-for-jax-rs/blob/master/raml-to-jaxrs/jaxrs-code-generator/src/main/java/org/raml/jaxrs/generator/RamlToJaxRSGenerationConfig.java

## Overriding RAML (traits) / query parameters

A module may require slight changes to existing RAML traits.
For example, a `limit` trait may be defined in the following manner:
 ```
        limit:
          description: Limit the number of elements returned in the response
          type: integer
          required: false
          example: 10
          default: 10
          minimum: 1
          maximum: 2147483647
```
However, a module may not want to allow such a high maximum as this may cause a crash.
A module can create a `raml_overrides.json` file and place it in the `/resources/overrides/` directory.

The file is defined in the schema:
`domain-models-interface-extensions/src/main/resources/overrides/raml_overrides.schema`

Note that `DEFAULTVALUE` only allows string values. `SIZE` requires a range ex. `"15, 20"`. `REQUIRED` does not accept a `"value"`, meaning an optional parameter can become required but not vice versa.

example:
`domain-models-interface-extensions/src/main/resources/overrides/raml_overrides.json`

## Drools integration

The RMB framework automatically scans the `/resources/rules` path in an implemented project for
`*.drl` files. A directory can also be passed via the command line `drools_dir`. The rule files are loaded and are applied automatically to all objects passed in the body (post,
put) by the runtime framework. This works in the following manner:
 - A POST / PUT request comes in with a body
 - The body for the request is mapped to a generated POJO
 - The POJO is inserted into the RMB's Drools session
 - All rules are run against the POJO

This allows for more complex validation of objects.

- For example, two specific fields can logically be null, but not at the
  same time. That can easily be implemented with a Drool, as those types of
  validations are harder to create in a RAML file.

- The `rules` project also exposes the drools session and allows validation
  within the implemented APIs. See the `tests` in the `rules` project.

For example: (Sample.drl)

```
package com.sample

import org.folio.rest.jaxrs.model.Patron;

rule "Patron needs one ID at the least"

    no-loop

    when
        p : Patron( patronBarcode  == null, patronLocalId == null )
    then
        throw new java.lang.Exception("Patron needs one ID field populated at the least");
end
```

It is also possible to create a Drools session in your code, and load rules into the session in a more dynamic way.
For example:

```java
import org.folio.rulez.Rules;
...
List<String> ruleList = generateDummyRule();
Rules rules = new Rules(ruleList);
ksession = rules.buildSession();
...
Messages message = new Messages();
ksession.insert(message);
ksession.fireAllRules();
Assert.assertEquals("THIS IS A TEST", message.getMessage());
```

An additional option to use the Drools framework in the RMB is to load rules dynamically. For example, a module may decide to store Drool `.drl` files in a database. This enables a module to allow admin users to update rules in the database and then load them into the RMB validation mechanism for use at runtime.

```java
      Rules rules = new Rules(List<String> rulesLoaded);
      ksession = rules.buildSession();
      RestVerticle.updateDroolsSession(ksession);
```

## Messages

The runtime framework comes with a set of messages it prints out to the logs /
sends back as error responses to incorrect API calls. These messages are
language-specific. In order to add your own message files, place the files in
your project under the `/resources/messages` directory.

Note that the format of the file names should be either:
- `[name]_[lang_2_letters].properties` (e.g.: `APIMessages_de.properties`)
- `[lang_2_letters]_messages.prop` (e.g.: `en_messages.prop`)

For example:
In the circulation project, the messages file can be found at `/circulation/src/main/resources/en_messages.prop` with the following content:

```sh
20002=Operation can not be calculated on a Null Amount
20003=Unable to pay fine, amount is larger than owed
20004=The item {0} is not renewable
20005=Loan period must be greater than 1, period entered: {0}
```

The circulation project exposes these messages as enums for easier usage in the code:

```java
package org.folio.utils;

import org.folio.rest.tools.messages.MessageEnum;

public enum CircMessageConsts implements MessageEnum {

  OperationOnNullAmount("20002"),
  FinePaidTooMuch("20003"),
  NonRenewable("20004"),
  LoanPeriodError("20005");

  private String code;
  private CircMessageConsts(String code){
    this.code = code;
  }
  public String getCode(){
    return code;
  }
}
```

Usage:

`private final Messages messages = Messages.getInstance();`

`messages.getMessage(lang, CircMessageConsts.OperationOnNullAmount);`

Note: parameters can also be passed when relevant. The raml-module-builder runtime also exposes generic error message enums which can be found at `/domain-models-runtime/src/main/java/org/folio/rest/tools/messages/MessageConsts.java`

## Documentation of the APIs

The runtime framework includes a web application which exposes RAMLs in a
view-friendly HTML format.
This uses [api-console](https://github.com/mulesoft/api-console)
(Powered by [MuleSoft](http://www.MuleSoft.org) for RAML
Copyright (c) 2013 MuleSoft, Inc.)

The `maven-resources-plugin` plugin described earlier
copies the RAML files into the correct directory in your project, so that the
runtime framework can access it and show local API documentation.

So for example, when running the [sample working module](#get-started-with-a-sample-working-module)
then its API documentation is at:

```
http://localhost:8081/apidocs/index.html?raml=raml/configuration/config.raml
```

If instead your [new module](#creating-a-new-module) is running on the default port,
then its API documentation is at:

```
http://localhost:8081/apidocs/index.html?raml=raml/my-project.raml
```
and remember to specify the "X-Okapi-Tenant: diku" header.

The RMB also automatically provides other documentation, such as the "Admin API":

```
http://localhost:8081/apidocs/index.html?raml=raml/admin.raml
```

All current API documentation is also available at [dev.folio.org/doc/api](https://dev.folio.org/reference/api/)

## Logging

RMB uses the Log4J logging library. Logs that are generated by RMB will print all log entries in the following format:
`%d{dd MMM yyyy HH:mm:ss:SSS} %-5p %C{1} %X{reqId} %m%n`

A module that wants to generate log4J logs in a different format can create a log4j.properties file in the /resources directory.

The log levels can also be changed via the `/admin` API provided by the framework. For example:

Get log level of all classes:

(GET) `http://localhost:8081/admin/loglevel`

Change log level of all classes to FINE:

(PUT) `http://localhost:8081/admin/loglevel?level=FINE`

A `java_package` parameter can also be passed to change the log level of a specific package. For example:

 `http://localhost:8081/admin/loglevel?level=INFO&java_package=org.folio.rest.persist.PostgresClient`

 `http://localhost:8081/admin/loglevel?level=INFO&java_package=org.folio.rest.persist`

## Monitoring

The runtime framework via the `/admin` API exposes (as previously mentioned) some APIs to help monitor the service (setting log levels, DB information).
Some are listed below (and see the [full set](#documentation-of-the-apis)):

 - `/admin/jstack` -- Stack traces of all threads in the JVM to help find slower and bottleneck methods.
 - `/admin/memory` -- A jstat type of reply indicating memory usage within the JVM on a per pool basis (survivor, old gen, new gen, metadata, etc.) with usage percentages.
 - `/admin/slow_queries` -- Queries taking longer than X seconds.
 - `/admin/cache_hit_rates` -- Cache hit rates in Postgres.
 - `/admin/table_index_usage` -- Index usage per table.
 - `/admin/postgres_table_size` -- Disk space used per table.
 - `/admin/postgres_table_access_stats` -- Information about how tables are being accessed.
 - `/admin/postgres_load` -- Load information in Postgres.
 - `/admin/postgres_active_sessions` -- Active sessions in Postgres.
 - `/admin/health` -- Returns status code 200 as long as service is up.
 - `/admin/module_stats` -- Summary statistics (count, sum, min, max, average) of all select / update / delete / insert DB queries in the last 2 minutes.

## Overriding Out of The Box RMB APIs
It is possible to over ride APIs that the RMB provides with custom implementations.
For example:
To override the `/health` API to return a relevant business logic health check for a specific module do the following:

1. `extend` the AdminAPI class that comes with the RMB framework - `public class CustomHealthCheck extends AdminAPI` and over ride the `getAdminHealth` function. The RMB will route the URL endpoint associated with the function to the custom module's implementation.

Example:

```java
public class CustomHealthCheck extends AdminAPI {

  @Override
  public void getAdminHealth(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    super.getAdminHealth(okapiHeaders,  res -> {
      System.out.println(" --- this is an over ride of the health API by the config module "+res.result().getStatus());
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminHealthResponse.withOK()));
    }, vertxContext);
  }

  @Override
  public void getAdminModuleStats(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    super.getAdminModuleStats(okapiHeaders,  res -> {

      JsonObject o = new JsonObject(res.result().getEntity().toString());

      System.out.println(" --- this is an over ride of the Module Stats API by the config module ");
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetAdminModuleStatsResponse.
        withPlainOK( o.encodePrettily() )));
    }, vertxContext);
  }
}
```

## Client Generator

The framework can generate a Client class for every RAML file with a function for every API endpoint in the RAML.

To generate a client API from your RAML add the following plugin to your pom.xml

```xml
      <plugin>
        <groupId>org.codehaus.mojo</groupId>
        <artifactId>exec-maven-plugin</artifactId>
        <version>1.5.0</version>
        <executions>
          <execution>
            <id>generate_client</id>
            <phase>process-classes</phase>
            <goals>
              <goal>java</goal>
            </goals>
            <configuration>
              <mainClass>org.folio.rest.tools.ClientGenerator</mainClass>
              <cleanupDaemonThreads>false</cleanupDaemonThreads>
              <systemProperties>
                <systemProperty>
                  <key>client.generate</key>
                  <value>true</value>
                </systemProperty>
                <systemProperty>
                  <key>project.basedir</key>
                  <value>${basedir}</value>
                </systemProperty>
                <systemProperty>
                  <key>json.type</key>
                  <value>postgres</value>
                </systemProperty>
              </systemProperties>
            </configuration>
          </execution>
        </executions>
      </plugin>
```

For the monitoring APIs exposed by the runtime framework, changing the log level via the client would look like this:

```java
    AdminClient aClient = new AdminClient("http://localhost:" + 8083, "myuniversityId", "sometoken");
    aClient.putLoglevel(Level.FINE, "org.folio",  apiResponse -> {
      System.out.println(apiResponse.statusCode());
    });
```

Requesting a stack trace would look like this:

```java
    AdminClient aClient = new AdminClient("http://localhost:" + 8083, "myuniversityId", "sometoken");
    aClient.getJstack( trace -> {
      trace.bodyHandler( content -> {
        System.out.println(content);
      });
    });
```

## Querying multiple modules via HTTP

The RMB has some tools available to help:
 - Make HTTP requests to other modules
 - Parse JSON responses received (as well as any JSON for that matter)
 - Merge together / Join JSON responses from multiple modules
 - Build simple CQL query strings based on values in a JSON

#### HTTP Requests

The `HttpModuleClient2` class exposes a basic HTTP Client.
The full constructor takes the following parameters
 - host
 - port
 - tenantId
 - keepAlive - of connections (default: true)
 - connTO - connection timeout (default: 2 seconds)
 - idleTO - idle timeout (default: 5 seconds)
 - autoCloseConnections - close connection when request completes (default: true)
 - cacheTO - cache of endpoint results timeout (in minutes, default: 30)

```
    HttpModuleClient hc = new HttpModuleClient("localhost", 8083, "myuniversity_new2", false);
    Response response = hc.request("/groups");
```

It is recommended to use the `HttpClientFactory` to get an instance of the `HttpModuleClient2`.
The factory will then return either the actual `HttpModuleClient2` class or an instance of the `HttpClientMock2`. To return an instance of the mock client, set the mock mode flag in the vertx config. One way to do this:
`new DeploymentOptions().setConfig(new JsonObject().put(HttpClientMock2.MOCK_MODE, "true"));`
See [mock_content.json](https://github.com/folio-org/raml-module-builder/blob/master/domain-models-runtime/src/test/resources/mock_content.json) for an example of how to associate a url with mocked data and headers

The client returns a `Response` object. The `Response` class has the following members:
  - endpoint - url the response came from
  - code - http returned status code for request
  - (JsonObject) body - the response data
  - (JsonObject) error -  in case of an error - The `error` member will be populated. The
  error object will contain the `endpoint`, the `statusCode`, and the `errorMessage`
  - (Throwable) exception - if an exception was thrown during the API call


The `HttpModuleClient2 request` function can receive the following parameters:
 - `HttpMethod` - (default: GET)
 - `endpoint` - API endpoint
 - `headers` - Default headers are passed in if this is not populated: Content-type=application/json, Accept: plain/test
 - `RollBackURL` - NOT SUPPORTED - URL to call if the request is unsuccessful [a non 2xx code is returned]. Note that if the Rollback URL call is unsuccessful, the response error object will contain the following three entries with more info about the error (`rbEndpoint`, `rbStatusCode`, `rbErrorMessage`)
 - `cachable` - Whether to cache the response
 - `BuildCQL` object - This allows you to build a simple CQL query string from content within a JSON object. For example:
`
Response userResponse =
hc.request("/users", new BuildCQL(groupsResponse, "usergroups[*].id", "patron_group"));
`
This will create a query string with all values from the JSON found in the path `usergroups[*].id` and will generate a CQL query string which will look something like this:
`?query=patron_group==12345+or+patron+group==54321+or+patron_group==09876...`
See `BuildCQL` for configuration options.

The `Response` class also exposes a joinOn function that allow you to join / merge the received JSON objects from multiple requests.

`public Response joinOn(String withField, Response response, String onField, String insertField,
      String intoField, boolean allowNulls)`


The Join occurs with the response initiating the joinOn call:

 - `withField` - the field within the response whose value / values will be used to join
 - `response` - the response to join this response with
 - `onField` - the field in the passed in response whose value / values will be used to join
 - `insertField` - the field in the passed in `response` to push into the current response (defaults to the `onField` value if this is not passed in)
 - `intoField` - the field to populate within this response
 - `allowNulls` - whether to populate with `null` if the field requested to push into the current response is `null` - if set to false - then the field will not be populated with a null value.

Example:

join:

(response1) `{"a": "1","b": "2"}`

with:

(response2) `{"arr2":[{"id":"1","a31":"1"},{"id":"1","a31":"2"},{"id":"1","a32":"4"},{"id":"2","a31":"4"}]}`

returns:
`{"a":"1","b":["1","2"]}`

with the following call:
`response1.joinOn("a", response2, "arr2[*].id", "a31", "b", false)`

Explanation:
Join response1 on field "a" with response2 on field "arr2[*].id" (this means all IDs in the arr2 array. If a match is found take the value found in field "a31" and place it in field "b".
Since in this case a single entry (response1) matches multiple entries from response2 - an array is created and populated. If this was a one-to-one match, then only the single value (whether a JSON object, JSON array, any value) would have been inserted.

#### Parsing

The RMB exposes a simple JSON parser for the vert.x JSONObject. The parser allows getting and setting nested JSON values. The parser allows retrieving values / nested values in a simpler manner.
For example:

`a.b` -- Get value of field 'b' which is nested within a JSONObject called 'a'

`a.c[1].d` -- Get 'd' which appears in array 'c[1]'

`a.'bb.cc'` -- Get field called 'bb.cc' (use '' when '.' in name)

`a.c[*].a2` -- Get all 'a2' values as a List for each entry in the 'c' array


See the `JsonPathParser` class for more info.


#### An example HTTP request

    //create a client
    HttpClientInterface client = HttpClientFactory.getHttpClient(okapiURL, tenant);
    //make a request
    CompletableFuture<Response> response1 = client.request(url, okapiHeaders);
    //chain a request to the previous request, the placeholder {users[0].username}
    //means that the value appearing in the first user[0]'s username in the json returned
    //in response1 will be injected here
    //the handlePreviousResponse() is a function you code and will receive the response
    //object (containing headers / body / etc,,,) of response1 so that you can decide what to do
    //before the chainedRequest is issued - see example below
    //the chained request will not be sent if the previous response (response1) has completed with
    //an error
    response1.thenCompose(client.chainedRequest("/authn/credentials/{users[0].username}",
        okapiHeaders, null, handlePreviousResponse());

        Consumer<Response> handlePreviousResponse(){
            return (response) -> {
                int statusCode = response.getCode();
                boolean ok = Response.isSuccess(statusCode);
                //if not ok, return error
            };
        }

    //if you send multiple chained Requests based on response1 you can use the
    //CompletableFuture.allOf() to wait till they are all complete
    //or you can chain one request to another in a pipeline manner as well

    //you can also generate a cql query param as part of the chained request based on the
    //response of the previous response. the below will create a username=<value> cql clause for
    //every value appearing in the response1 json's users array -> username
    response1.thenCompose(client.chainedRequest("/authn/credentials", okapiHeaders, new BuildCQL(null, "users[*].username", "username")),...

    //join the values within 2 responses - injecting the value from a field in one json into the field of another json when a constraint between the two jsons exists (like field a from json 1 equals field c from json 2)
    //compare all users->patron_groups in response1 to all usergroups->id in groupResponse, when there is a match, push the group field in the specific entry of groupResonse into the patron_group field in the specific entry in the response1 json
    response1.joinOn("users[*].patron_group", groupResponse, "usergroups[*].id", "group", "patron_group", false);
    //close the http client
    hClient.closeClient();

## A Little More on Validation

Query parameters and header validation
![](images/validation.png)

#### Object validations

![](images/object_validation.png)

#### function example
```java
  @Validate
  @Override
  public void getConfigurationsEntries(String query, int offset, int limit,
      String lang,java.util.Map<String, String>okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context context) throws Exception {

    CQLWrapper cql = getCQL(query,limit, offset);
    /**
    * http://host:port/configurations/entries
    */
    context.runOnContext(v -> {
      try {
        System.out.println("sending... getConfigurationsTables");
        String tenantId = TenantTool.calculateTenantId( okapiHeaders.get(RestVerticle.OKAPI_HEADER_TENANT) );

        PostgresClient.getInstance(context.owner(), tenantId).get(CONFIG_TABLE, Config.class,
          new String[]{"*"}, cql, true,
            reply -> {
              try {
                if(reply.succeeded()){
                  Configs configs = new Configs();
                  List<Config> config = (List<Config>) reply.result()[0];
                  configs.setConfigs(config);
                  configs.setTotalRecords((Integer)reply.result()[1]);
                  asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetConfigurationsEntriesResponse.withJsonOK(
                    configs)));
                }
                else{
                  log.error(reply.cause().getMessage(), reply.cause());
                  asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetConfigurationsEntriesResponse
                    .withPlainBadRequest(reply.cause().getMessage())));
                }
              } catch (Exception e) {
                log.error(e.getMessage(), e);
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetConfigurationsEntriesResponse
                  .withPlainInternalServerError(messages.getMessage(
                    lang, MessageConsts.InternalServerError))));
              }
            });
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        String message = messages.getMessage(lang, MessageConsts.InternalServerError);
        if(e.getCause() != null && e.getCause().getClass().getSimpleName().endsWith("CQLParseException")){
          message = " CQL parse error " + e.getLocalizedMessage();
        }
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetConfigurationsEntriesResponse
          .withPlainInternalServerError(message)));
      }
    });
  }
```

## Advanced Features
1. RMB handles all routing, so this is abstracted from the developer. However, there are cases where third party functionality may need access to routing information. Once again, this is not to be used for routing, but in order to pass in routing information to a third party (one such example is the pac4j vertx saml client). RMB allows a developer to receive the Vertx RoutingContext object as a parameter to a generated function by indicating the endpoint represented by the function in the pom.xml (uses a comma delimiter for multiple paths).
```java
  <properties>
    <generate_routing_context>/rmbtests/test</generate_routing_context>
  </properties>
```

## Additional Tools

#### De-Serializers
At runtime RMB will serialize/deserialize the received JSON in the request body of PUT and POST requests into a POJO and pass this on to an implementing function, as well as the POJO returned by the implementing function into JSON. A module can implement its own version of this. For example, the below will register a de-serializer that will tell RMB to set a User to not active if the expiration date has passed. This will be run when a User JSON is passed in as part of a request
```
ObjectMapperTool.registerDeserializer(User.class, new UserDeserializer());

public class UserDeserializer extends JsonDeserializer<User> {

  @Override
  public User deserialize(JsonParser parser, DeserializationContext context) throws IOException, JsonProcessingException {
    ObjectMapper mapper = ObjectMapperTool.getDefaultMapper();
    ObjectCodec objectCodec = parser.getCodec();
    JsonNode node = objectCodec.readTree(parser);
    User user = mapper.treeToValue(node, User.class);
    Optional<Date> expirationDate = Optional.ofNullable(user.getExpirationDate());
    if (expirationDate.isPresent()) {
      Date now = new Date();
      if (now.compareTo(expirationDate.get()) > 0) {
        user.setActive(false);
      }
    }
    return user;
  }
}
```
#### Error handling tool

Making async calls to the PostgresClient requires handling failures of different kinds. RMB exposes a tool that can handle the basic error cases, and return them as a 422 validation error status falling back to a 500 error status when the error is not one of the standard DB errors.

Usage:

```
if(reply.succeeded()){
  ..........
}
else{
   ValidationHelper.handleError(reply.cause(), asyncResultHandler);
}
```
RMB will return a response to the client as follows:

- invalid UUID - 422 status
- duplicate key violation - 422 status
- Foreign key violation - 422 status
- tenant does not exist / auth error to db - 401 status
- Various CQL errors - 422 status
- Anything else will fall back to a 500 status error

RMB will not cross check the raml to see that these statuses have been defined for the endpoint. This is the developer's responsibility.



## Some REST examples

Have these in the headers - currently not validated hence not mandatory:

- Accept: application/json,text/plain
- Content-Type: application/json;

#### Example 1: Add a fine to a patron (post)

```
http://localhost:8080/patrons/56dbe25ea12958478cec42ba/fines
{
  "fine_amount": 10,
  "fine_outstanding": 0,
  "fine_date": 1413879432,
  "fine_pay_in_full": true,
  "fine_pay_in_partial": false,
  "fine_note": "aaaaaa",
  "item_id": "56dbe160a129584dc8de7973",
  "fine_forgiven": {
 "user": "the cool librarian",
 "amount": "none"
  },
  "patron_id": "56dbe25ea12958478cec42ba"
}
```

#### Example 2: Get fines for patron with id

```
http://localhost:8080/patrons/56dbe25ea12958478cec42ba/fines
```

#### Example 3: Get a specific patron

```
http://localhost:8080/patrons/56dbe25ea12958478cec42ba
```

#### Example 4: Get all patrons

```
http://localhost:8080/patrons
```

#### Example 5: Delete a patron (delete)

```
http://localhost:8080/patrons/56dbe791a129584a506fb41a
```

#### Example 6: Add a patron (post)

```
http://localhost:8080/patrons
{
 "status": "ACTIVE",
 "patron_name": "Smith,John",
 "patron_barcode": "00007888",
 "patron_local_id": "abcdefd",
 "contact_info": {
  "patron_address_local": {
   "line1": "Main Street 1",
   "line2": "Nice building near the corner",
   "city": "London",
   "state_province": "",
   "postal_code": "",
   "address_note": "",
   "start_date": "2013-12-26Z"
  },
  "patron_address_home": {
   "line1": "Main Street 1",
   "line2": "Nice building near the corner",
   "city": "London",
   "state_province": "",
   "postal_code": "",
   "address_note": "",
   "start_date": "2013-12-26Z"
  },
  "patron_address_work": {
   "line1": "Main Street 1",
   "line2": "Nice building near the corner",
   "city": "London",
   "state_province": "",
   "postal_code": "",
   "address_note": "",
   "start_date": "2013-12-26Z"
  },
  "patron_email": "johns@mylib.org",
  "patron_email_alternative": "johns@mylib.org",
  "patron_phone_cell": "123456789",
  "patron_phone_home": "123456789",
  "patron_phone_work": "123456789",
  "patron_primary_contact_info": "patron_email"
 },
 "total_loans": 50,
 "total_fines": "100$",
 "total_fines_paid": "0$",
 "patron_code": {
  "value": "CH",
  "description": "Child"
 }
}
```

## Additional information

Other [RMB documentation](doc/) (e.g. DB schema migration, Upgrading notes).

Other [modules](https://dev.folio.org/source-code/#server-side).

See project [RMB](https://issues.folio.org/browse/RMB)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker/).

Other FOLIO Developer documentation is at [dev.folio.org](https://dev.folio.org/)
