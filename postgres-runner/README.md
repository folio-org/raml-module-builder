## Goal

This module starts and stops an embedded Postgres if needed.

It can be used to start Postgres once for the complete maven
integration-test phase avoiding to start and stop it for each individual
integration test class. Starting embedded Postgres takes long
time under windows, up to one minute.

### Intended usage

Run PostgresRunner in maven pre-integration-test phase to start a Postgres instance.

Run PostgresWaiter in maven pre-integration-test phase to wait until the new
Postgres instance is up and running.

Run the integration tests in maven integration-test phase.

Run PostgresStopper in maven post-integration-test phase to shut down the Postgres
instance that was started before.

### Configuration

The command line parameters take precedence over DB_* environment variables. Last resort
is the default configuration.

Example command line invocation:

* `PostgresRunner 5434 5432 jane the-password-that-must-not-be-named`
* `PostgresWaiter 5434`
* `PostgresStopper 5434`

The same configuration using environment variables (no command line parameters):
* `DB_RUNNER_PORT=5434`
* `DB_PORT=5432`
* `DB_USERNAME=jane`
* `DB_PASSWORD=the-password-that-must-not-be-named`

The default configuration is the same as domain-models-runtime PostgresClient
default configuration:
* `DB_RUNNER_PORT=6001`
* `DB_PORT=6000`
* `DB_USERNAME=username`
* `DB_PASSWORD=password`

### Maven example

```
  <dependencies>
    ...
    <dependency>
      <groupId>org.folio</groupId>
      <artifactId>postgres-runner</artifactId>
      <version>...</version>
      <scope>test</scope>
    </dependency>
    ...
  </dependencies>
```

```
      <plugin>
        <groupId>org.codehaus.mojo</groupId>
        <artifactId>exec-maven-plugin</artifactId>
        <version>...</version>
        <executions>
          <execution>
            <id>start-postgres</id>
            <phase>pre-integration-test</phase>
            <goals>
              <goal>exec</goal>
            </goals>
            <configuration>
              <executable>java</executable>
              <async>true</async>
              <classpathScope>test</classpathScope>
              <arguments>
                <argument>-classpath</argument>
                <classpath />
                <argument>org.folio.rest.persist.PostgresRunner</argument>
              </arguments>
            </configuration>
          </execution>
          <execution>
            <id>wait-for-postgres</id>
            <phase>pre-integration-test</phase>
            <goals>
              <goal>java</goal>
            </goals>
            <configuration>
              <classpathScope>test</classpathScope>
              <mainClass>org.folio.rest.persist.PostgresWaiter</mainClass>
            </configuration>
          </execution>
          <execution>
            <id>stop-postgres</id>
            <phase>post-integration-test</phase>
            <goals>
              <goal>java</goal>
            </goals>
            <configuration>
              <classpathScope>test</classpathScope>
              <mainClass>org.folio.rest.persist.PostgresStopper</mainClass>
            </configuration>
          </execution>
        </executions>
      </plugin>
```

### Processes

PostgresRunner starts a new Postgres instance only if `DB_PORT` is free; if the
port is in use both PostgresRunner and PostgresWaiter return success.

In all cases PostgresRunner opens a port for PostgresWaiter and PostgresStopper.
PostgresRunner runs in the background (spawn).

PostgresWaiter waits until Postgres is ready; when PostgresWaiter returns
Postgres is ready and integration tests may start.

PostgresStopper posts a message to PostgresRunner to stop both
Postgres instance started by Postgres runner and PostgresRunner.
