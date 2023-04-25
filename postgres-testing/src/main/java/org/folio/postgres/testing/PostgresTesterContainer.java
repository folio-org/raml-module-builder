package org.folio.postgres.testing;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.time.Duration;

import org.folio.util.PostgresTester;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class PostgresTesterContainer implements PostgresTester {
  static public final String DEFAULT_IMAGE_NAME = "postgres:12-alpine";

  private PostgreSQLContainer<?> primary;
  private PostgreSQLContainer<?> standby;
  private String dockerImageName;

  /**
   * Create postgres container based on given image.
   * @param dockerImageName
   */
  public PostgresTesterContainer(String dockerImageName) {
    this.dockerImageName = dockerImageName;
  }

  /**
   *  Create postgres container with default image Postgres 12.
   */
  public PostgresTesterContainer() {
    this(DEFAULT_IMAGE_NAME);
  }

  // S2095: Resources should be closed
  // We can't close in start. As this whole class is Closeable!
  @java.lang.SuppressWarnings({"squid:S2095", "resource"})
  /**
   * Start the container.
   */
  @Override
  public void start(String database, String username, String password) throws SQLException, IOException, InterruptedException {
    if (primary != null) {
      throw new IllegalStateException("already started");
    }

    primary = new PostgreSQLContainer<>(dockerImageName)
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password)
        .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
    primary.start();

    String sqlCommand = "CREATE USER replicator WITH REPLICATION PASSWORD 'password'";
    System.out.println(primary.execInContainer("psql", "-U", username, "-d", database, "-c", sqlCommand).getStdout());

    standby = new PostgreSQLContainer<>(dockerImageName)
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password)
        // TODO is this necessary when using streaming?
        //.withCommand("postgres -c default_transaction_read_only=on")
        .withStartupTimeout(Duration.ofSeconds(60));
    standby.start();

    // TODO Can I get away with not running pg_basebackup?
    // TODO Is this the right line?
    primary.execInContainer("echo 'host replication replicator " + standby.getHost() + "/32 md5' >> /var/lib/postgresql/data/pg_hba.conf");
    // TODO Are these the right paths?
    primary.execInContainer("echo 'max_wal_senders = 1' >> /var/lib/postgresql/data/postgresql.conf");
    primary.execInContainer("echo 'wal_level = replica' >> /var/lib/postgresql/data/postgresql.conf");
    primary.execInContainer("echo 'archive_mode = off' >> /var/lib/postgresql/data/postgresql.conf");
    primary.execInContainer("echo 'listen_addresses = *' >> /var/lib/postgresql/data/postgresql.conf");
    primary.execInContainer("/usr/local/bin/docker-entrypoint.sh postgres");

    standby.withCommand("postgres -c primary_conninfo='host=" + primary.getHost() +
        " port=" + primary.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT) +
        " user=replicator password=password' -c standby_mode=on -c primary_slot_name=my_slot");
    standby.waitingFor(Wait.forLogMessage("database system is ready to accept connections", 1));

    //standby.start();
  }

  @Override
  public Integer getPort() {
    if (primary == null) {
      throw new IllegalStateException("not started");
    }
    return primary.getFirstMappedPort();
  }

  @Override
  public String getHost() {
    if (primary == null) {
      throw new IllegalStateException("not started");
    }
    return primary.getHost();
  }

  // TODO Add getters for standby

  @Override
  public boolean isStarted() {
    return primary != null;
  }

  @Override
  public void close() {
    if (primary != null) {
      primary.close();
      primary = null;
    }
  }
}
