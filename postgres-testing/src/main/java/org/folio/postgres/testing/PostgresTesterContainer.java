package org.folio.postgres.testing;

import java.io.IOException;
import java.sql.SQLException;

import java.time.Duration;

import org.folio.util.PostgresTester;
import org.testcontainers.containers.Container;
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
        // TODO Why is this regex necessary here but not below?
        .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
    primary.start();

    standby = new PostgreSQLContainer<>(dockerImageName)
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password)
        // TODO is this necessary when using streaming?
        //.withCommand("postgres -c default_transaction_read_only=on")
        .withStartupTimeout(Duration.ofSeconds(60));
    standby.start();

    String replicationSlot = "replication_slot1";
    String replicationUser = "replicator";
    String replicationPassword = "password";

    String createReplicationUser = "CREATE USER " + replicationUser +" WITH REPLICATION PASSWORD '" + replicationPassword + "'";
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", createReplicationUser));

    String createSlot = "SELECT * FROM pg_create_physical_replication_slot('" + replicationSlot + "');";
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", createSlot));

    String inspectSlot = "SELECT * FROM pg_replication_slots;";
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", inspectSlot));

    logExecResult(primary.execInContainer("sh", "-c", "echo '" + hbaConf(replicationUser) + "' > /var/lib/pg_hba.conf"));
    logExecResult(primary.execInContainer("cat", "/var/lib/pg_hba.conf"));
    logExecResult(primary.execInContainer("sh", "-c", "echo '" + pgConfPrimary() + "' > /var/lib/postgresql.conf"));
    logExecResult(primary.execInContainer("cat", "/var/lib/postgresql.conf"));

    // Since we have changed the configs, we need to restart.
    primary.stop();
    primary.start();

    logExecResult(standby.execInContainer("sh", "-c", "echo '" + hbaConf(replicationUser) + "' > /var/lib/pg_hba.conf"));
    logExecResult(standby.execInContainer("cat", "/var/lib/pg_hba.conf"));
    logExecResult(standby.execInContainer("sh", "-c", "echo '" + pgConfStandby() + "' > /var/lib/postgresql.conf"));
    logExecResult(standby.execInContainer("cat", "/var/lib/postgresql.conf"));
    standby.stop();
    standby.start();

    // TODO Does this method of passing in runtime configs work?
    standby.withCommand("postgres -c primary_conninfo='host=" + primary.getHost() +
        " port=" + primary.getFirstMappedPort() +
        " user=" + replicationUser + " password=" + replicationPassword +
        "' -c standby_mode=on -c primary_slot_name=" + replicationSlot);
    standby.waitingFor(Wait.forLogMessage("database system is ready to accept connections", 1));

    // TODO Can test if replication is working here using psql just to validate that the setup is correct. Then need to probably have a unit test or two.
  }

  public String hbaConf(String user) {
    return "host all all 127.0.0.1/32 trust\n" +
           "host all all ::1/128 trust\n" +
           "host replication " + user + " 0.0.0.0/0 trust\n" +
           "host all all all md5";
  }

  public String pgConfPrimary() {
    return "wal_level = replica\n" +
           "max_wal_senders = 1\n" +
           "wal_keep_segments = 32\n";
  }

  public String pgConfStandby() {
    return "hot_standby = on";
  }

  private void logExecResult(Container.ExecResult result) {
    String stdout = result.getStdout();
    if (!stdout.isEmpty()) {
      // TODO Probably want log4j here.
      System.out.println("Out: " + stdout);
    }

    String stderr = result.getStderr();
    if (!stderr.isEmpty()) {
      System.out.println("Err: " + stderr);
    }
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
