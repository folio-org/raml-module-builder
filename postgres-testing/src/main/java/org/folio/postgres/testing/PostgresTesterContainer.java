package org.folio.postgres.testing;

import java.io.IOException;
import java.util.UUID;

import org.folio.util.PostgresTesterStartException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import org.folio.util.PostgresTester;

public class PostgresTesterContainer implements PostgresTester {
  public static final String DEFAULT_IMAGE_NAME = "postgres:12-alpine";

  private static final int READY_MESSAGE_TIMES = 2;

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
  public void start(String database, String username, String password) throws PostgresTesterStartException {
    if (primary != null) {
      throw new IllegalStateException("already started");
    }

    GenericContainer<?> tempStandby;
    var replicationUser = "replicator";
    var primaryHost = "primaryhost";
    var dataDirectory = "/var/lib/postgresql/standby/";
    var tempDirectory = "/tmp/standby/";
    var hostVolume = "/tmp/rmb-standby-" + UUID.randomUUID();
    var network = Network.newNetwork();

    try {
      primary = new PostgreSQLContainer<>(dockerImageName)
          .withDatabaseName(database)
          .withUsername(username)
          .withPassword(password)
          .withNetwork(network)
          .withNetworkAliases(primaryHost)
          .withEnv("PGOPTIONS", "-c synchronous_commit=remote_apply")
          .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", READY_MESSAGE_TIMES));
      primary.start();

      // Modify_hba.conf to allow for replication.
      var hbaConf = String.format("host replication %s 0.0.0.0/0 trust%n", replicationUser);
      var echo = String.format("echo '%s' >> /var/lib/postgresql/data/pg_hba.conf", hbaConf);
      primary.execInContainer("sh", "-c", echo);

      // Reload primary configuration to allow for standby to connect to primary.
      primary.execInContainer("psql", "-U", username, "-d", database, "-c", "SELECT pg_reload_conf();");
      var waitForHbaRelooad = Wait.forLogMessage(".*database system is ready to accept connections.*", 1);
      primary.waitingFor(waitForHbaRelooad);

      // Create replication user.
      var createReplicationUser = "CREATE USER " + replicationUser + " WITH REPLICATION PASSWORD 'abc123'";
      primary.execInContainer("psql", "-U", username, "-d", database, "-c", createReplicationUser);

      // Make a temporary container that only gets used to generate the data directory, which we bind to the host
      // filesystem.Don't use PostgreSQLContainer for this because it will start a db and make it very hard to clear
      // out the data dir which is required for pg_basebackup.
      tempStandby = new GenericContainer<>(dockerImageName)
          .withCommand("tail", "-f", "/dev/null") // Start it with something that will keep it alive.
          .withFileSystemBind(hostVolume, tempDirectory) // Bind it to the filesystem on the host.
          .withNetwork(network);
      tempStandby.start();

      // Run pg_basebackup on the temporary container and set the directory to the filesystem on the host.
      // pg_basebackup takes care of configuring our connection to the primary.
      var containerPort = "5432";
      tempStandby.execInContainer("pg_basebackup", "-h", primaryHost, "-p", containerPort,
          "-U", replicationUser, "-D", tempDirectory, "-Fp", "-Xs", "-R", "-P");

      // We can stop it now because we don't need it anymore.
      tempStandby.stop();

      // Finally create the standby container which will be our read-only replica/standby container. Use the
      // host's filesystem to populate the data directory on the standby.
      standby = new PostgreSQLContainer<>(dockerImageName)
          .withUsername(username)
          .withPassword(password)
          .withDatabaseName(database)
          .withFileSystemBind(hostVolume, dataDirectory)
          .withEnv("PGDATA", dataDirectory)
          .withNetwork(network)
          .waitingFor(Wait.forLogMessage(".*started streaming WAL.*", 1));
      standby.start();

      // Make replication synchronous.
      var setSyncStandbyNames = "ALTER SYSTEM SET synchronous_standby_names TO 'walreceiver';";
      primary.execInContainer("psql", "-U", username, "-d", database, "-c", setSyncStandbyNames);
      primary.execInContainer("psql", "-U", username, "-d", database, "-c", "SELECT pg_reload_conf();");
      var waitForSyncConfig = Wait.forLogMessage(".*START_REPLICATION.*", 1);
      primary.waitingFor(waitForSyncConfig);
    } catch (IOException | InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PostgresTesterStartException(e.getMessage());
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

  @Override
  public Integer getReadPort() {
    if (standby == null) {
      throw new IllegalStateException("read only not started");
    }
    return standby.getFirstMappedPort();
  }

  @Override
  public String getReadHost() {
    if (standby == null) {
      throw new IllegalStateException("read only not started");
    }
    return standby.getHost();
  }

  @Override
  public boolean isStarted() {
    return primary != null && standby != null;
  }

  @Override
  public void close() {
    if (primary != null) {
      primary.close();
      primary = null;
    }

    if (standby != null) {
      standby.close();
      standby = null;
    }
  }
}
