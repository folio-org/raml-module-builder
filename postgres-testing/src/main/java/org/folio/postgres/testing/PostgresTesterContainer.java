package org.folio.postgres.testing;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.time.Duration;
import java.util.UUID;

import org.folio.util.PostgresTester;
import org.testcontainers.containers.*;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;


public class PostgresTesterContainer implements PostgresTester {
  static public final String DEFAULT_IMAGE_NAME = "postgres:12-alpine";

  private PostgreSQLContainer<?> primary;
  private GenericContainer<?> tempStandby;
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

    String replicationUser = "replicator";
    String replicationPassword = "abc123";
    String primaryHost = "primaryhost";
    //String primaryHost = "host.docker.internal";

    Network network = Network.newNetwork();

    primary = new PostgreSQLContainer<>(dockerImageName)
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password)
        .withNetwork(network)
        .withNetworkAliases(primaryHost)
        .withEnv("PGOPTIONS", "-c synchronous_commit=on")
        .withStartupTimeout(Duration.ofSeconds(60))
        .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
    primary.start();

    logExecResult(primary.execInContainer("sh", "-c", "echo '" + hbaConf(replicationUser) + "' >> /var/lib/postgresql/data/pg_hba.conf"));

    System.out.println("----------------------------------------------");
    System.out.println("Restarting primary to make changes take effect");
    System.out.println("----------------------------------------------");
    logExecResult(primary.execInContainer("su-exec", "postgres", "pg_ctl", "reload"));

    Thread.sleep(2000);

    String createReplicationUser = "CREATE USER " + replicationUser + " WITH REPLICATION PASSWORD '" + replicationPassword + "'";
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", createReplicationUser));

    // Don't use PostgreSQLContainer because it will create a db and make it very hard to clear out the data dir
    // which is required for pg_basebackup.
    String dataDirectory = "/var/lib/postgresql/standby/";
    String tempDirectory = "/tmp/standby/";
    String hostVolume = "/tmp/rmb-standby-" + UUID.randomUUID();

    tempStandby = new GenericContainer<>(dockerImageName)
        .withCommand("tail", "-f", "/dev/null")
        .withFileSystemBind(hostVolume, tempDirectory)
        .withNetwork(network);
    tempStandby.start();

    System.out.println("--------------------------------");
    System.out.println("Netcat to primary from secondary");
    System.out.println("--------------------------------");
    System.out.println("The primary host: " + primaryHost);
    System.out.println("Primary getHost: " + primary.getHost());
    System.out.println("Primary first mapped port: " + primary.getFirstMappedPort());
    logExecResult(tempStandby.execInContainer("nc", "-vz", primaryHost, String.valueOf(primary.getFirstMappedPort())));

    System.out.println("---------------------");
    System.out.println("Running pg_basebackup");
    System.out.println("---------------------");
    logExecResult(tempStandby.execInContainer("pg_basebackup", "-h", primaryHost, "-p",
        "5432", "-U", replicationUser, "-D", tempDirectory, "-Fp", "-Xs", "-R", "-P"));

    System.out.println("-----------------------");
    System.out.println("Result of pg_basebackup");
    System.out.println("-----------------------");
    logExecResult(tempStandby.execInContainer("ls", "-al", tempDirectory));
    logExecResult(tempStandby.execInContainer("cat", tempDirectory + "postgresql.auto.conf"));
    tempStandby.stop();

    // Try to expose the port now that postgres is running.
    System.out.println("---------------------");
    System.out.println("Change data directory");
    System.out.println("---------------------");
    standby = new PostgreSQLContainer<>(dockerImageName)
        .withUsername(username)
        .withPassword(password)
        .withDatabaseName(database)
        .withFileSystemBind(hostVolume, dataDirectory)
        .withEnv("PGDATA", dataDirectory)
        .withEnv("PGOPTIONS", "-c synchronous_commit=on")
        .withNetwork(network)
        .waitingFor(Wait.forLogMessage(".*started streaming WAL.*", 1));
    standby.start();

    System.out.println("Value of data dir in env: " + standby.getEnvMap().get("PGDATA"));
    logExecResult(standby.execInContainer("cat", dataDirectory + "postgresql.auto.conf"));
    logExecResult(standby.execInContainer("ls", "-al", dataDirectory));

    String getDataDir = "SHOW data_directory;";
    logExecResult(standby.execInContainer("psql", "-U", username, "-d", database, "-c", getDataDir));

    System.out.println("--------------------------");
    System.out.println("Make streaming synchronous");
    System.out.println("--------------------------");
    String verifyStreaming = "SELECT * FROM pg_stat_replication;";
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", verifyStreaming));
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", "ALTER SYSTEM SET synchronous_standby_names TO 'walreceiver';"));
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", "SELECT pg_reload_conf();"));
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", verifyStreaming));

    System.out.println("----------------");
    System.out.println("Test replication");
    System.out.println("----------------");

    String createTablePrimary = "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(50));";
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", createTablePrimary));
    String insertRecord = "INSERT INTO users (name) VALUES ('John Doe');";
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", insertRecord));
    // TODO this delay is needed which of course is bad. This _only_ observed on mac. On linux it doesn't appear to
    // be needed when using the file system bind. Because of the host volume mount? Need to make it sync rather than async perhaps.
    //Thread.sleep(1000);
    String selectStandby = "SELECT * FROM users;"; // Should have 1 row.
    logExecResult(standby.execInContainer("psql", "-U", username, "-d", database, "-c", selectStandby));

    testConnection(primary.getFirstMappedPort(), primary.getHost(), database, username, password, "");
    testConnection(standby.getFirstMappedPort(), standby.getHost(), database, username, password, "");

    // Take a look at the logs.
    String primaryLogs = primary.getLogs();
    System.out.println("------------");
    System.out.println("Primary logs");
    System.out.println("------------");
    System.out.println(primaryLogs);
    String tempStandbyLogs = tempStandby.getLogs();
    System.out.println("-----------------");
    System.out.println("Temp standby logs");
    System.out.println("-----------------");
    System.out.println(tempStandbyLogs);
    String standbyLogs = standby.getLogs();
    System.out.println("------------");
    System.out.println("Standby logs");
    System.out.println("------------");
    System.out.println(standbyLogs);
  }

  private void testConnection(int port, String host, String database, String username, String password, String sql) {
    String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);

    try {
      Connection conn = DriverManager.getConnection(url, username, password);
      System.out.println("Connection to database established.");
      // Use the connection to execute SQL queries here
      conn.close();
      System.out.println("Connection to database closed.");
    } catch (SQLException e) {
      System.err.println("Connection to database failed: " + e.getMessage());
    }
  }

  public String hbaConf(String user) {
    return
        "host replication " + user + " 0.0.0.0/0 trust\n";
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
