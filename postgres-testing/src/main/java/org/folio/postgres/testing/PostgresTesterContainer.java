package org.folio.postgres.testing;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import java.time.Duration;

import org.folio.util.PostgresTester;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testcontainers.utility.MountableFile;

import static java.time.temporal.ChronoUnit.SECONDS;

public class PostgresTesterContainer implements PostgresTester {
  static public final String DEFAULT_IMAGE_NAME = "postgres:12-alpine";

  private PostgreSQLContainer<?> primary;
  private GenericContainer<?> standby;
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
    //String primaryHost = "primaryhost";
    String primaryHost = "host.docker.internal";

    Network network = Network.newNetwork();

    primary = new PostgreSQLContainer<>(dockerImageName)
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password)
        .withNetwork(network)
        // TODO This doesn't seem to work.
        //.withNetworkAliases(primaryHost)
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
    standby = new GenericContainer<>(dockerImageName)
        .withCommand("tail", "-f", "/dev/null")
        .withEnv("PGDATA", dataDirectory)
        .withNetwork(network);
    standby.start();

    System.out.println("--------------------------------");
    System.out.println("Netcat to primary from secondary");
    System.out.println("--------------------------------");
    System.out.println("The primary host: " + primaryHost);
    System.out.println("Primary getHost: " + primary.getHost());
    System.out.println("Primary first mapped port: " + primary.getFirstMappedPort());
    logExecResult(standby.execInContainer("nc", "-vz", primaryHost, String.valueOf(primary.getFirstMappedPort())));

    System.out.println("---------------------");
    System.out.println("Running pg_basebackup");
    System.out.println("---------------------");
    logExecResult(standby.execInContainer("su-exec", "postgres", "pg_basebackup", "-h", primaryHost, "-p",
        String.valueOf(primary.getFirstMappedPort()), "-U", replicationUser, "-D", dataDirectory, "-Fp", "-Xs", "-R", "-P"));

    Thread.sleep(2000);

    System.out.println("-----------------------");
    System.out.println("Result of pg_basebackup");
    System.out.println("-----------------------");
    logExecResult(standby.execInContainer("cat", dataDirectory + "postgresql.auto.conf"));
    logExecResult(standby.execInContainer("ls", "-al", dataDirectory));

    // Start up standby.
    logExecResult(standby.execInContainer("su-exec", "postgres", "pg_ctl", "start"));

    String getDataDir = "SHOW data_directory;";
    logExecResult(standby.execInContainer("psql", "-U", username, "-d", database, "-c", getDataDir));

    // Maybe wait some time to see the restart take effect?
    Thread.sleep(2000);

    // Check that primary is streaming to replica. This should have one row if replication is working according
    // to everything I have read.
    System.out.println("-----------------------------------------------");
    System.out.println("Is it working? This will have one row if it is.");
    System.out.println("-----------------------------------------------");
    String verifyStreaming = "SELECT * FROM pg_stat_replication;";
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", verifyStreaming));

    System.out.println("-----------------");
    System.out.println("Test replication.");
    System.out.println("-----------------");
    String createTablePrimary = "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(50));";
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", createTablePrimary));
    String insertRecord = "INSERT INTO users (name) VALUES ('John Doe');";
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", insertRecord));
    Thread.sleep(2000);
    String selectStandby = "SELECT * FROM users;";
    logExecResult(standby.execInContainer("psql", "-U", username, "-d", database, "-c", selectStandby));

    // Let some logs accumulate.
    Thread.sleep(2000);

    // Take a look at the logs.
    String primaryLogs = primary.getLogs();
    System.out.println("------------");
    System.out.println("Primary logs");
    System.out.println("------------");
    System.out.println(primaryLogs);
    String standbyLogs = standby.getLogs();
    System.out.println("------------");
    System.out.println("Standby logs");
    System.out.println("------------");
    System.out.println(standbyLogs);
  }

  public String hbaConf(String user) {
    return
        "host replication " + user + " 0.0.0.0/0 trust\n";
  }

  public String pgConfPrimary() {
    return
           "listen_addresses = \\'127.0.0.1\\'\n";
  }

  public String pgConfStandby(String host, int port, String user, String password) {
    return
        "primary_conninfo = '\\''host=" + host + " port=" + port + " password=''''password'''' user=" + user + "'\\''";
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
