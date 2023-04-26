package org.folio.postgres.testing;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import java.time.Duration;

import org.folio.util.PostgresTester;
import org.testcontainers.containers.Container;
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

    String replicationSlot = "replication_slot1";
    String replicationUser = "replicator";
    String replicationPassword = "password";

    File primaryPgConfig = File.createTempFile("primary-postgresql", ".conf");
    FileUtils.writeStringToFile(primaryPgConfig, pgConfPrimary(), "UTF-8");

    File hbaConfig = File.createTempFile("secondary-postgresql", ".conf");
    FileUtils.writeStringToFile(hbaConfig, hbaConf(replicationUser), "UTF-8");

    primary = new PostgreSQLContainer<>(dockerImageName)
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password)
        .withCopyFileToContainer(MountableFile.forHostPath(primaryPgConfig.getPath()), "/var/lib/postgresql.conf")
        //.withCopyFileToContainer(MountableFile.forHostPath(hbaConfig.getPath()), "/var/lib/postgresql/data/pg_hba.conf")
        .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
    primary.start();

    logExecResult(primary.execInContainer("sh", "-c", "echo '" + hbaConf(replicationUser) + "' >> /var/lib/postgresql/data/pg_hba.conf"));

    logExecResult(primary.execInContainer("cat", "/var/lib/postgresql/data/pg_hba.conf"));
    logExecResult(primary.execInContainer("cat", "/var/lib/postgresql.conf"));

    String createReplicationUser = "CREATE USER " + replicationUser + " WITH REPLICATION PASSWORD '" + replicationPassword + "'";
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", createReplicationUser));

//    String createSlot = "SELECT * FROM pg_create_physical_replication_slot('" + replicationSlot + "');";
//    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", createSlot));

//    String inspectSlot = "SELECT * FROM pg_replication_slots;";
//    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", inspectSlot));

    String primaryConnInfo = "host=" + primary.getHost() +
    " port=" + primary.getFirstMappedPort() + " user=" + replicationUser + " password=" + replicationPassword;

    File standbyPgConfig = File.createTempFile("secondary-postgresql", ".conf");
    //FileUtils.writeStringToFile(standbyPgConfig, pgConfStandby(primary.getHost(), primary.getFirstMappedPort(), replicationUser, replicationPassword, replicationSlot), "UTF-8");
    FileUtils.writeStringToFile(standbyPgConfig, pgConfStandby("127.0.0.1", primary.getFirstMappedPort(), replicationUser, replicationPassword, replicationSlot), "UTF-8");

    File standbySignal = File.createTempFile("standby", ".signal");
    FileUtils.writeStringToFile(standbySignal, "", "UTF-8");

    standby = new PostgreSQLContainer<>(dockerImageName)
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password)
        // TODO This line causes the container to not start. But the default hba file seems to contain what we need.
        //.withCopyFileToContainer(MountableFile.forHostPath(hbaConfig.getPath()), "/var/lib/postgresql/data/pg_hba.conf")
        .withCopyFileToContainer(MountableFile.forHostPath(standbyPgConfig.getPath()), "/var/lib/postgresql.conf")
        // TODO This line is causing the container to not start so I try adding the signal file after the container has started.
        //.withCopyFileToContainer(MountableFile.forHostPath(standbySignal.getPath()), "/var/lib/postgresql/data/standby.signal")
        .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));

    standby.start();

    logExecResult(standby.execInContainer("sh", "-c", "echo '" + hbaConf(replicationUser) + "' >> /var/lib/postgresql/data/pg_hba.conf"));

    logExecResult(standby.execInContainer("cat", "/var/lib/postgresql/data/pg_hba.conf"));
    logExecResult(standby.execInContainer("cat", "/var/lib/postgresql.conf"));

    // Show the location of the data directory.
    String getDataDir = "SHOW data_directory;";
    logExecResult(standby.execInContainer("psql", "-U", username, "-d", database, "-c", getDataDir));

    // One online source suggests removing everything in the data directory in the standby.
    //logExecResult(standby.execInContainer("su-exec", "postgres", "pg_ctl", "stop"));
    Thread.sleep(5000);


    // TODO This fails because testcontainers now says that the container is not running even though I only
    // have stopped postgres.
    //logExecResult(standby.execInContainer("su-exec", "rm", "-rf", "/var/lib/postgresql/data/*"));


    // Try to add the standby signal file and get replication to start working. This is required according to
    // the postgres docs.
    logExecResult(standby.execInContainer("touch", "/var/lib/postgresql/data/standby.signal"));

    // Make sure the file is there.
    logExecResult(standby.execInContainer("ls", "/var/lib/postgresql/data"));

    // Restart
    logExecResult(primary.execInContainer("su-exec", "postgres", "pg_ctl", "reload"));
    logExecResult(standby.execInContainer("su-exec", "postgres", "pg_ctl", "reload"));
    //logExecResult(standby.execInContainer("su-exec", "postgres", "pg_ctl", "start"));

    // Maybe wait some time to see the restart take effect?
    Thread.sleep(5000);

    // Check that primary is streaming to replica. This should have one row if replication is working according
    // to everything I have read.
    System.out.println("-----------------------------------------------");
    System.out.println("Is it working? This will have one row if it is.");
    System.out.println("-----------------------------------------------");
    String verifyStreaming = "SELECT * FROM pg_stat_replication;";
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", verifyStreaming));

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
           "host replication " + user + " 0.0.0.0/32 trust\n";
  }

  public String pgConfPrimary() {
    return
           "listen_addresses = '*'\n";
  }

  public String pgConfStandby(String host, int port, String user, String password, String slot) {
    return
        "primary_conninfo = 'host=" + host + " port=" + port + " user=" + user + " password=" + password + "\n";
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
