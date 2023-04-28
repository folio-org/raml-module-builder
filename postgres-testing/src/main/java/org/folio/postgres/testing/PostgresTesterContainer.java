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

    String replicationSlot = "replication_slot1";
    String replicationUser = "replicator";
    String replicationPassword = "abc123";

    File primaryPgConfig = File.createTempFile("primary-postgresql", ".conf");
    FileUtils.writeStringToFile(primaryPgConfig, pgConfPrimary(), "UTF-8");

    File hbaConfig = File.createTempFile("secondary-postgresql", ".conf");
    FileUtils.writeStringToFile(hbaConfig, hbaConf(replicationUser), "UTF-8");

    Network network = Network.newNetwork();

    primary = new PostgreSQLContainer<>(dockerImageName)
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password)
        .withNetwork(network)
        .withStartupTimeout(Duration.ofSeconds(60))
        //.withCopyFileToContainer(MountableFile.forHostPath(primaryPgConfig.getPath()), "/var/postgresql/data/postgresql.conf")
        // TODO Container won't start if this is set here.
        //.withCopyFileToContainer(MountableFile.forHostPath(hbaConfig.getPath()), "/var/lib/postgresql/data/pg_hba.conf")
        .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
    primary.start();

    //logExecResult(primary.execInContainer("sh", "-c", "echo '" + pgConfPrimary() + "' >> /var/lib/postgresql/data/postgresql.conf"));
    //logExecResult(primary.execInContainer("sh", "-c", "echo 'port = " + primary.getFirstMappedPort() + "' >> /var/lib/postgresql/data/postgresql.conf"));

    logExecResult(primary.execInContainer("sh", "-c", "echo '" + hbaConf(replicationUser) + "' >> /var/lib/postgresql/data/pg_hba.conf"));

    //logExecResult(primary.execInContainer("cat", "/var/lib/postgresql/data/pg_hba.conf"));
    //logExecResult(primary.execInContainer("cat", "/var/lib/postgresql/data/postgresql.conf"));

    System.out.println("----------------------------------------------");
    System.out.println("Restarting primary to make changes take effect");
    System.out.println("----------------------------------------------");
    logExecResult(primary.execInContainer("su-exec", "postgres", "pg_ctl", "reload"));

    Thread.sleep(5000);

    String createReplicationUser = "CREATE USER " + replicationUser + " WITH REPLICATION PASSWORD '" + replicationPassword + "'";
    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", createReplicationUser));

// How to get files so that we can use withCopyFileToContainer. Probably don't want to do this.
//    String createSlot = "SELECT * FROM pg_create_physical_replication_slot('" + replicationSlot + "');";
//    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", createSlot));
//    String inspectSlot = "SELECT * FROM pg_replication_slots;";
//    logExecResult(primary.execInContainer("psql", "-U", username, "-d", database, "-c", inspectSlot));
//    String primaryConnInfo = "host=" + primary.getHost() +
//    " port=" + primary.getFirstMappedPort() + " user=" + replicationUser + " password=" + replicationPassword;
//    File standbyPgConfig = File.createTempFile("secondary-postgresql", ".conf");
//    //FileUtils.writeStringToFile(standbyPgConfig, pgConfStandby(primary.getHost(), primary.getFirstMappedPort(), replicationUser, replicationPassword, replicationSlot), "UTF-8");
//    FileUtils.writeStringToFile(standbyPgConfig, pgConfStandby("127.0.0.1", primary.getFirstMappedPort(), replicationUser, replicationPassword, replicationSlot), "UTF-8");
//    File standbySignal = File.createTempFile("standby", ".signal");
//    FileUtils.writeStringToFile(standbySignal, "", "UTF-8");

    // Don't use PostgreSQLContainer because it will create a db and make it very hard to delete things.
    standby = new GenericContainer<>(dockerImageName)
        .withCommand("tail", "-f", "/dev/null")
        .withNetwork(network);
//        .withEnv("POSTGRES_DB", "")
//        .withEnv("POSTGRES_USER", username)
//        .withEnv("POSTGRES_PASSWORD", password)
//        .withCommand("postgres", "-c", "fsync=off", "-c", "synchronous_commit=off")
//        .withNetwork(network)
        // TODO This line causes the container to not start. But the default hba file seems to contain what we need.
        //.withCopyFileToContainer(MountableFile.forHostPath(hbaConfig.getPath()), "/var/lib/postgresql/data/pg_hba.conf")
        //.withCopyFileToContainer(MountableFile.forHostPath(standbyPgConfig.getPath()), "/var/lib/postgresql.conf")
        // TODO This line is causing the container to not start so I try adding the signal file after the container has started.
        //.withCopyFileToContainer(MountableFile.forHostPath(standbySignal.getPath()), "/var/lib/postgresql/data/standby.signal")
        //.waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));

    standby.start();

//    System.out.println("-------------");
//    System.out.println("Can it start");
//    System.out.println("-------------");
//    logExecResult(standby.execInContainer("su-exec", "postgres", "pg_ctl", "start"));

    // Show the location of the data directory on the standby.
    String getDataDir = "SHOW data_directory;";
    logExecResult(standby.execInContainer("psql", "-U", username, "-d", database, "-c", getDataDir));

    //String primaryHost = primary.getContainerName().substring(1, primary.getContainerName().length());
    String primaryHost = "host.docker.internal";
    System.out.println("The primary host: " + primaryHost);

    // Can I connect via other means?
    System.out.println("--------------------------------");
    System.out.println("Netcat to primary from secondary");
    System.out.println("--------------------------------");
    System.out.println("Primary host: " + primary.getHost());
    System.out.println("Primary first mapped port: " + primary.getFirstMappedPort());
    //System.out.println("Primary first mapped port: " + primary.port());
    logExecResult(standby.execInContainer("nc", "-vz", primaryHost, String.valueOf(primary.getFirstMappedPort())));

    // Perform pg_basebackup on the standby.


    // My many failed attempts to clear out the data directory.
    //logExecResult(standby.execInContainer("sh", "-c", "chown -R postgres:postgres /var/lib/postgresql"));
    //logExecResult(standby.execInContainer("su-exec", "postgres", "rm", "-rf", "/var/lib/postgresql/data/*"));
    //logExecResult(standby.execInContainer("sh", "-c", "rm -rf /var/lib/postgresql/data/*"));
    //logExecResult(standby.execInContainer("su-exec", "postgres", "pg_ctl", "stop"));
    //logExecResult(standby.execInContainer("rm", "-rf", "/var/lib/postgresql/data/*"));

    System.out.println("-------------");
    System.out.println("Setting perms");
    System.out.println("-------------");
    //logExecResult(standby.execInContainer("su-exec", "postgres", "pg_ctl", "stop"));
    //logExecResult(standby.execInContainer("", "", "sh", "-c", "chown -R postgres:postgres /var/lib/postgresql"));
    logExecResult(standby.execInContainer("ls", "-al", "/var/lib/postgresql/data"));
    logExecResult(standby.execInContainer("su-exec", "postgres", "sh", "-c", "rm -rf /var/lib/postgresql"));
    logExecResult(standby.execInContainer("su-exec", "postgres", "sh", "-c", "mkdir /var/lib/postgresql"));
    logExecResult(standby.execInContainer("su-exec", "postgres", "sh", "-c", "chmod 700 /var/lib/postgresql"));
    logExecResult(standby.execInContainer("ls", "-al", "/var/lib/postgresql/data"));

    // This succeeds.
    System.out.println("---------------------");
    System.out.println("Running pg_basebackup");
    System.out.println("---------------------");
    String dataDirectory = "/var/lib/postgresql/data/";
    logExecResult(standby.execInContainer("su-exec", "postgres", "pg_basebackup", "-h", primaryHost, "-p",
        String.valueOf(primary.getFirstMappedPort()), "-U", replicationUser, "-D", dataDirectory, "-Fp", "-Xs", "-R", "-P"));

    // This shouldn't be necessary on the standby.
    //logExecResult(standby.execInContainer("sh", "-c", "echo '" + hbaConf(replicationUser) + "' >> /var/lib/postgresql/data/pg_hba.conf"));

    // This shouldn't be necessary on the standby.
    //logExecResult(standby.execInContainer("cat", "/var/lib/postgresql/data/pg_hba.conf"));
    //logExecResult(standby.execInContainer("cat", "/var/lib/postgresql.conf"));

    // One online source suggests removing everything in the data directory in the standby.
    //logExecResult(standby.execInContainer("su-exec", "postgres", "pg_ctl", "stop"));
    Thread.sleep(5000);

    //logExecResult(standby.execInContainer("su-exec", "rm", "-rf", "/var/lib/postgresql/data/*"));

    // Try to add the standby signal file and get replication to start working. This is required according to
    // the postgres docs.
    //logExecResult(standby.execInContainer("touch", "/var/lib/postgresql/data/standby.signal"));

    // Now things can be there.
    logExecResult(standby.execInContainer("ls", "/var/lib/postgresql/data"));

    // My failed attempts to create my own primary_conninfo object. All failed.
    //String primaryConnInfo = pgConfStandby(primaryHost, primary.getFirstMappedPort(), replicationUser, replicationPassword);
    ///System.out.println("primaryConnInfo: " + primaryConnInfo);
    //logExecResult(standby.execInContainer("sh", "-c", "echo '" + primaryConnInfo + "' >> /var/lib/postgresql/data/postgresql.conf"));
    // "echo \"primary_conninfo = 'user=myuser password=''mypassword'' host=primaryhost port=5432 sslmode=require'\" >> /var/lib/postgresql/data/postgresql.conf"
    //String echo = String.format("echo \"primary_conninfo = 'host=host.docker.internal port=%s user=%s password=%s'\n\" >> /var/lib/postgresql/data/postgresql.conf", primary.getFirstMappedPort(), replicationUser, replicationPassword);
    //logExecResult(standby.execInContainer("sh", "-c", echo));
    // Take a look at the configuration generated by pg_basebackup.
    System.out.println("---------------------------");
    System.out.println("Contents of postgresql.auto.conf");
    System.out.println("---------------------------");
    logExecResult(standby.execInContainer("cat", "/var/lib/postgresql/data/postgresql.auto.conf"));
    //logExecResult(standby.execInContainer("cat", "/var/lib/postgresql/data/postgresql.conf"));

    // Restart
    //logExecResult(primary.execInContainer("su-exec", "postgres", "pg_ctl", "reload"));
    //logExecResult(standby.execInContainer("su-exec", "postgres", "pg_ctl", "reload"));

    //logExecResult(standby.execInContainer("sh", "-c", "chown -R postgres:postgres /var/lib/postgresql"));
    //logExecResult(standby.execInContainer("su-exec", "postgres", "sh", "-c", "chmod 700 /var/lib/postgresql"));

    logExecResult(standby.execInContainer("ls", "-al", "/var/lib/postgresql/data"));


    logExecResult(standby.execInContainer("su-exec", "postgres", "pg_ctl", "start"));

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
        "host replication " + user + " 0.0.0.0/0 trust\n";
        //"host all all 0.0.0.0/0 trust\n";
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
