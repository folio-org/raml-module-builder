package org.folio.postgres.testing;

import java.util.Map;

import org.folio.util.PostgresTester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

public class PostgresTesterContainer implements PostgresTester {
  public static final String DEFAULT_IMAGE_NAME = "postgres:12-alpine";

  /**
   * the Docker network the hostname alias for the primary container.
   */
  public static final String PRIMARY_ALIAS = "postgresprimary";

  /**
   * the Docker network the hostname alias for the read-only standby container.
   */
  public static final String STANDBY_ALIAS = "postgresstandby";

  /**
   * Key for an environment variable or system property, that, if present, will configure the replication between the
   * primary and standby containers to be asynchronous. This is an unofficial experimental option that might be removed
   * without notice.
   */
  public static final String POSTGRES_ASYNC_COMMIT = "PG_ASYNC_COMMIT";

  /**
   * The number of milliseconds that replication will take between the primary and the standby in order to simulate
   * a variable real-world replication delay between primary and standby replicas.
   * This is only applied when the experimental {@link #POSTGRES_ASYNC_COMMIT} is applied.
   */
  public static final int SIMULATED_ASYNC_REPLICATION_LAG_MILLISECONDS = 300;

  private static final String IMAGE_NAME = getImageName(System.getenv());

  private static final int READY_MESSAGE_TIMES = 2;

  private static final Logger LOG = LoggerFactory.getLogger(PostgresTesterContainer.class);

  @SuppressWarnings("squid:S1314")  // Suppress false positive "Octal values should not be used"
  private static final int FILE_MODE = 0755; // octal

  private static boolean hasLog;

  private PostgreSQLContainer<?> primary;
  private PostgreSQLContainer<?> standby;
  private String dockerImageName;
  private Network network = Network.newNetwork();

  /**
   * Create postgres container based on given image.
   * @param dockerImageName
   */
  public PostgresTesterContainer(String dockerImageName) {
    this.dockerImageName = dockerImageName;
  }

  /**
   * Create postgres container with the image name configured by environment variable
   * TESTCONTAINERS_POSTGRES_IMAGE, or the default {@link #DEFAULT_IMAGE_NAME} if undefined.
   */
  public PostgresTesterContainer() {
    this(IMAGE_NAME);
  }

  static String getImageName(Map<String, String> env) {
    return env.getOrDefault("TESTCONTAINERS_POSTGRES_IMAGE", DEFAULT_IMAGE_NAME);
  }

  /**
   * The image name configured by environment variable TESTCONTAINERS_POSTGRES_IMAGE,
   * or the default {@link #DEFAULT_IMAGE_NAME} if undefined.
   */
  public static String getImageName() {
    return IMAGE_NAME;
  }

  /**
   * Enable or disable logging of the PostgreSQL containers.
   * <p>
   * Requires
   * <pre>{@code
   * <dependency>
   *   <groupId>org.apache.logging.log4j</groupId>
   *   <artifactId>log4j-slf4j-impl</artifactId>
   *   <scope>test</scope>
   * </dependency>
   * }</pre>
   */
  public static void enableLog(boolean hasLog)  {
    PostgresTesterContainer.hasLog = hasLog;
  }

  /**
   * See {@link #POSTGRES_ASYNC_COMMIT}.
   * @return True if this instance of the class is configured for asynchronous commit.
   */
  public boolean hasAsyncCommitConfig() {
    return System.getProperty(POSTGRES_ASYNC_COMMIT) != null || System.getenv(POSTGRES_ASYNC_COMMIT) != null;
  }

  // S2095: Resources should be closed
  // We can't close in start. As this whole class is Closeable!
  @java.lang.SuppressWarnings({"squid:S2095", "resource"})
  /**
   * Start the container.
   */
  @Override
  public void start(String database, String username, String password) {
    if (primary != null) {
      throw new IllegalStateException("already started");
    }

    var baseReplicationSh =
        "echo 'host replication replicator 0.0.0.0/0 trust' >> /var/lib/postgresql/data/pg_hba.conf\n"
        + "psql -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\" -c 'CREATE USER replicator WITH REPLICATION'";

    var replicationSh = Transferable.of(
        "echo 'synchronous_commit=remote_apply' >> /var/lib/postgresql/data/postgresql.conf\n"
            + "echo \"synchronous_standby_names='*'\" >> /var/lib/postgresql/data/postgresql.conf\n"
            + baseReplicationSh);

    if (hasAsyncCommitConfig()) {
      var simulatedDelayConfig =
          String.format("echo \"recovery_min_apply_delay = '%sms'\" >> /var/lib/postgresql/data/postgresql.conf%n",
              SIMULATED_ASYNC_REPLICATION_LAG_MILLISECONDS);
      replicationSh = Transferable.of(simulatedDelayConfig + baseReplicationSh);
    }

    primary = new PostgreSQLContainer<>(dockerImageName)
        .withCopyToContainer(replicationSh, "/docker-entrypoint-initdb.d/replication.sh")
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password)
        .withNetwork(network)
        .withNetworkAliases(PRIMARY_ALIAS)
        .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", READY_MESSAGE_TIMES));
    primary.start();
    if (hasLog) {
      primary.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams().withPrefix("primary"));
    }

    var basebackupSh = Transferable.of(
        "pg_ctl -D /var/lib/postgresql/data stop\n"
            + "rm -rf /var/lib/postgresql/data/*\n"
            + "pg_basebackup -h postgresprimary -p 5432 -U replicator -D /var/lib/postgresql/data -Fp -Xs -R -P\n"
            + "pg_ctl -D /var/lib/postgresql/data start\n",
        FILE_MODE);
    standby = new PostgreSQLContainer<>(dockerImageName)
        .withCopyToContainer(basebackupSh, "/docker-entrypoint-initdb.d/basebackup.sh")
        .withUsername(username)
        .withPassword(password)
        .withDatabaseName(database)
        .withNetwork(network)
        .withNetworkAliases(STANDBY_ALIAS)
        .waitingFor(Wait.forLogMessage(".*started streaming WAL.*", READY_MESSAGE_TIMES));
    standby.start();
    if (hasLog) {
      standby.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams().withPrefix("standby"));
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
  public Network getNetwork() {
    return network;
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
