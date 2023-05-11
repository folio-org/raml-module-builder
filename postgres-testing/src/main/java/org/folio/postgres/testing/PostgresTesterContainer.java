package org.folio.postgres.testing;

import org.folio.util.PostgresTesterStartException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.folio.util.PostgresTester;

public class PostgresTesterContainer implements PostgresTester {
  public static final String DEFAULT_IMAGE_NAME = "postgres:12-alpine";
  public static final String PRIMARY_ALIAS = "postgresprimary";
  public static final String STANDBY_ALIAS = "postgresstandby";

  private static final int READY_MESSAGE_TIMES = 2;

  private static final Logger LOG = LoggerFactory.getLogger(PostgresTester.class);
  private static boolean hasLog = false;

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
   *  Create postgres container with default image Postgres 12.
   */
  public PostgresTesterContainer() {
    this(DEFAULT_IMAGE_NAME);
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

    try {
      var replicationSh = Transferable.of(
            "echo 'synchronous_commit=remote_apply' >> /var/lib/postgresql/data/postgresql.conf\n"
          + "echo \"synchronous_standby_names='*'\" >> /var/lib/postgresql/data/postgresql.conf\n"
          + "echo 'host replication replicator 0.0.0.0/0 trust' >> /var/lib/postgresql/data/pg_hba.conf\n"
          + "psql -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\" -c 'CREATE USER replicator WITH REPLICATION'");
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
          + "pg_basebackup -h postgresprimary -p 5432 -U replicator "
          +   "-D /var/lib/postgresql/data -Fp -Xs -R -P\n"
          + "pg_ctl -D /var/lib/postgresql/data start\n",
          0755);
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
    } catch (Exception e) {
      Thread.currentThread().interrupt();
      throw new PostgresTesterStartException(e.getMessage(), e);
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

  /**
   * On the Docker network the hostname aliases are {@link #PRIMARY_ALIAS} and {@link #STANDBY_ALIAS},
   * they listen on port 5432.
   */
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
