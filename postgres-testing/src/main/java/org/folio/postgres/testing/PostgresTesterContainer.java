package org.folio.postgres.testing;

import java.sql.*;
import java.time.Duration;

import org.folio.util.PostgresTester;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresTesterContainer implements PostgresTester {

  static public final String DEFAULT_IMAGE_NAME = "postgres:12-alpine";
  static public final String PUBLICATION_NAME = "replication_pub";

  private PostgreSQLContainer<?> masterContainer;
  private PostgreSQLContainer<?> replicaContainer;
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
  public void start(String database, String username, String password) throws SQLException {
    if (masterContainer != null) {
      throw new IllegalStateException("already started");
    }

    masterContainer = new PostgreSQLContainer<>(dockerImageName)
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password)
        .withStartupTimeout(Duration.ofSeconds(60));
    masterContainer.start();

    replicaContainer = new PostgreSQLContainer<>(dockerImageName)
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password)
        .withCommand("postgres -c default_transaction_read_only=on")
        .withStartupTimeout(Duration.ofSeconds(60));
    replicaContainer.start();

    try (Connection conn = DriverManager.getConnection(
        masterContainer.getJdbcUrl(),
        username,
        password)) {
      // Create a publication on the master container
      Statement createPublicationStatement = conn.createStatement();
      createPublicationStatement.execute(String.format("CREATE PUBLICATION %s FOR ALL TABLES", PUBLICATION_NAME));

      // Get the current position of the master's WAL (Write Ahead Log)
      Statement getCurrentWALStatement = conn.createStatement();
      ResultSet walResultSet = getCurrentWALStatement.executeQuery("SELECT pg_current_wal_lsn()");
      walResultSet.next();
      String walLsn = walResultSet.getString(1);

      // Set up replication on the replica container
      String replicationSql = String.format(
          "CREATE SUBSCRIPTION my_subscription CONNECTION 'host=%s port=%s dbname=%s user=%s password=%s' " +
              "PUBLICATION %s WITH (slot_name = my_slot, create_slot = true, start_lsn = '%s', " +
              "slot_type = logical)", masterContainer.getContainerIpAddress(),
          masterContainer.getFirstMappedPort(), database, username, password, PUBLICATION_NAME, walLsn);

      try (Connection replicaConn = DriverManager.getConnection(
          replicaContainer.getJdbcUrl(),
          username,
          password)) {
        Statement replicaStatement = replicaConn.createStatement();
        replicaStatement.execute(replicationSql);
      }
    }
  }

  @Override
  public Integer getPort() {
    if (masterContainer == null) {
      throw new IllegalStateException("not started");
    }
    return masterContainer.getFirstMappedPort();
  }

  @Override
  public String getHost() {
    if (masterContainer == null) {
      throw new IllegalStateException("not started");
    }
    return masterContainer.getHost();
  }

  @Override
  public boolean isStarted() {
    return masterContainer != null;
  }

  @Override
  public void close() {
    if (masterContainer != null) {
      masterContainer.close();
      masterContainer = null;
    }
  }
}
