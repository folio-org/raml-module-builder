package org.folio.postgres.testing;

import java.time.Duration;

import org.folio.util.PostgresTester;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresTesterContainer implements PostgresTester {

  private PostgreSQLContainer<?> postgreSQLContainer;
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
    this("postgres:12-alpine");
  }

  // S2095: Resources should be closed
  // We can't close in start. As this whole class is Closeable!
  @java.lang.SuppressWarnings({"squid:S2095"})
  /**
   * Start the container.
   */
  @Override
  public void start(String database, String username, String password) {
    if (postgreSQLContainer != null) {
      throw new IllegalStateException("already started");
    }
    postgreSQLContainer = new PostgreSQLContainer<>(dockerImageName)
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password)
        .withStartupTimeout(Duration.ofSeconds(60));
    postgreSQLContainer.start();
  }

  @Override
  public Integer getPort() {
    if (postgreSQLContainer == null) {
      throw new IllegalStateException("not started");
    }
    return postgreSQLContainer.getFirstMappedPort();
  }

  @Override
  public String getHost() {
    if (postgreSQLContainer == null) {
      throw new IllegalStateException("not started");
    }
    return postgreSQLContainer.getHost();
  }

  @Override
  public boolean isStarted() {
    return postgreSQLContainer != null;
  }

  @Override
  public void close() {
    if (postgreSQLContainer != null) {
      postgreSQLContainer.close();
      postgreSQLContainer = null;
    }
  }
}
