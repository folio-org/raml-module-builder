package org.folio.postgres.testing;

import org.folio.util.PostgresTester;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.logging.Level;
import java.util.logging.Logger;

public class PostgresTesterContainer implements PostgresTester {

  private static final Logger log = Logger.getLogger("PostgresTesterContainer");
  private PostgreSQLContainer<?> postgreSQLContainer;
  private String dockerImageName;

  public PostgresTesterContainer(String containerName) {
    this.dockerImageName = containerName;
  }

  public PostgresTesterContainer() {
    this("postgres:12-alpine");
  }

  @Override
  public void start(String database, String username, String password) {
    if (postgreSQLContainer != null) {
      throw new IllegalStateException("already started");
    }
    postgreSQLContainer = new PostgreSQLContainer<>(dockerImageName)
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password);
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

  public void close() {
    if (postgreSQLContainer != null) {
      postgreSQLContainer.close();
      postgreSQLContainer = null;
    }
  }
}
