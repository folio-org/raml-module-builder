package org.folio.postgres.testing;

import org.folio.util.PostgresTester;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresTesterContainer implements PostgresTester {

  private static PostgreSQLContainer<?> postgresSQLContainer;

  @Override
  public void start(String database, String username, String password) {
    postgresSQLContainer = new PostgreSQLContainer<>("postgres:12-alpine")
        .withDatabaseName(database)
        .withUsername(username)
        .withPassword(password);
    postgresSQLContainer.start();
  }

  @Override
  public int getPort() {
    return postgresSQLContainer.getFirstMappedPort();
  }

  @Override
  public String getHost() {
    return postgresSQLContainer.getHost();
  }

  @Override
  public boolean isStarted() {
    return postgresSQLContainer != null;
  }

  @Override
  public boolean stop() {
    if (postgresSQLContainer == null) {
      return false;
    }
    postgresSQLContainer.stop();
    postgresSQLContainer = null;
    return true;
  }
}
