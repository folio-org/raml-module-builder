package org.folio.postgres.testing;

import org.folio.util.PostgresTester;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.logging.Level;
import java.util.logging.Logger;

public class PostgresTesterContainer implements PostgresTester {

  private static final Logger log = Logger.getLogger("PostgresTesterContainer");

  private PostgreSQLContainer<?> postgresSQLContainer;

  @Override
  public void start(String database, String username, String password) {
    if (postgresSQLContainer != null) {
      throw new IllegalStateException("postgresTesterContainer already started");
    }
    try {
      postgresSQLContainer = new PostgreSQLContainer<>("postgres:12-alpine")
          .withDatabaseName(database)
          .withUsername(username)
          .withPassword(password);
      postgresSQLContainer.start();
    } catch (Exception e) {
      postgresSQLContainer.stop();
      postgresSQLContainer = null;
      log.log(Level.WARNING, e.getMessage(), e);
    }
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
