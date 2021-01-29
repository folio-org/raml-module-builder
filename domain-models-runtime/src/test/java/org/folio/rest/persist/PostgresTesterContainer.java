package org.folio.rest.persist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresTesterContainer implements PostgresTester {

  static Logger log = LogManager.getLogger(PostgresTesterContainer.class);

  private static PostgreSQLContainer<?> postgresSQLContainer;

  @Override
  public void start(String database, String username, String password) {
    log.info("start");
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
