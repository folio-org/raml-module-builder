package org.folio.rest.persist;

import io.vertx.ext.asyncsql.AsyncSQLClient;

public final class PostgresClientHelper {
  /**
   * For testing only circumvent the private visibility of PostgresClient.getClient().
   */
  public static final AsyncSQLClient getClient(PostgresClient postgresClient) {
    return postgresClient.getClient();
  }
}
