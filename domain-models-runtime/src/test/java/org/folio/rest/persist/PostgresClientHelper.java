package org.folio.rest.persist;

import io.vertx.pgclient.PgPool;

public final class PostgresClientHelper {
  /**
   * For testing only circumvent the private visibility of PostgresClient.getClient().
   */
  public static final PgPool getClient(PostgresClient postgresClient) {
    return postgresClient.getClient();
  }

  /**
   * For testing only circumvent the package visibility of PostgresClient.sharedPgPool.
   */
  public static final void setSharedPgPool(boolean sharedPgPool) {
    PostgresClient.sharedPgPool = sharedPgPool;
  }
}
