package org.folio.rest.persist;

public final class PostgresClientHelper {
  /**
   * For testing only circumvent the private visibility of PostgresClient.getClient().
   */
  public static final io.vertx.sqlclient.Pool getClient(PostgresClient postgresClient) {
    return postgresClient.getClient();
  }

  /**
   * For testing only circumvent the package visibility of PostgresClient.sharedPgPool.
   */
  public static void setSharedPgPool(boolean sharedPgPool) {
    PostgresClient.setSharedPgPool(sharedPgPool);
  }
}
