package org.folio.rest.persist;

import io.vertx.core.Vertx;

public class PostgresClientWithAsyncReadConn extends PostgresClient {
  protected PostgresClientWithAsyncReadConn(Vertx vertx, String tenantId) throws Exception {
    super(vertx, tenantId);
  }

  public static PostgresClient getInstance(Vertx vertx, String tenantId) {
    var postgresClient = getInstanceInternal(vertx, tenantId);
    var initializer = postgresClient.getPostgresClientInitializer();
    postgresClient.setReaderClient(initializer.getReadClientAsync());
    return postgresClient;
  }
}
