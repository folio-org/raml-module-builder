package org.folio.rest.persist;

import io.vertx.core.Vertx;

public class PostgresClientWithAsyncReadConn extends PostgresClient {
  protected PostgresClientWithAsyncReadConn(Vertx vertx, String tenantId) throws Exception {
    super(vertx, tenantId);
  }

  public static PostgresClient getInstance(Vertx vertx, String tenantId) {
    var client = getInstanceInternal(vertx, tenantId);
    var initializer = client.getPostgresClientInitializer();
    client.setReaderClient(initializer.getReadClientAsync());
    return client;
  }
}
