package org.folio.rest.persist;

import io.vertx.core.Vertx;
import org.folio.rest.persist.cache.ConnectionCache;

/**
 * Clients which, for performance reasons, would like to take advantage of asynchronous read replication can use
 * this class to create their instance of the {@link PostgresClient} using the {@link PostgresClient#getInstance(Vertx)}
 * or the {@link PostgresClient#getInstance(Vertx, String)} method.
 * Note that the data on the read-only host is asynchronously replicated. This means that it is eventually consistent
 * and not ACID. In other words, it is not guaranteed to be up-to-date or synchronized with the read/write host.
 * The async read host used by this class is therefore intended to be used in cases where eventually consistent data
 * is acceptable, such as reporting.
 * APIs using the async client should have a warning in the API documentation that it uses stale data (for performance
 * reasons).
 */
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
