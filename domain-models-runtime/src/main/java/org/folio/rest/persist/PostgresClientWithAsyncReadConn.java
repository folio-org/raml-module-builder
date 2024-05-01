package org.folio.rest.persist;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgPool;

import java.util.HashMap;
import java.util.Map;

public class PostgresClientWithAsyncReadConn {
  /**
   * Allows for pools to be reused when in shared pool mode.
   */
  private static final Map<Vertx,PgPool> PG_POOLS_ASYNC_READER = new HashMap<>();
  private static final String    HOST_READER = "host_async_reader";
  private static final String    PORT_READER = "port_async_reader";
  private final PostgresClient postgresClient;
  private PgPool readAsyncClient;

  public PostgresClientWithAsyncReadConn(PostgresClient postgresClient) {
    this.postgresClient = postgresClient;
    init();
  }

  private void init() {
    // TODO
  }

  public Future<PgConnection> getAsyncReadConnection() {
    return postgresClient.getConnection(readAsyncClient)
        .recover(e -> {
          if (! "Timeout".equals(e.getMessage())) {
            return Future.failedFuture(e);
          }
          return Future.failedFuture("Timeout for DB_HOST_ASYNC_READER:DB_PORT_ASYNC_READER="
              + " TODO -- get from this config :"
              + " TODO -- get from this config");
        });
  }
}
