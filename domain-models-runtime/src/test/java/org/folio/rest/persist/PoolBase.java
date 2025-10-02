package org.folio.rest.persist;

import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PrepareOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;

/**
 * Base mock implementation to be extended for testing.
 */
public class PoolBase implements Pool {

  @Override
  public Future<SqlConnection> getConnection() {
    return Future.succeededFuture();
  }

  @Override
  public Query<RowSet<Row>> query(String sql) {
    return null;
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String sql) {
    return null;
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String sql, PrepareOptions options) {
    return null;
  }

  @Override
  public Future<Void> close() {
    return Future.succeededFuture();
  }

  @Override
  public int size() {
    return 0;
  }
}
