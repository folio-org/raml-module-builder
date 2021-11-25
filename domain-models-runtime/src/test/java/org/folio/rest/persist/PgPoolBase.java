package org.folio.rest.persist;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PrepareOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import java.util.function.Function;

/**
 * Base mock implementation to be extended for testing.
 */
public class PgPoolBase implements PgPool {

  @Override
  public void getConnection(Handler<AsyncResult<SqlConnection>> handler) {
    getConnection().onComplete(handler);
  }

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
  public void close(Handler<AsyncResult<Void>> handler) {
    close().onComplete(handler);
  }

  @Override
  public Future<Void> close() {
    return Future.succeededFuture();
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public PgPool connectHandler(Handler<SqlConnection> handler) {
    return this;
  }

  @Override
  public PgPool connectionProvider(Function<Context, Future<SqlConnection>> provider) {
    return this;
  }

}
