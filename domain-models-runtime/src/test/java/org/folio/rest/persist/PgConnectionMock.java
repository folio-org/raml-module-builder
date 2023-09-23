package org.folio.rest.persist;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgNotice;
import io.vertx.pgclient.PgNotification;
import io.vertx.pgclient.impl.PgDatabaseMetadata;
import io.vertx.sqlclient.PrepareOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.spi.DatabaseMetadata;

public class PgConnectionMock implements PgConnection {
  public class PgConnectionMockException extends RuntimeException {
    public PgConnectionMockException() {
      super();
    }
  }

  @Override
  public PgConnection notificationHandler(Handler<PgNotification> handler) {
    return this;
  }

  @Override
  public PgConnection cancelRequest(Handler<AsyncResult<Void>> handler) {
    return this;
  }

  @Override
  public Future<Void> cancelRequest() {
    return Future.succeededFuture();
  }

  @Override
  public int processId() {
    return 0;
  }

  @Override
  public int secretKey() {
    return 0;
  }

  @Override
  public PgConnection prepare(String s, Handler<AsyncResult<PreparedStatement>> handler) {
    prepare(s).onComplete(handler);
    return this;
  }

  @Override
  public Future<PreparedStatement> prepare(String s) {
    return Future.failedFuture(new PgConnectionMockException());
  }

  @Override
  public SqlConnection prepare(String sql, PrepareOptions options, Handler<AsyncResult<PreparedStatement>> handler) {
    prepare(sql, options).onComplete(handler);
    return this;
  }

  @Override
  public Future<PreparedStatement> prepare(String sql, PrepareOptions options) {
    return prepare(sql);
  }

  @Override
  public PgConnection exceptionHandler(Handler<Throwable> handler) {
    return this;
  }

  @Override
  public PgConnection closeHandler(Handler<Void> handler) {
    return this;
  }

  @Override
  public PgConnection noticeHandler(Handler<PgNotice> handler) {
    throw new RuntimeException();
  }

  @Override
  public void begin(Handler<AsyncResult<Transaction>> handler) {
    begin().onComplete(handler);
  }

  @Override
  public Future<Transaction> begin() {
    throw new PgConnectionMockException();
  }

  @Override
  public Transaction transaction() {
    return null;
  }

  @Override
  public boolean isSSL() {
    return false;
  }

  @Override
  public Query<RowSet<Row>> query(String s) {
    throw new PgConnectionMockException();
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
    throw new PgConnectionMockException();
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String sql, PrepareOptions options) {
    throw new PgConnectionMockException();
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
  public DatabaseMetadata databaseMetadata() {
    return new PgDatabaseMetadata("12.0.0");
  }
}
