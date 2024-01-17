package org.folio.rest.persist;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgNotice;
import io.vertx.pgclient.PgNotification;
import io.vertx.sqlclient.PrepareOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.spi.DatabaseMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SharedPgConnection implements PgConnection {
  private static final Logger LOG = LogManager.getLogger(SharedPgConnection.class);

  private final PgConnection conn;

  public SharedPgConnection(PgConnection conn) {
    this.conn = conn;
  }

  @Override
  public Future<Void> close() {
    LOG.debug("Calling extended method: close");
    // TODO Here we can intercept close and track availability.
    return conn.close();
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    LOG.debug("Calling extended method: Handler<AsyncResult");
    conn.close(handler);
  }

  @Override
  public PgConnection closeHandler(Handler<Void> handler) {
    LOG.debug("Calling extended method: close");
    return conn.closeHandler(handler);
  }

  @Override
  public PgConnection notificationHandler(Handler<PgNotification> handler) {
    return conn.notificationHandler(handler);
  }

  @Override
  public PgConnection noticeHandler(Handler<PgNotice> handler) {
    return conn.noticeHandler(handler);
  }

  @Override
  public PgConnection cancelRequest(Handler<AsyncResult<Void>> handler) {
    return conn.cancelRequest(handler);
  }

  @Override
  public Future<Void> cancelRequest() {
    return conn.cancelRequest();
  }

  @Override
  public int processId() {
    return conn.processId();
  }

  @Override
  public int secretKey() {
    return conn.secretKey();
  }

  @Override
  public PgConnection prepare(String s, Handler<AsyncResult<PreparedStatement>> handler) {
    return conn.prepare(s, handler);
  }

  @Override
  public Future<PreparedStatement> prepare(String s) {
    return conn.prepare(s);
  }

  @Override
  public SqlConnection prepare(String s, PrepareOptions prepareOptions, Handler<AsyncResult<PreparedStatement>> handler) {
    return conn.prepare(s, prepareOptions, handler);
  }

  @Override
  public Future<PreparedStatement> prepare(String s, PrepareOptions prepareOptions) {
    return conn.prepare(s, prepareOptions);
  }

  @Override
  public PgConnection exceptionHandler(Handler<Throwable> handler) {
    return conn.exceptionHandler(handler);
  }

  @Override
  public void begin(Handler<AsyncResult<Transaction>> handler) {
    conn.begin(handler);
  }

  @Override
  public Future<Transaction> begin() {
    return conn.begin();
  }

  @Override
  public Transaction transaction() {
    return conn.transaction();
  }

  @Override
  public boolean isSSL() {
    return conn.isSSL();
  }

  @Override
  public Query<RowSet<Row>> query(String s) {
    return conn.query(s);
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
    return conn.preparedQuery(s);
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String s, PrepareOptions prepareOptions) {
    return conn.preparedQuery(s, prepareOptions);
  }

  @Override
  public DatabaseMetadata databaseMetadata() {
    return conn.databaseMetadata();
  }
}
