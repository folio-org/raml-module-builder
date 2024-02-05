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

import java.util.UUID;

public class CachedPgConnection implements PgConnection {
  private static final Logger LOG = LogManager.getLogger(CachedPgConnection.class);
  private final PgConnection connection;
  private final PostgresConnectionManager manager;
  private final UUID sessionId;
  private final String tenantId;
  private final long timestamp;
  private boolean available;

  public CachedPgConnection(String tenantId, PgConnection connection, PostgresConnectionManager manager) {
    this.connection = connection;
    this.manager = manager;
    this.sessionId = UUID.randomUUID();
    this.tenantId = tenantId;
    this.timestamp = System.currentTimeMillis();
  }

  @Override
  public Future<Void> close() {
    LOG.debug("Calling close: {} {}", this.tenantId, this.sessionId);
    this.available = true;
    this.manager.tryClose(this);
    return Future.succeededFuture();
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    LOG.debug("Calling close: Handler<AsyncResult<Void>>");
    this.available = true;
    this.manager.tryClose(this);
  }

  @Override
  public PgConnection closeHandler(Handler<Void> handler) {
    LOG.debug("Calling close: closeHandler Handler<Void>");
    this.available = true;
    this.manager.tryClose(this);
    return this;
  }

  @Override
  public PgConnection notificationHandler(Handler<PgNotification> handler) {
    return connection.notificationHandler(handler);
  }

  @Override
  public PgConnection noticeHandler(Handler<PgNotice> handler) {
    return connection.noticeHandler(handler);
  }

  @Override
  public PgConnection cancelRequest(Handler<AsyncResult<Void>> handler) {
    return connection.cancelRequest(handler);
  }

  @Override
  public Future<Void> cancelRequest() {
    return connection.cancelRequest();
  }

  @Override
  public int processId() {
    return connection.processId();
  }

  @Override
  public int secretKey() {
    return connection.secretKey();
  }

  @Override
  public PgConnection prepare(String s, Handler<AsyncResult<PreparedStatement>> handler) {
    return connection.prepare(s, handler);
  }

  @Override
  public Future<PreparedStatement> prepare(String s) {
    return connection.prepare(s);
  }

  @Override
  public SqlConnection prepare(String s, PrepareOptions prepareOptions,
                               Handler<AsyncResult<PreparedStatement>> handler) {
    return connection.prepare(s, prepareOptions, handler);
  }

  @Override
  public Future<PreparedStatement> prepare(String s, PrepareOptions prepareOptions) {
    return connection.prepare(s, prepareOptions);
  }

  @Override
  public PgConnection exceptionHandler(Handler<Throwable> handler) {
    return connection.exceptionHandler(handler);
  }

  @Override
  public void begin(Handler<AsyncResult<Transaction>> handler) {
    connection.begin(handler);
  }

  @Override
  public Future<Transaction> begin() {
    return connection.begin();
  }

  @Override
  public Transaction transaction() {
    return connection.transaction();
  }

  @Override
  public boolean isSSL() {
    return connection.isSSL();
  }

  @Override
  public Query<RowSet<Row>> query(String s) {
    return connection.query(s);
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
    return connection.preparedQuery(s);
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String s, PrepareOptions prepareOptions) {
    return connection.preparedQuery(s, prepareOptions);
  }

  @Override
  public DatabaseMetadata databaseMetadata() {
    return connection.databaseMetadata();
  }

  public boolean isAvailable() {
    return this.available;
  }

  public void setAvailable() {
    LOG.debug("Setting available: {} {}", this.tenantId, sessionId);
    this.available = true;
  }

  public void setUnavailable() {
    this.available = false;
  }

  public UUID getSessionId() {
    return sessionId;
  }

  public String getTenantId() {
    return tenantId;
  }

  public int getConnectionHash() {
    if (connection == null) {
      return -1;
    }
    return connection.hashCode();
  }

  public PgConnection getWrappedConnection() {
    return connection;
  }

  public long getTimestamp() {
    return this.timestamp;
  }
}
