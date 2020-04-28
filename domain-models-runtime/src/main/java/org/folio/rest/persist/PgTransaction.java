package org.folio.rest.persist;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;

public class PgTransaction {

  final SqlConnection conn;
  final Transaction tx;
  public PgTransaction(SqlConnection conn, Transaction tx) {
    this.conn = conn;
    this.tx = tx;
  }

  public AsyncResult<SqlConnection> connection() {
    return Future.succeededFuture(conn);
  }
}
