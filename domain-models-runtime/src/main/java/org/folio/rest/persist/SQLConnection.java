package org.folio.rest.persist;

import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;

public class SQLConnection {

  final SqlConnection conn;
  final int executionTimeLimit;
  final long acquiringTime;
  final Transaction tx;

  public SQLConnection(SqlConnection conn, Transaction tx, int executionTimeLimit) {
    this.conn = conn;
    this.tx = tx;
    this.executionTimeLimit = executionTimeLimit;
    acquiringTime = System.currentTimeMillis();
  }
}
