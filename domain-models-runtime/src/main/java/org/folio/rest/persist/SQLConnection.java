package org.folio.rest.persist;

import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Transaction;

public class SQLConnection {

  final PgConnection conn;
  final int executionTimeLimit;
  final long acquiringTime;
  final Transaction tx;

  public SQLConnection(PgConnection conn, Transaction tx) {
   this(conn,tx,0);
  }

  public SQLConnection(PgConnection conn, Transaction tx, int executionTimeLimit) {
    this.conn = conn;
    this.tx = tx;
    this.executionTimeLimit = executionTimeLimit;
    acquiringTime = System.currentTimeMillis();
  }
}
