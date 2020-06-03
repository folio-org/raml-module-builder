package org.folio.rest.persist;

import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Transaction;

public class SQLConnection {

  final PgConnection conn;
  final Transaction tx;
  final Long timerId;

  public SQLConnection(PgConnection conn, Transaction tx, Long timerId) {
    this.conn = conn;
    this.tx = tx;
    this.timerId = timerId;
  }
}
