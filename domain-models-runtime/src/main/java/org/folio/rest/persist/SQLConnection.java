package org.folio.rest.persist;

import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Transaction;

public class SQLConnection {

  final PgConnection conn;
  final Transaction tx;
  private Long timeId;

  public SQLConnection(PgConnection conn, Transaction tx) {
    this.conn = conn;
    this.tx = tx;
  }

  public void setTimeId(Long timeId) {
    this.timeId = timeId;
  }

  public Long getTimeId() {
    return timeId;
  }
}
