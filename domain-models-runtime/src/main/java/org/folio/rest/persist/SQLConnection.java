package org.folio.rest.persist;

import io.vertx.core.Vertx;
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

  /**
   * Close the connection and cancel the timer.
   * @param vertx The {@link Vertx} that started the timer, ignored if timerId is null.
   */
  public void close(Vertx vertx) {
    RuntimeException timerException = null;
    if (timerId != null) {
      try {
        vertx.cancelTimer(timerId);
      } catch (RuntimeException e) {
        timerException = e;
      }
    }
    if (conn != null) {
      conn.close();
    }
    if (timerException != null) {
      // first close the connection, then throw the exception
      throw timerException;
    }
  }
}
