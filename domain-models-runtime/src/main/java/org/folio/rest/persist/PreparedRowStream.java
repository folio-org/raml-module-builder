package org.folio.rest.persist;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.Tuple;

/**
 * Extend a RowStream<Row> by adding a {@link PreparedStatement#close} call
 * to any {@link RowStream#close} call. This will release the resources allocated
 * by the prepared statement.
 */
public class PreparedRowStream implements RowStream<Row> {
  private final PreparedStatement preparedStatement;
  private final RowStream<Row> rowStream;

  /**
   * Extend rowStream by adding a preparedStatement.close call
   * to any {@link PreparedRowStream#close} call. This will release the resources
   * allocated by the prepared statement.
   */
  public PreparedRowStream(PreparedStatement preparedStatement, RowStream<Row> rowStream) {
    this.preparedStatement = preparedStatement;
    this.rowStream = rowStream;
  }

  /**
   * Create a RowStream<Row> using the preparedStatement, fetch and tuple.
   * A call to PreparedRowStream#close will close the preparedStatement to
   * release the resources that it allocates.
   * @param preparedStatement the query to execute
   * @param fetch the cursor fetch size
   * @param tuple the arguments for preparedStatement
   */
  public PreparedRowStream(PreparedStatement preparedStatement, int fetch, Tuple tuple) {
    this(preparedStatement, preparedStatement.createStream(fetch, tuple));
  }

  @Override
  public ReadStream<Row> fetch(long amount) {
    return rowStream.fetch(amount);
  }

  @Override
  public RowStream<Row> exceptionHandler(Handler<Throwable> handler) {
    rowStream.exceptionHandler(handler);
    return this;
  }

  @Override
  public RowStream<Row> handler(Handler<Row> handler) {
    rowStream.handler(handler);
    return this;
  }

  @Override
  public RowStream<Row> pause() {
    rowStream.pause();
    return this;
  }

  @Override
  public RowStream<Row> resume() {
    rowStream.resume();
    return this;
  }

  @Override
  public RowStream<Row> endHandler(Handler<Void> endHandler) {
    rowStream.endHandler(endHandler);
    return this;
  }

  @Override
  public void close() {
    preparedStatement.close(close -> {
      rowStream.close();
    });
  }

  @Override
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    preparedStatement.close(close1 -> {
      rowStream.close(close2 -> {
        if (close1.failed()) {
          completionHandler.handle(close1);
          return;
        }
        completionHandler.handle(close2);
      });
    });
  }
}
