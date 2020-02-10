package org.folio.rest.persist;

import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import org.folio.rest.jaxrs.model.ResultInfo;

/**
 * The result of successful completion of PostgresCLient.streamGet
 *
 * @param <T> each item returned in stream is of this type
 */
class PostgresClientStreamResult<T> implements ReadStream<T> {

  private final ResultInfo resultInfo;

  private Handler<T> streamHandler;
  private Handler<Void> endHandler;
  private Handler<Throwable> exceptionHandler;

  /**
   * Only to be constructed from PostgresClient itself
   *
   * @param resultInfo
   */
  PostgresClientStreamResult(ResultInfo resultInfo) {
    this.resultInfo = resultInfo;
  }

  /**
   * The ResultInfo for the streamGet.
   *
   * @return Result information.. including totalRecords and facets
   */
  public ResultInfo resultInto() {
    return this.resultInfo;
  }

  /**
   * Sets stream handler for each returned item of type T
   *
   * @param streamHandler
   * @return this instance (fluent)
   */
  @Override
  public PostgresClientStreamResult<T> handler(Handler<T> streamHandler) {
    this.streamHandler = streamHandler;
    return this;
  }

  /**
   * Sets handler to be called when stream is complete
   *
   * @param endHandler
   * @return this instance (fluent)
   */
  @Override
  public PostgresClientStreamResult<T> endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }

  /**
   * Sets handler to be called if an exception occurs Both exceptions in user
   * handlers and PostgresClient are caught. This handler will be called only once
   * and neither the item handler, nor the endHandler will be called afterwards.
   *
   * @param exceptionHandler
   * @return this instance (fluent)
   */
  @Override
  public PostgresClientStreamResult<T> exceptionHandler(Handler<Throwable> exceptionHandler) {
    this.exceptionHandler = exceptionHandler;
    return this;
  }

  /**
   * Only to be called by PostgresCLient itself
   *
   * @param t
   */
  void fireHandler(T t) {
    if (streamHandler != null) {
      streamHandler.handle(t);
    }
  }

  /**
   * Only to be called by PostgresClient itself
   */
  void fireEndHandler() {
    if (endHandler != null) {
      endHandler.handle(null);
    }
  }

  /**
   * Only to be called by PostgresClient itself
   *
   * @param cause
   */
  void fireExceptionHandler(Throwable cause) {
    if (exceptionHandler != null) {
      exceptionHandler.handle(cause);
    }
  }

  @Override
  public ReadStream<T> pause() {
    throw new UnsupportedOperationException("Not supported yet: pause");
  }

  @Override
  public ReadStream<T> resume() {
    throw new UnsupportedOperationException("Not supported yet: resume");
  }

  @Override
  public ReadStream<T> fetch(long amount) {
    throw new UnsupportedOperationException("Not supported yet: fetch");
  }
}
