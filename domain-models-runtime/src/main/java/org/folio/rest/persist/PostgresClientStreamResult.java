package org.folio.rest.persist;

import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import org.folio.rest.jaxrs.model.ResultInfo;

/**
 * The result of successful completion of PostgresCLient.streamGet
 *
 * @param <T> each item returned in stream is of this type
 */
public class PostgresClientStreamResult<T> implements ReadStream<T> {

  private final ResultInfo resultInfo;

  private Handler<T> streamHandler;
  private Handler<Void> endHandler;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> closeHandler;
  private boolean failed = false; // to ensure exceptionHandler being called at most once

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
  public ResultInfo resultInfo() {
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
   * Set a handler that is called before the endHandler or the exceptionHandler is to be fired.
   * The handler is called even if endHandler or exceptionHandler is null.
   */
  PostgresClientStreamResult<T> setCloseHandler(Handler<Void> closeHandler) {
    this.closeHandler = closeHandler;
    return this;
  }

  /**
   * Only to be called by PostgresClient itself
   *
   * @param t
   */
  void fireHandler(T t) {
    if (!failed && streamHandler != null) {
      streamHandler.handle(t);
    }
  }

  /**
   * Only to be called by PostgresClient itself
   */
  void fireEndHandler() {
    if (closeHandler != null) {
      closeHandler.handle(null);
    }
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
    if (failed) {
      return;
    }
    failed = true;
    if (closeHandler != null) {
      try {
        closeHandler.handle(null);
      } catch (Exception e) {
        // ignore yet another exception, connection may have been closed before
      }
    }
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
