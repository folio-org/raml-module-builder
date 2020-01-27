package org.folio.rest.persist;

import io.vertx.core.Handler;
import org.folio.rest.jaxrs.model.ResultInfo;

/**
 * Upon successful completion of PostgresCLient.streamGet, the result is this
 * class.
 *
 * @param <T> each item returned in stream is of this type
 */
class PostgresClientStreamResult<T> {

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
  public PostgresClientStreamResult<T> endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }

  /**
   * Sets handler to be called if an exception occurs Both exceptions in user
   * handlers and PostgresClient are caught.
   *
   * @param exceptionHandler
   * @return this instance (fluent)
   */
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
    try {
      if (streamHandler != null) {
        streamHandler.handle(t);
      }
    } catch (Exception ex) {
      fireExceptionHandler(ex);
    }
  }

  /**
   * Only to be called by PostgresClient itself
   */
  void fireEndHandler() {
    try {
      if (endHandler != null) {
        endHandler.handle(null);
      }
    } catch (Exception ex) {
      fireExceptionHandler(ex);
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
}
