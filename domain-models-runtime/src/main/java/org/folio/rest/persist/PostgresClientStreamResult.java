package org.folio.rest.persist;

import io.vertx.core.Handler;
import org.folio.rest.jaxrs.model.ResultInfo;

class PostgresClientStreamResult<T> {

  private final ResultInfo resultInfo;

  private Handler<T> streamHandler;
  private Handler<Void> endHandler;
  private Handler<Throwable> exceptionHandler;

  public PostgresClientStreamResult(ResultInfo resultInfo) {
    this.resultInfo = resultInfo;
  }

  ResultInfo resultInto() {
    return this.resultInfo;
  }

  public PostgresClientStreamResult<T> handler(Handler<T> streamHandler) {
    this.streamHandler = streamHandler;
    return this;
  }

  public PostgresClientStreamResult<T> endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }

  public PostgresClientStreamResult<T> exceptionHandler(Handler<Throwable> exceptionHandler) {
    this.exceptionHandler = exceptionHandler;
    return this;
  }

  void fireHandler(T t) {
    try {
      if (streamHandler != null) {
        streamHandler.handle(t);
      }
    } catch (Exception ex) {
      fireExceptionHandler(ex);
    }
  }

  void fireEndHandler() {
    try {
      if (endHandler != null) {
        endHandler.handle(null);
      }
    } catch (Exception ex) {
      fireExceptionHandler(ex);
    }
  }

  void fireExceptionHandler(Throwable cause) {
    if (exceptionHandler != null) {
      exceptionHandler.handle(cause);
    }
  }
}
