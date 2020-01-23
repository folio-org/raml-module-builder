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
  
  public void handler(Handler<T> streamHandler) {
    this.streamHandler = streamHandler;
  }
  
  public void endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
  }

  public void exceptionHandler(Handler<Throwable> exceptionHandler) {
    this.exceptionHandler = exceptionHandler;
  }

  void fireHandler(T t) {
    if (streamHandler != null) {
      streamHandler.handle(t);
    }
  }

  void fireEndHandler() {
    if (endHandler != null) {
      endHandler.handle(null);
    }
  }

  void fireExceptionHandler(Throwable cause) {
    if (exceptionHandler != null) {
      exceptionHandler.handle(cause);
    }
  }
}
