package org.folio.rest.resource.interfaces;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * @author shale
 *
 */
public interface PostDeployVerticle {

  /** this implementation will be run immediately after the verticle is initially deployed. Failure does not stop
   * deployment success. The implementing function MUST call the resultHandler to pass back
   * control to the verticle, like so: resultHandler.handle(io.vertx.core.Future.succeededFuture(true));
   * if not, this function will hang the verticle during deployment */
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> resultHandler);

}
