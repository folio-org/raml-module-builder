package org.folio.rest.resource.interfaces;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 *
 *Interface that can be implemented by a module to have code run before the verticle is deployed
 *the init() function will be called during verticle startup.
 *any custom startup code can be implemented here - for example - db init stuff, cache stuff, static
 *stuff, etc...should be ok to even run execBlocking in here and in the handler call back the
 *passed in resultHandler
 */
public interface InitAPI {
  /** this implementation will be run before the verticle is initially deployed. An error will most
   * likely fail deployment. The implementing function MUST call the resultHandler to pass back
   * control to the verticle, like so: resultHandler.handle(io.vertx.core.Future.succeededFuture(true));
   * if not, this function will hang the verticle during deployment */
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> resultHandler);

}
