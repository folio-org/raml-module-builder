/**
 * SuhtdownAPI
 * 
 * Jul 12, 2016
 *
 * Apache License Version 2.0
 */
package com.sling.rest.resource.interfaces;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * @author shale
 *
 */
public interface ShutdownAPI {

  public void shutdown(Vertx vertx, Context context, Handler<AsyncResult<Void>> handler);
  
}
