/**
 * PeriodicAPI
 * 
 * Jul 5, 2016
 *
 * Apache License Version 2.0
 */
package com.sling.rest.resource.interfaces;

import io.vertx.core.Context;
import io.vertx.core.Vertx;

/**
 * @author shale
 *
 */
public interface PeriodicAPI {
  /** this implementation should return the delay in which to run the function */
  public long runEvery();
  /** this is the implementation that will be run every runEvery() milliseconds*/
  public void run(Vertx vertx, Context context);

}
