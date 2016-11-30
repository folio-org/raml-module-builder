package org.folio.rest.resource.interfaces;

import io.vertx.core.Context;
import io.vertx.core.Vertx;

/**
 *
 * Hook allowing code to run periodically by the framework. All plugins implementing this interface will
 * be loaded at verticle startup and scheduled at that point.
 *
 */
public interface PeriodicAPI {
  /** this implementation should return the delay in which to run the function */
  public long runEvery();
  /** this is the implementation that will be run every runEvery() milliseconds*/
  public void run(Vertx vertx, Context context);

}
