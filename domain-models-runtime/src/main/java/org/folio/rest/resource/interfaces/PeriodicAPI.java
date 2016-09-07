package org.folio.rest.resource.interfaces;

import io.vertx.core.Context;
import io.vertx.core.Vertx;

public interface PeriodicAPI {
  /** this implementation should return the delay in which to run the function */
  public long runEvery();
  /** this is the implementation that will be run every runEvery() milliseconds*/
  public void run(Vertx vertx, Context context);

}
