package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import org.folio.rest.jaxrs.model.Job;
import org.folio.rest.persist.MongoCRUD;
import org.folio.rest.resource.interfaces.JobAPI;

/**
 * an example of a job - jobs saved into the jobs collection with module = MY_MODULE and
 * name = jobs_generic_demo in PENDING state - will be passed to this implementation by the
 * job runner
 */
public class ImportJobRunnerDemo implements JobAPI {

  @Override
  public int getPriority() {
    return 10;
  }

  @Override
  public String getModule() {
    return "MY_MODULE";
  }

  @Override
  public String[] getName() {
    return new String[]{"jobs_generic_demo"};
  }

  @Override
  public boolean getRunOffPeakOnly() {
    return false;
  }


  @Override
  public void process(Job job, Handler<AsyncResult<Job>> replyHandler) {
    System.out.print(MongoCRUD.entity2Json(job).encodePrettily());
    replyHandler.handle(io.vertx.core.Future.succeededFuture(job));
  }

  @Override
  public void init(Vertx vertx) {
  }

  @Override
  public boolean isResumable() {
    return false;
  }

}
