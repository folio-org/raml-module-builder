package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import org.folio.rest.jaxrs.model.Job;
import org.folio.rest.persist.MongoCRUD;
import org.folio.rest.resource.interfaces.JobAPI;


public class ImportJobRunnerDemo implements JobAPI {

  @Override
  public int getPriority() {
    return 10;
  }

  @Override
  public String getModule() {
    return "IMPORTS";
  }

  @Override
  public String[] getName() {
    return new String[]{"uploads.generic.import"};
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
    // TODO Auto-generated method stub

  }

}
