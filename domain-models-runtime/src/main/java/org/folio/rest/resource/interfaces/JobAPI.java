package org.folio.rest.resource.interfaces;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import org.folio.rest.jaxrs.model.Job;

/**
 *
 */
public interface JobAPI {

  /**
   *
   * @return name of module associated with this job
   */
  public String getModule();

  /**
   *
   * @return name of job / jobs that this job implementation can process
   */
  public String[] getName();

  /**
   * function passed a job object. this function should run the business logic
   * @param job
   */
  public void process(Job job, Handler<AsyncResult<Job>> replyHandler);

  /**
   *
   * @return priority of this job 1-100 (1 being lowest)
   */
  public int getPriority();

  /**
   * run job only in off peak times
   * @return
   */
  public boolean getRunOffPeakOnly();

  /**
   * this function will receive the vertx instance from the job runner
   * so that access to things like the event bus can be handled by a job
   * @param vertx
   */
  public void init(Vertx vertx);

  /**
   * Does this type of job support resume
   */
  public boolean isResumable();

}
