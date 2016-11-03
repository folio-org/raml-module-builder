package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.Bulk;
import org.folio.rest.jaxrs.model.Bulks;
import org.folio.rest.jaxrs.model.Job;
import org.folio.rest.jaxrs.model.JobConf;
import org.folio.rest.jaxrs.model.Jobs;
import org.folio.rest.jaxrs.model.JobsConfs;
import org.folio.rest.jaxrs.resource.JobsResource;
import org.folio.rest.persist.MongoCRUD;
import org.folio.rest.tools.RTFConsts;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.OutStream;

/**
 * API to add job configurations and job instances associated with the job configurations
 * Jobs are saved into the folio_shared schema declared in the MongoCRUD with each record containing an institution id
 *
 */
public class JobAPI implements JobsResource {


  private static final Logger log = LoggerFactory.getLogger(JobAPI.class);
  private final Messages messages = Messages.getInstance();

  private static final String JOB_CONF_CLASS_NAME   = JobConf.class.getName();
  private static final String JOB_CLASS_NAME        = Job.class.getName();

  @Validate
  @Override
  public void getJobsJobconfs(String query, String orderBy, Order order, int offset, int limit,
      String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    try {
      System.out.println("sending... getJobsJobconfs");
      vertxContext.runOnContext(v -> {
        MongoCRUD.getInstance(vertxContext.owner()).get(
          MongoCRUD.buildJson(JOB_CONF_CLASS_NAME, RTFConsts.JOB_CONF_COLLECTION, query, orderBy, order,
            offset, limit), reply -> {
            try {
              JobsConfs ps = new JobsConfs();
              List<JobConf> jobConfs = (List<JobConf>) reply.result();
              ps.setJobConfs(jobConfs);
              ps.setTotalRecords(jobConfs.size());
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsResponse.withJsonOK(ps)));
        } catch (Exception e) {
          log.error(e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      } );
      });
    } catch (Exception e) {
      log.error(e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsResponse
        .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }

  @Validate
  @Override
  public void postJobsJobconfs(String lang, JobConf entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    System.out.println("sending... postJobsJobconfs");
    try {
      vertxContext.runOnContext(v -> {
        MongoCRUD.getInstance(vertxContext.owner()).save(RTFConsts.JOB_CONF_COLLECTION,
          entity,
          reply -> {
            try {
              OutStream stream = new OutStream();
              stream.setData(entity);
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsResponse.withJsonCreated(
                reply.result(), stream)));
        } catch (Exception e) {
          log.error(e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      } );
      });
    } catch (Exception e) {
      log.error(e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsResponse
        .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }

  @Validate
  @Override
  public void getJobsJobconfsByJobconfsId(String jobconfsId, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) throws Exception {

    JobConf query = new JobConf();
    query.setId(jobconfsId);

    System.out.println("sending... getJobsJobconfsByJobconfsId");
    try {
      vertxContext.runOnContext(v -> {
        MongoCRUD.getInstance(vertxContext.owner()).get(RTFConsts.JOB_CONF_COLLECTION, query,
          reply -> {
            try {
              List<JobConf> confs = (List<JobConf>) reply.result();
              if (confs.isEmpty()) {
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  GetJobsJobconfsByJobconfsIdResponse.withPlainNotFound("JobConf "
                    + messages.getMessage(lang, "10008"))));
                return;
              }
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                GetJobsJobconfsByJobconfsIdResponse.withJsonOK(confs.get(0))));
            } catch (Exception e) {
              log.error(e);
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdResponse
                .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
            }
          });
      });
    } catch (Exception e) {
      log.error(e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdResponse
        .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }

  @Validate
  @Override
  public void deleteJobsJobconfsByJobconfsId(String jobconfsId, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) throws Exception {

    System.out.println("sending... deleteJobsJobconfsByJobconfsId");
    try {
      vertxContext.runOnContext(v -> {
        MongoCRUD.getInstance(vertxContext.owner()).delete(
          RTFConsts.JOB_CONF_COLLECTION, jobconfsId,
          reply -> {
            if(reply.succeeded()){
              if(reply.result().getRemovedCount() == 1){
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  DeleteJobsJobconfsByJobconfsIdResponse.withNoContent()));
              }
              else{
                String message = messages.getMessage(lang, MessageConsts.DeletedCountError, 1,reply.result().getRemovedCount());
                log.error(message);
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsByJobconfsIdResponse
                  .withPlainNotFound(message)));
              }
            }
            else{
              log.error(reply.cause());
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsByJobconfsIdResponse
                .withPlainInternalServerError(messages.getMessage(lang,  MessageConsts.InternalServerError))));
            }
          });
      });
    } catch (Exception e) {
      log.error(e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsByJobconfsIdResponse
        .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }

  @Validate
  @Override
  public void putJobsJobconfsByJobconfsId(String jobconfsId, String lang, JobConf entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) throws Exception {

    JobConf query = new JobConf();
    query.setId(jobconfsId);

    System.out.println("sending... putJobsJobconfsByJobconfsId");

    try {
      vertxContext.runOnContext(v -> {
        MongoCRUD.getInstance(vertxContext.owner()).update(
          RTFConsts.JOB_CONF_COLLECTION, entity, query, false ,true,
          reply -> {
            if (reply.succeeded() && reply.result().getDocMatched() == 0) {
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                PutJobsJobconfsByJobconfsIdResponse.withPlainNotFound(jobconfsId)));
            } else {
              try {
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  PutJobsJobconfsByJobconfsIdResponse.withNoContent()));
              } catch (Exception e) {
                log.error(e);
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutJobsJobconfsByJobconfsIdResponse
                  .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            }
          });
      });
    } catch (Exception e) {
      log.error(e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutJobsJobconfsByJobconfsIdResponse
        .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }


  @Validate
  @Override
  public void getJobsJobconfsByJobconfsIdJobs(String jobconfsId, String query, String orderBy,
      Order order, int offset, int limit, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    System.out.println("sending... getJobsJobconfsByJobconfsIdJobs");
    try {
      vertxContext.runOnContext(v -> {
        MongoCRUD.getInstance(vertxContext.owner()).get(
          MongoCRUD.buildJson(JOB_CLASS_NAME, RTFConsts.JOBS_COLLECTION, query, orderBy, order,
          offset, limit), reply -> {
            try {
              List<Job> jobs = (List<Job>) reply.result();
              Jobs jobList = new Jobs();
              jobList.setJobs(jobs);
              jobList.setTotalRecords(jobs.size());
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                GetJobsJobconfsByJobconfsIdJobsResponse.withJsonOK(jobList)));
            } catch (Exception e) {
              log.error(e);
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdJobsResponse
                .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
            }
          });
      });
    } catch (Exception e) {
      log.error(e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdJobsResponse
        .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }

  @Validate
  @Override
  public void postJobsJobconfsByJobconfsIdJobs(String jobconfsId, String lang, Job entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) throws Exception {

    System.out.println("sending... postJobsJobconfsByJobconfsIdJobs");

    entity.setJobConfId(jobconfsId);
    try {
      vertxContext.runOnContext(v -> {
        MongoCRUD.getInstance(vertxContext.owner()).save(RTFConsts.JOBS_COLLECTION,
          entity,
          reply -> {
            try {
              OutStream stream = new OutStream();
              stream.setData(entity);
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                PostJobsJobconfsByJobconfsIdJobsResponse.withJsonCreated(reply.result(), stream)));
        } catch (Exception e) {
          log.error(e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsByJobconfsIdJobsResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      } );
      });
    } catch (Exception e) {
      log.error(e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsByJobconfsIdJobsResponse
        .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Validate
  @Override
  public void getJobsJobconfsByJobconfsIdJobsByJobId(String jobId, String jobconfsId, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) throws Exception {

    Job query = new Job();
    query.setId(jobId);
    query.setJobConfId(jobconfsId);

    System.out.println("sending... getJobsJobconfsByJobconfsIdJobsByJobId");
    try {
      vertxContext.runOnContext(v -> {
        MongoCRUD.getInstance(vertxContext.owner()).get(RTFConsts.JOBS_COLLECTION, query,
          reply -> {
            try {
              List<Job> job = (List<Job>) reply.result();
              if (job.isEmpty()) {
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  GetJobsJobconfsByJobconfsIdJobsByJobIdResponse.withPlainNotFound("Job "
                    + messages.getMessage(lang, "10008"))));
                return;
              }
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                GetJobsJobconfsByJobconfsIdJobsByJobIdResponse.withJsonOK(job.get(0))));
            } catch (Exception e) {
              log.error(e);
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdJobsByJobIdResponse
                .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
            }
          });
      });
    } catch (Exception e) {
      log.error(e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdJobsByJobIdResponse
        .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }


  @Validate
  @Override
  public void deleteJobsJobconfsByJobconfsIdJobsByJobId(String jobId, String jobconfsId,
      String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    Job query = new Job();
    query.setId(jobId);
    query.setJobConfId(jobconfsId);

    System.out.println("sending... deleteJobsJobconfsByJobconfsIdJobsByJobId");
    try {
      vertxContext.runOnContext(v -> {
        MongoCRUD.getInstance(vertxContext.owner()).delete(
          RTFConsts.JOBS_COLLECTION, query,
          reply -> {
            if(reply.succeeded()){
              if(reply.result().getRemovedCount() == 1){
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  DeleteJobsJobconfsByJobconfsIdJobsByJobIdResponse.withNoContent()));
              }
              else{
                String message = messages.getMessage(lang, MessageConsts.DeletedCountError, 1,reply.result().getRemovedCount());
                log.error(message);
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsByJobconfsIdJobsByJobIdResponse
                  .withPlainNotFound(message)));
              }
            }
            else{
              log.error(reply.cause());
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsByJobconfsIdJobsByJobIdResponse
                .withPlainInternalServerError(messages.getMessage(lang,  MessageConsts.InternalServerError))));
            }
          });
      });
    } catch (Exception e) {
      log.error(e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsByJobconfsIdJobsByJobIdResponse
        .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Validate
  @Override
  public void putJobsJobconfsByJobconfsIdJobsByJobId(String jobId, String jobconfsId, String lang,
      Job entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    Job query = new Job();
    query.setId(jobId);
    query.setJobConfId(jobconfsId);

    System.out.println("sending... putJobsJobconfsByJobconfsId");

    try {
      vertxContext.runOnContext(v -> {
        MongoCRUD.getInstance(vertxContext.owner()).update(
          RTFConsts.JOBS_COLLECTION, entity, query, false, true,
          reply -> {
            if (reply.succeeded() && reply.result().getDocMatched() == 0) {
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                PutJobsJobconfsByJobconfsIdJobsByJobIdResponse.withPlainNotFound(jobId)));
            } else {
              try {
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  PutJobsJobconfsByJobconfsIdJobsByJobIdResponse.withNoContent()));
              } catch (Exception e) {
                log.error(e);
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutJobsJobconfsByJobconfsIdJobsByJobIdResponse
                  .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            }
          });
      });
    } catch (Exception e) {
      log.error(e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutJobsJobconfsByJobconfsIdJobsByJobIdResponse
        .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }


  @Override
  public void getJobsJobconfsByJobconfsIdJobsByJobIdBulks(String jobId, String jobconfsId,
      String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    Bulk query = new Bulk();
    query.setJobId(jobId);

    System.out.println("sending... getJobsJobconfsByJobconfsIdJobsByJobIdBulks");
    try {
      vertxContext.runOnContext(v -> {
        MongoCRUD.getInstance(vertxContext.owner()).get(RTFConsts.BULKS_COLLECTION, query,
          reply -> {
            try {
              List<Bulk> bulks = (List<Bulk>) reply.result();
              Bulks bulkList = new Bulks();
              bulkList.setBulks(bulks);
              bulkList.setTotalRecords(bulks.size());
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                GetJobsJobconfsByJobconfsIdJobsByJobIdBulksResponse.withJsonOK(bulkList)));
            } catch (Exception e) {
              log.error(e);
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdJobsByJobIdBulksResponse
                .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
            }
          });
      });
    } catch (Exception e) {
      log.error(e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdJobsByJobIdBulksResponse
        .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }

  @Override
  public void postJobsJobconfsByJobconfsIdJobsByJobIdBulks(String jobId, String jobconfsId,
      String lang, Bulk entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
      PostJobsJobconfsByJobconfsIdJobsByJobIdBulksResponse.withPlainBadRequest("Not implemented yet")));

  }

}
