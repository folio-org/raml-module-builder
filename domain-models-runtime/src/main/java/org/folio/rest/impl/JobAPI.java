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
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.Criteria.Order.ORDER;
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
  private static final String JOB_CONF_CLASS_NAME   = JobConf.class.getName();
  private static final String JOB_CLASS_NAME        = Job.class.getName();
  private final Messages messages = Messages.getInstance();

  @Validate
  @Override
  public void getJobsJobconfs(String query, String orderBy, Order order, int offset, int limit,
      String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    try {
      Criterion criterion = Criterion.json2Criterion(query);
      criterion.setLimit(new Limit(limit)).setOffset(new Offset(offset));
      org.folio.rest.persist.Criteria.Order or = getOrder(order, orderBy);
      if (or != null) {
        criterion.setOrder(or);
      }
      System.out.println("sending... getJobsJobconfs");
      vertxContext.runOnContext(v -> {

        try {
          PostgresClient.getInstance(vertxContext.owner()).get(RTFConsts.JOB_CONF_COLLECTION, JobConf.class,
            criterion, true, reply -> {
                JobsConfs ps = new JobsConfs();
                List<JobConf> jobConfs = (List<JobConf>) reply.result()[0];
                ps.setJobConfs(jobConfs);
                ps.setTotalRecords(jobConfs.size());
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsResponse.withJsonOK(ps)));
          });
        }
        catch (Exception e) {
          log.error(e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
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
        try {
          PostgresClient.getInstance(vertxContext.owner()).save(RTFConsts.JOB_CONF_COLLECTION, entity,
            reply -> {

              OutStream stream = new OutStream();
              stream.setData(entity);
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsResponse.withJsonCreated(
                reply.result(), stream)));
            });
        }
        catch (Exception e) {
          log.error(e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
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

    System.out.println("sending... getJobsJobconfsByJobconfsId");
    try {
      Criteria c = new Criteria();
      c.addField("_id");
      c.setOperation("=");
      c.setValue(jobconfsId);
      c.setJSONB(false);
      vertxContext.runOnContext(v -> {
        try {
          PostgresClient.getInstance(vertxContext.owner()).get(RTFConsts.JOB_CONF_COLLECTION, JobConf.class,
            new Criterion(c), true, reply -> {
              List<JobConf> confs = (List<JobConf>) reply.result()[0];
              if (confs.isEmpty()) {
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  GetJobsJobconfsByJobconfsIdResponse.withPlainNotFound("JobConf "
                    + messages.getMessage(lang, "10008"))));
                return;
              }
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                GetJobsJobconfsByJobconfsIdResponse.withJsonOK(confs.get(0))));
            });
        }
        catch (Exception e) {
          log.error(e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
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
        try{
        PostgresClient.getInstance(vertxContext.owner()).delete(
          RTFConsts.JOB_CONF_COLLECTION, jobconfsId,
          reply -> {
            if(reply.succeeded()){
              if(reply.result().getUpdated() == 1){
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  DeleteJobsJobconfsByJobconfsIdResponse.withNoContent()));
              }
              else{
                String message = messages.getMessage(lang, MessageConsts.DeletedCountError, 1,reply.result().getUpdated());
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
        }
        catch(Exception e){
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsByJobconfsIdResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
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
        try {
          PostgresClient.getInstance(vertxContext.owner()).update(
            RTFConsts.JOB_CONF_COLLECTION, entity, jobconfsId,
            reply -> {
              if (reply.succeeded() && reply.result().getUpdated() == 0) {
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
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutJobsJobconfsByJobconfsIdResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
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
      Criterion criterion = Criterion.json2Criterion(query);
      criterion.setLimit(new Limit(limit)).setOffset(new Offset(offset));
      org.folio.rest.persist.Criteria.Order or = getOrder(order, orderBy);
      if (or != null) {
        criterion.setOrder(or);
      }
      vertxContext.runOnContext(v -> {
        try {
          PostgresClient.getInstance(vertxContext.owner()).get(RTFConsts.JOBS_COLLECTION, Job.class, criterion,
            true, reply -> {
              try {
                List<Job> jobs = (List<Job>) reply.result()[0];
                Jobs jobList = new Jobs();
                jobList.setJobs(jobs);
                jobList.setTotalRecords(jobs.size());
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  GetJobsJobconfsByJobconfsIdJobsResponse.withJsonOK(jobList)));
              } catch (Exception e) {
                log.error(e.getMessage(), e);
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdJobsResponse
                  .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            });
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdJobsResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
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
        try {
          PostgresClient.getInstance(vertxContext.owner()).save(RTFConsts.JOBS_COLLECTION,
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
          });
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsByJobconfsIdJobsResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
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
        try {
          PostgresClient.getInstance(vertxContext.owner()).get(RTFConsts.JOBS_COLLECTION, query, true,
            reply -> {
              try {
                List<Job> job = (List<Job>) reply.result()[0];
                if (job.isEmpty()) {
                  asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                    GetJobsJobconfsByJobconfsIdJobsByJobIdResponse.withPlainNotFound("Job "
                      + messages.getMessage(lang, "10008"))));
                  return;
                }
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  GetJobsJobconfsByJobconfsIdJobsByJobIdResponse.withJsonOK(job.get(0))));
              } catch (Exception e) {
                log.error(e.getMessage(), e);
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdJobsByJobIdResponse
                  .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            });
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdJobsByJobIdResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
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
        try {
          PostgresClient.getInstance(vertxContext.owner()).delete(RTFConsts.JOBS_COLLECTION, query,
            reply -> {
              if(reply.succeeded()){
                if(reply.result().getUpdated() == 1){
                  asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                    DeleteJobsJobconfsByJobconfsIdJobsByJobIdResponse.withNoContent()));
                }
                else{
                  String message = messages.getMessage(lang, MessageConsts.DeletedCountError, 1,reply.result().getUpdated());
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
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsByJobconfsIdJobsByJobIdResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsByJobconfsIdJobsByJobIdResponse
        .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Validate
  @Override
  public void putJobsJobconfsByJobconfsIdJobsByJobId(String jobId, String jobconfsId, String lang,
      Job entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    System.out.println("sending... putJobsJobconfsByJobconfsId");

    try {
      Criteria c = new Criteria();
      c.addField("_id");
      c.setOperation("=");
      c.setValue(jobId);
      c.setJSONB(false);
      Criteria d = new Criteria();
      d.addField("job_conf_id");
      d.setOperation("=");
      d.setValue(jobconfsId);
      d.setJSONB(false);
      vertxContext.runOnContext(v -> {
        try {
          PostgresClient.getInstance(vertxContext.owner()).update(RTFConsts.JOBS_COLLECTION, entity,
            new Criterion().addCriterion(c, "AND", d),  true,
            reply -> {
              if (reply.succeeded() && reply.result().getUpdated() == 0) {
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
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutJobsJobconfsByJobconfsIdJobsByJobIdResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
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
        try {
          PostgresClient.getInstance(vertxContext.owner()).get(RTFConsts.BULKS_COLLECTION, query, true,
            reply -> {
              try {
                List<Bulk> bulks = (List<Bulk>) reply.result()[0];
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
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsByJobconfsIdJobsByJobIdBulksResponse
            .withPlainInternalServerError(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
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

  private org.folio.rest.persist.Criteria.Order getOrder(Order order, String field) {

    if (field == null) {
      return null;
    }

    String sortOrder = org.folio.rest.persist.Criteria.Order.ASC;
    if (order.name().equals("asc")) {
      sortOrder = org.folio.rest.persist.Criteria.Order.DESC;
    }

    return new org.folio.rest.persist.Criteria.Order(field, ORDER.valueOf(sortOrder.toUpperCase()));
  }
}
