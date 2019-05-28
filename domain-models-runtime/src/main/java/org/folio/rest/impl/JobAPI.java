package org.folio.rest.impl;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.Bulk;
import org.folio.rest.jaxrs.model.Bulks;
import org.folio.rest.jaxrs.model.Job;
import org.folio.rest.jaxrs.model.JobConf;
import org.folio.rest.jaxrs.model.Jobs;
import org.folio.rest.jaxrs.model.JobsConf;
import org.folio.rest.jaxrs.model.JobsConfs;
import org.folio.rest.jaxrs.model.JobsJobconfsGetOrder;
import org.folio.rest.jaxrs.model.JobsJobconfsJobconfsIdJobsGetOrder;
import org.folio.rest.persist.PgUtil;
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

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * API to add job configurations and job instances associated with the job configurations
 * Jobs are saved into the folio_shared schema declared in the MongoCRUD with each record containing an institution id
 *
 */
public class JobAPI implements org.folio.rest.jaxrs.resource.Jobs {

  private static final Logger log = LoggerFactory.getLogger(JobAPI.class);
  private final Messages messages = Messages.getInstance();

  @Validate
  @Override
  public void getJobsJobconfs(String query, String orderBy, JobsJobconfsGetOrder order, int offset, int limit,
      String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

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
                @SuppressWarnings("unchecked")
                List<JobConf> jobConfs = (List<JobConf>) reply.result().getResults();
                ps.setJobConfs(jobConfs);
                ps.setTotalRecords(jobConfs.size());
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsResponse.
                  respond200WithApplicationJson(ps)));
          });
        }
        catch (Exception e) {
          log.error(e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsResponse
            .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsResponse
        .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }

  @Validate
  @Override
  public void postJobsJobconfs(String lang, JobsConf entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    System.out.println("sending... postJobsJobconfs");
    try {
      vertxContext.runOnContext(v -> {
        try {
          PostgresClient.getInstance(vertxContext.owner()).save(RTFConsts.JOB_CONF_COLLECTION, entity,
            reply -> {

              OutStream stream = new OutStream();
              stream.setData(entity);
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsResponse.respond201WithApplicationJson(
                stream , PostJobsJobconfsResponse.headersFor201().withLocation(reply.result()))));
            });
        }
        catch (Exception e) {
          log.error(e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsResponse
            .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsResponse
        .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }

  @Validate
  @Override
  public void getJobsJobconfsByJobconfsId(String jobconfsId, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    log.info("sending... getJobsJobconfsByJobconfsId");
    PgUtil.getById(RTFConsts.JOB_CONF_COLLECTION, JobsConf.class, jobconfsId, okapiHeaders, vertxContext,
        GetJobsJobconfsByJobconfsIdResponse.class, asyncResultHandler);
  }

  @Validate
  @Override
  public void deleteJobsJobconfsByJobconfsId(String jobconfsId, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

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
                  DeleteJobsJobconfsByJobconfsIdResponse.respond204()));
              }
              else{
                String message = messages.getMessage(lang, MessageConsts.DeletedCountError, 1,reply.result().getUpdated());
                log.error(message);
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsByJobconfsIdResponse
                  .respond400WithTextPlain(message)));
              }
            }
            else{
              log.error(reply.cause());
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsByJobconfsIdResponse
                .respond500WithTextPlain(messages.getMessage(lang,  MessageConsts.InternalServerError))));
            }
          });
        }
        catch(Exception e){
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsByJobconfsIdResponse
            .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsByJobconfsIdResponse
        .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }

  @Validate
  @Override
  public void putJobsJobconfsByJobconfsId(String jobconfsId, String lang, JobsConf entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

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
                  PutJobsJobconfsByJobconfsIdResponse.respond400WithTextPlain(jobconfsId)));
              } else {
                try {
                  asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                    PutJobsJobconfsByJobconfsIdResponse.respond204()));
                } catch (Exception e) {
                  log.error(e);
                  asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutJobsJobconfsByJobconfsIdResponse
                    .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
                }
              }
            });
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutJobsJobconfsByJobconfsIdResponse
            .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutJobsJobconfsByJobconfsIdResponse
        .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }


  @Validate
  @Override
  public void getJobsJobconfsJobsByJobconfsId(String jobconfsId, String query, String orderBy,
      JobsJobconfsJobconfsIdJobsGetOrder order, int offset, int limit, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

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
                @SuppressWarnings("unchecked")
                List<Job> jobs = (List<Job>) reply.result().getResults();
                Jobs jobList = new Jobs();
                jobList.setJobs(jobs);
                jobList.setTotalRecords(jobs.size());
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  GetJobsJobconfsJobsByJobconfsIdResponse.respond200WithApplicationJson(jobList)));
              } catch (Exception e) {
                log.error(e.getMessage(), e);
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsJobsByJobconfsIdResponse
                  .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            });
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsJobsByJobconfsIdResponse
            .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsJobsByJobconfsIdResponse
        .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }

  @Validate
  @Override
  public void postJobsJobconfsJobsByJobconfsId(String jobconfsId, String lang, org.folio.rest.jaxrs.model.Job entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

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
                  PostJobsJobconfsJobsByJobconfsIdResponse.
                  respond201WithApplicationJson(stream,
                    PostJobsJobconfsJobsByJobconfsIdResponse.headersFor201().withLocation(reply.result()))));
              } catch (Exception e) {
                log.error(e);
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsJobsByJobconfsIdResponse
                  .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
          });
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsJobsByJobconfsIdResponse
            .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostJobsJobconfsJobsByJobconfsIdResponse
        .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Validate
  @Override
  public void getJobsJobconfsJobsByJobconfsIdAndJobId(String jobId, String jobconfsId, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

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
                @SuppressWarnings("unchecked")
                List<Job> job = (List<Job>) reply.result().getResults();
                if (job.isEmpty()) {
                  asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                    GetJobsJobconfsJobsByJobconfsIdAndJobIdResponse.respond404WithTextPlain("Job "
                      + messages.getMessage(lang, "10008"))));
                  return;
                }
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  GetJobsJobconfsJobsByJobconfsIdAndJobIdResponse.respond200WithApplicationJson(job.get(0))));
              } catch (Exception e) {
                log.error(e.getMessage(), e);
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsJobsByJobconfsIdAndJobIdResponse
                  .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            });
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsJobsByJobconfsIdAndJobIdResponse
            .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsJobsByJobconfsIdAndJobIdResponse
        .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }


  @Validate
  @Override
  public void deleteJobsJobconfsJobsByJobconfsIdAndJobId(String jobId, String jobconfsId,
      String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

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
                    DeleteJobsJobconfsJobsByJobconfsIdAndJobIdResponse.respond204()));
                }
                else{
                  String message = messages.getMessage(lang, MessageConsts.DeletedCountError, 1,reply.result().getUpdated());
                  log.error(message);
                  asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsJobsByJobconfsIdAndJobIdResponse
                    .respond404WithTextPlain(message)));
                }
              }
              else{
                log.error(reply.cause());
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsJobsByJobconfsIdAndJobIdResponse
                  .respond500WithTextPlain(messages.getMessage(lang,  MessageConsts.InternalServerError))));
              }
            });
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsJobsByJobconfsIdAndJobIdResponse
            .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(DeleteJobsJobconfsJobsByJobconfsIdAndJobIdResponse
        .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Validate
  @Override
  public void putJobsJobconfsJobsByJobconfsIdAndJobId(String jobId, String jobconfsId, String lang,
      org.folio.rest.jaxrs.model.Job entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    System.out.println("sending... putJobsJobconfsByJobconfsId");

    try {
      Criteria c = new Criteria();
      c.addField("id");
      c.setOperation("=");
      c.setVal(jobId);
      c.setJSONB(false);
      Criteria d = new Criteria();
      d.addField("'job_conf_id'");
      d.setOperation("=");
      d.setVal(jobconfsId);
      Criterion criterion = new Criterion().addCriterion(c, "AND", d);
      vertxContext.runOnContext(v -> {
        try {
          PostgresClient.getInstance(vertxContext.owner()).update(RTFConsts.JOBS_COLLECTION, entity,
            criterion, true,
            reply -> {
              if (reply.succeeded() && reply.result().getUpdated() == 0) {
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  PutJobsJobconfsJobsByJobconfsIdAndJobIdResponse.respond404WithTextPlain(jobId)));
              } else {
                try {
                  asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                    PutJobsJobconfsJobsByJobconfsIdAndJobIdResponse.respond204()));
                } catch (Exception e) {
                  log.error(e);
                  asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutJobsJobconfsJobsByJobconfsIdAndJobIdResponse
                    .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
                }
              }
            });
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutJobsJobconfsJobsByJobconfsIdAndJobIdResponse
            .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PutJobsJobconfsJobsByJobconfsIdAndJobIdResponse
        .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }


  @Override
  public void getJobsJobconfsJobsBulksByJobconfsIdAndJobId(String jobId, String jobconfsId,
      String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    Bulk query = new Bulk();
    query.setJobId(jobId);

    System.out.println("sending... getJobsJobconfsByJobconfsIdJobsByJobIdBulks");
    try {
      vertxContext.runOnContext(v -> {
        try {
          PostgresClient.getInstance(vertxContext.owner()).get(RTFConsts.BULKS_COLLECTION, query, true,
            reply -> {
              try {
                @SuppressWarnings("unchecked")
                List<Bulk> bulks = (List<Bulk>) reply.result().getResults();
                Bulks bulkList = new Bulks();
                bulkList.setBulks(bulks);
                bulkList.setTotalRecords(bulks.size());
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                  GetJobsJobconfsJobsBulksByJobconfsIdAndJobIdResponse.respond200WithApplicationJson(bulkList)));
              } catch (Exception e) {
                log.error(e);
                asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsJobsBulksByJobconfsIdAndJobIdResponse
                  .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            });
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsJobsBulksByJobconfsIdAndJobIdResponse
            .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetJobsJobconfsJobsBulksByJobconfsIdAndJobIdResponse
        .respond500WithTextPlain(messages.getMessage(lang, MessageConsts.InternalServerError))));
    }

  }

  @Override
  public void postJobsJobconfsJobsBulksByJobconfsIdAndJobId(String jobId, String jobconfsId,
      String lang, Bulk entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
      PostJobsJobconfsJobsBulksByJobconfsIdAndJobIdResponse.respond400WithTextPlain("Not implemented yet")));

  }

  private org.folio.rest.persist.Criteria.Order getOrder(Enum order, String field) {

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
