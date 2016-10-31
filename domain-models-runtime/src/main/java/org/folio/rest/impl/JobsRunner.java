package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.folio.rest.jaxrs.model.Job;
import org.folio.rest.persist.MongoCRUD;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.rest.resource.interfaces.JobAPI;
import org.folio.rest.tools.RTFConsts;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.InterfaceToImpl;

/**
 * This class implements the InitAPI and therefore is run during verticle deployments.
 * <br>loads JobAPI implementations from the class path
 * <br>queries the job collection for pending jobs
 * <br>looks for an implementation associated with the job's module + job name
 * <br>runs pending jobs, setting their status to running and calling the implementation associated with
 * the module + name
 * <be>sets status to complete / error
 */
public class JobsRunner implements InitAPI {

  private static final String LOG_LANG             = "en";
  private static final Logger log                  = LoggerFactory.getLogger(JobsRunner.class);

  private int                 concurrentJobs       = 2;
  private final Messages      messages             = Messages.getInstance();
  private Vertx               vertx;
  private Map<String, JobAPI> jobCache             = new HashMap<>();

  private static String MODULE_CLAUSE              = "{ \"module\": \"{0}\"}";

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> resultHandler) {

    /**
     * load all implementations in the class path of JobAPI
     */

    this.vertx = vertx;

    //get all jobAPI implementations in the class path
    ArrayList<Class<?>> impls = new ArrayList<>();
    try {
      impls = InterfaceToImpl.convert2Impl(RTFConsts.PACKAGE_OF_IMPLEMENTATIONS,
        RTFConsts.PACKAGE_OF_HOOK_INTERFACES + ".JobAPI", true);
    } catch (Exception e) {
      //no impl found
      log.error(e);
    }

    //loop on jobs implementations - create new instance and call init with vertx param
    for (int i = 0; i < impls.size(); i++) {
      JobAPI job = null;
      String []jobId =  new String[]{null};
      try {
        job = (JobAPI)impls.get(i).newInstance();
        job.init(vertx);
      } catch (Exception e) {
        log.error(e);
      }
      if(job != null){
        String[] jobNames = job.getName();
        //associate multiple job names with a single Job class
        //relevant for example for importing data - multiple job names (import patrons, users, items, etc..)
        //with one implementation - which can internally handle differently based on the name
        for (int j = 0; j < jobNames.length; j++) {
          jobId[0] =  job.getModule() + "_" +  jobNames[j];
          //cache the jobAPI implementations
          jobCache.put(jobId[0], job);
          log.info("Loaded Job implementation module: " + job.getModule() + " job name: " + jobNames[j]);
        }
      }
    }

    // set periodic to query db for pending states and run them if there is an open slot
    vertx.setPeriodic(60000, todo -> {
      try {
        //kick off the processing
        process();
      } catch (Exception e) {
        log.error(e);
        resultHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
      }
    });

    //return
    resultHandler.handle(io.vertx.core.Future.succeededFuture(true));
  }


  /**
  * check how many import processes are active - if less then threshold - then go to the mongo job collection and
  * pull pending jobs and run them.
  */
  private void process() throws Exception {

   long start = System.nanoTime();

   String pendingOrRunningEntries = "{\"$or\" : [{ \"status\": \"PENDING\"},{ \"status\": \"RUNNING\"}]}";

       //"{\"$and\": [ { \"module\": \""+RTFConsts.IMPORT_MODULE+"\"}, "
       //+ "{\"$and\": [{ \"status\": \"status\"},"
       //+ "{\"$or\" : [{ \"status\": \"PENDING\"},{ \"status\": \"RUNNING\"}]}]}";

   JsonObject j = new JsonObject(pendingOrRunningEntries);

   //get running and pending jobs
   MongoCRUD.getInstance(vertx).get(
     MongoCRUD.buildJson(Job.class.getName(), RTFConsts.JOBS_COLLECTION, j,
       "last_modified", "asc"), reply -> {
       if (reply.succeeded()) {
         int runningCounter = 0;
         List<Job> runCandidates = new ArrayList<>();
         List<Job> conf = (List<Job>) reply.result();
         //check how many jobs in running and collect pending jobs to run in case
         //there is a slot open
         for (int i = 0; i < conf.size(); i++) {
           if (RTFConsts.STATUS_RUNNING.equals(conf.get(i).getStatus())) {//<----FIX must
             //update counter of running jobs so that we dont start a pending job
             //if no free run slots are available
             runningCounter++;
           }
           else {
              // pending state - it is a run candidate - note the asc sort so we deal with earlier uploads before later ones
              runCandidates.add(conf.get(i));
           }
         }
         if(runCandidates.isEmpty()){
           return;
         }
         // for every available slot set status to running and start handling
         for (int i = 0; i < Math.min(concurrentJobs-runningCounter , runCandidates.size()); i++) {
           Job torun = runCandidates.get(i);
           if (torun != null) {
             updateStatusAndExecute(torun);
           }
         }
   } else {
     log.error("Unable to get uploaded file queue, nothing will not be run, ", reply.cause());
   }
   long end = System.nanoTime();
   log.debug(messages.getMessage(LOG_LANG, MessageConsts.Timer, "Reading jobs to process from mongo ",
     end - start));
   });

  }

  private void updateStatusAndExecute(Job conf){

   String jobType = conf.getModule() + "_" + conf.getName();
   //build query from params then update to running
   JsonObject query = MongoCRUD.entity2JsonNoLastModified(conf);

   conf.setStatus(RTFConsts.STATUS_RUNNING);
   // update status of file
   MongoCRUD.getInstance(vertx).update(
     RTFConsts.JOBS_COLLECTION,
     conf, query, false, true,
     reply2 -> {
       if (reply2.failed()) {
         log.error("Unable to save job to running state, it will not be run now "
             + query.encodePrettily(), reply2.cause());
       } else {
         //process job
         JobAPI job = jobCache.get(jobType);
         job.process(conf, reply -> {
           if(reply.succeeded()){
             reply.result().setStatus(RTFConsts.STATUS_COMPLETED);
             updateStatusDB(reply.result());
           }else{
             conf.setStatus(RTFConsts.STATUS_ERROR);
             updateStatusDB(conf);
           }
         });
       }
     });
  }

  /**
   * update the conf object in mongo with the object passed in using the code as the key
   * @param conf
   */
  private void updateStatusDB(Job conf) {
    String[] removeFromQuery = new String[]{"last_modified", "status", "parameters"};
    JsonObject query = MongoCRUD.entity2Json(conf, removeFromQuery);
    MongoCRUD.getInstance(vertx).update(RTFConsts.JOBS_COLLECTION, conf, query, false, true, reply2 -> {
      if (reply2.failed()) {
        log.error("Unable to save job status for job,, " + query.encodePrettily());
      }
    });
  }

}
