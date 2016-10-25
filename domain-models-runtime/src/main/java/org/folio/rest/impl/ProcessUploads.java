package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.folio.rest.jaxrs.model.Job;
import org.folio.rest.jaxrs.model.JobConf;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.persist.MongoCRUD;
import org.folio.rest.resource.interfaces.Importer;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.InterfaceToImpl;
import org.folio.rest.tools.utils.LogUtil;
import org.folio.rest.tools.RTFConsts;
import org.folio.rest.resource.handlers.FileDataHandler;

/**
 * This class implements the InitAPI and therefore is run during verticle deployments.
 * <br>Its purpose is to handle file uploads.
 * <br>The class registers on the event bus and therefore can get messages from the /admin/upload service when a file
 * <br>was successfully uploaded.
 * <br>The implementation is generic as it listens for import event address notifications and once a message indicating a file has been uploaded
 * <br>into the temp directory - it:
 * <br>1. adds an entry in the config collection with the path to the file with a pending state
 * <br>2. checks in the mongo collection how many imports are currently in the running state
 * <br>3. if less then the threshold (currently hard coded at 2 - should be configurable TODO) - then the
 * entry in the collection is updated for the uploaded file from pending to running
 * <br>4. the file is parsed , validated , and loaded into the appropriate collection
 * <br>5. if there is a critical error the job stops and sets status to ERROR
 * <br>6. if the job completed with failed records the status is updated to COMPLETED with a count of
 * successful and count of errors
 * <br>7. failed items are printed to the log for now
 *
 * <br>The work is split between the ProcessUploads class and an {@code org.folio.rest.resource.interfaces.Importer} interface implementation.
 * The implementation needs to declare some configurations, indicate the line delimiter, and run any processing it needs on a specific line 
 * it receives.
 */
public class ProcessUploads implements InitAPI {

  private static final String LOG_LANG             = "en";
  private static final Logger log                  = LoggerFactory.getLogger(ProcessUploads.class);

  private int                 concurrentImports    = 2;
  private final Messages      messages             = Messages.getInstance();
  private Vertx               vertx;
  private Map<String, Importer> importerCache      = new HashMap<>();
  
  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> resultHandler) {
    this.vertx = vertx;
    try {
      //get all importer implementations in the classpath
      ArrayList<Class<?>> impls = InterfaceToImpl.convert2Impl(RTFConsts.PACKAGE_OF_IMPLEMENTATIONS, 
        RTFConsts.PACKAGE_OF_HOOK_INTERFACES + ".Importer", true);
      //loop on importer impl, extract the address field and create a event bus handler on each
      //of the implementation's addresses
      for (int i = 0; i < impls.size(); i++) {
        Importer importer = (Importer)impls.get(i).newInstance();
        String address =  importer.getImportAddress();
        if(address == null){
          //throw exception
        }
        //cache the importer impl
        importerCache.put(address, importer);
        
        //register each address from each Importer impl on the event bus
        MessageConsumer<Object> consumer = vertx.eventBus().consumer(address);
        consumer.handler(message -> {
          log.debug("Received a message to " + address + ": " + message.body());
          JobConf cObj = (JobConf) message.body();
          registerJob(cObj, message);
        });
        LogUtil.formatLogMessage(getClass().getName(), "runHook",
          "One time hook called with implemented class " + "named " + impls.get(i).getName());
      }
      // set periodic to query db for pending states and run them if there is an open slot
      // this is a terrible hack and should be in the periodicAPI hook TODO
      vertx.setPeriodic(60000, todo -> {
        try {
          //kick off the running of the import file process
          process();
        } catch (Exception e) {
          log.error(e);
        }
      });
    } catch (Exception e) {
      log.error(e.getCause().getMessage(), e);
    }
    resultHandler.handle(io.vertx.core.Future.succeededFuture(true));
  }
  
  
  /**
   * register a job to run - saved in the folio_shared schema with institution id field.
   * online access should contain the inst id field to filter on only inst records
   * 1. check if there is a configuration for this job in the job configuration collection
   * 2. if there is none, add one
   * 3. once there is a config for the job , add a job entry in the job collection
   * @param cObj
   * @param message
   */
  private void registerJob(JobConf cObj, Message<Object> message) {

    //only one parameter passed and it is the filename to process
    //this is a hack as this param does not belong in the job conf object
    //but is used in this case to transport this info from the rest verticle to here
    String filename = cObj.getParameters().get(0).getValue();
    cObj.setParameters(null);
    
    String instId = cObj.getInstId();
    
    String jobConfExistsQuery = "{\"$and\": [ { \"module\": \""+RTFConsts.IMPORT_MODULE+"\"}, "
      + "{ \"name\": \""+cObj.getName()+"\"} ]}";
    
    JsonObject j = new JsonObject(jobConfExistsQuery);
    // check if there is a job configuration of this type
    MongoCRUD.getInstance(vertx).get(
      MongoCRUD.buildJson(JobConf.class.getName(), RTFConsts.JOB_CONF_COLLECTION, j), reply -> {
        if (reply.succeeded()) {
          if(reply.result().size() == 1){
            //there is a job configuration for this job
            //save an instance of this job to the job collection            
            saveAsPending2DB((JobConf)reply.result().get(0), filename, message);
          }
          else if(reply.result().size() == 0){
            //add a job configuration for this job            
            MongoCRUD.getInstance(vertx).save(RTFConsts.JOB_CONF_COLLECTION, cObj, reply2 -> {
              if (reply2.failed()) {
                log.error("Unable to save the module " + cObj.getModule() + " with name " + cObj.getName() + " to the job conf"
                  + " collection, job will not run... Error: " + reply.cause().getMessage(), reply.cause());
                message.reply(RTFConsts.ERROR_PROCESSING_STATUS);
              }
              else{
                String id = reply2.result();//job conf id
                cObj.setId(id);
                log.info("Added job configuration entry for module: " + cObj.getModule() + " and name " + cObj.getName());
                saveAsPending2DB(cObj, filename, message);
              }
            });
          }
          else{
            //there is a problem, more then one configurations for thie job, stop processing
            //and return an error
            log.error("The module " + cObj.getModule() + " with name " + cObj.getName() +
              " has more then one configurations associated with it, can not continue processing");
          }
        }
      });
  }
  
  /**
   * Save an entry of the path to the uploaded file to mongo in pending state
   * @param message - message to return to the runtime environment with status of the db save
   */
  private void saveAsPending2DB(JobConf cObj, String filename, Message<Object> message) {

    Job job = new Job();
    job.setJobConfId(cObj.getId());
    job.setStatus(RTFConsts.STATUS_PENDING);
    job.setModule(cObj.getModule());
    job.setName(cObj.getName());
    Parameter p2 = new Parameter();
    p2.setKey("file");
    p2.setValue(filename);    
    
    List<Parameter> jobParams = new ArrayList<>();
    jobParams.add(p2);
    
    job.setParameters(jobParams);
    
    MongoCRUD.getInstance(vertx).save(RTFConsts.JOBS_COLLECTION, job, reply2 -> {
      if (reply2.failed()) {
        log.error("Unable to save uploaded file to jobs collection, it will not be run, " + filename);
        message.reply(RTFConsts.ERROR_PROCESSING_STATUS);
      }
      else{
        log.info("Set job to pending state for file: " + filename);
        message.reply(RTFConsts.OK_PROCESSING_STATUS);
      }
    });
  }

  /**
  * check how many import processes are active - if less then threshold - then go to the mongo queue and
  * pull pending jobs and run them.
  * read a tab delimited file containing 6 columns representing a basic item and push them into mongo 
  * reading the file is async if this is * uploaded from a form and contains boundaries - then those 
  * rows should be filtered out by the cols.length==6 - if not for some reason -
  * the validation on the item will filter them out
  */
  private void process() throws Exception {

   long start = System.nanoTime();

   String pendingOrRunningEntries = "{\"$and\": [ { \"module\": \""+RTFConsts.IMPORT_MODULE+"\"}, "
       //+ "{\"$and\": [{ \"status\": \"status\"},"
       + "{\"$or\" : [{ \"status\": \"PENDING\"},{ \"status\": \"RUNNING\"}]}]}";
   
   JsonObject j = new JsonObject(pendingOrRunningEntries);

   //get running and pending jobs
   MongoCRUD.getInstance(vertx).get(
     MongoCRUD.buildJson(Job.class.getName(), RTFConsts.JOBS_COLLECTION, j,
       "parameters.file", "asc"), reply -> {
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
         for (int i = 0; i < Math.min(concurrentImports-runningCounter , runCandidates.size()); i++) {
           Job torun = runCandidates.get(i);
           if (torun != null) {
             updateStatusAndExecute(torun);
           }
         }
   } else {
     log.error("Unable to get uploaded file queue, nothing will not be run, ", reply.cause());
   }
   long end = System.nanoTime();
   log.debug(messages.getMessage(LOG_LANG, MessageConsts.Timer, "Reading configs for import process",
     end - start));
   });
   
  }
  
  private void updateStatusAndExecute(Job conf){
    
   String file = conf.getParameters().get(0).getValue();
   conf.setStatus(RTFConsts.STATUS_RUNNING);
   // update status of file                             
   MongoCRUD.getInstance(vertx).update(
     RTFConsts.JOBS_COLLECTION,
     conf, new JsonObject("{\"parameters.value\":\""+StringEscapeUtils.escapeJava(file)+"\"}"), false, true,
     reply2 -> {
       if (reply2.failed()) {
         log.error("Unable to save uploaded file to queue, it will not be run, "
             + conf.getParameters().get(0).getValue(), reply2.cause());
       } else {
         vertx.fileSystem().props( conf.getParameters().get(0).getValue(),
           reply3 -> {
             if (reply3.result() != null) {
               long fileSize = reply3.result().size();
               parseFile(fileSize, conf);
             } else {
               log.error("Unable to get properties of uploaded file, it will not be run, "
                   + file);
             }
           });
       }
     });
  }

  private void parseFile(long fileSize, Job conf) {
    String file = conf.getParameters().get(0).getValue();
    vertx.fileSystem().open(file, new OpenOptions(), ar -> {
      if (ar.succeeded()) {
        AsyncFile rs = ar.result();
        rs.handler(new FileDataHandler(vertx, conf, fileSize, importerCache.get(conf.getName())));
        rs.exceptionHandler(t -> {
          log.error("Error reading from file " + file, t);
          conf.setStatus(RTFConsts.STATUS_ERROR);
          updateStatusDB(conf);
        });
        rs.endHandler(v -> {
          rs.close(ar2 -> {
            if (ar2.failed()) {
              log.error("Error closing file " + file, ar2.cause());
            }
          });   
        });
      } else {
        log.error("Error opening file " + file, ar.cause());
        conf.setStatus(RTFConsts.STATUS_ERROR);
        updateStatusDB(conf);
      }
    });
  }
  
  /**
   * update the conf object in mongo with the object passed in using the code as the key
   * @param conf
   */
  private void updateStatusDB(Job conf) {
    String file = conf.getParameters().get(0).getValue();

    String query = "{\"parameters.value\":\""+file+"\"}";    
    MongoCRUD.getInstance(vertx).update(RTFConsts.JOBS_COLLECTION, conf, new JsonObject(query), false, true, reply2 -> {
      if (reply2.failed()) {
        log.error("Unable to save uploaded file to queue, it will not be run, " + file);
      }
    });
  }

}
