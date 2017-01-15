package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
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
import java.util.HashMap;
import java.util.Map;

import org.folio.rest.jaxrs.model.Job;
import org.folio.rest.jaxrs.model.JobConf;
import org.folio.rest.resource.handlers.FileDataHandler;
import org.folio.rest.resource.interfaces.Importer;
import org.folio.rest.resource.interfaces.JobAPI;
import org.folio.rest.tools.RTFConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.InterfaceToImpl;

/**
 * This is a job that loads importer implementations in the class path and registers their address on the event bus so
 * that when an event on that address comes in, this job handles calling the correct importer to process the import
 *
 * This class implements the JobAPI therefore is run during verticle deployment by the JobRunner.
 * <br>Its purpose is to handle file uploads.
 * <br>The class registers on the event bus and therefore can get messages from the /admin/upload service when a file
 * <br>was successfully uploaded.
 * <br>The implementation is generic as it listens for import event address notifications and once a message indicating a file has been uploaded
 * <br>into the temp directory - it:
 * <br>1. adds an entry in the config collection with the path to the file with a pending state
 * <br> It then uses the JobAPI to manage its lifecycle which -
 * <br>1. checks in the mongo collection how many imports are currently in the running state
 * <br>2. if less then the threshold then the
 * entry in the collection is updated for the uploaded file from pending to running
 * <br> It then uses the Importer interface:
 * <br>1. the file is processed by an implementation of the {@code org.folio.rest.resource.interfaces.Importer} interface and
 * loaded into the appropriate collection by the {@code FileDataHandler}
 * <br>2. if there is a critical error the job stops and sets status to ERROR
 *
 * <br>if the job completed with failed records the status is updated to COMPLETED with a count of
 * successful and count of errors
 * <br>failed items are printed to the log for now in debug mode logging
 *
 * <br>The work is split between the ProcessUploads class and an {@code org.folio.rest.resource.interfaces.Importer} interface implementation.
 * The implementation needs to declare some configurations, indicate the line delimiter, and run any processing it needs on a specific line
 * it receives.
 */
public class ProcessUploads implements JobAPI {

  private static final String LOG_LANG             = "en";
  private static final Logger log                  = LoggerFactory.getLogger(ProcessUploads.class);
  private final Messages      messages             = Messages.getInstance();
  private Vertx               vertx;
  private Map<String, Importer> importerCache      = new HashMap<>();

  @Override
  public void init(Vertx vertx) {

    this.vertx = vertx;

    /*
     * get all importer implementations in the classpath
     */
    ArrayList<Class<?>> impls = new ArrayList<>();
    try {
      impls = InterfaceToImpl.convert2Impl(RTFConsts.PACKAGE_OF_IMPLEMENTATIONS,
        RTFConsts.PACKAGE_OF_HOOK_INTERFACES + ".Importer", true);
    } catch (Exception e) {
      //no impl found
      log.error(e);
    }

    /*
     * loop on importer impl, extract the address field and create a event bus handler on each
     * of the implementation's addresses
     */
    for (int i = 0; i < impls.size(); i++) {
      Importer importer = null;
      String []address =  new String[]{null};
      try {
        importer = (Importer)impls.get(i).newInstance();
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
      if(importer != null){
        address[0] =  importer.getImportAddress();

        if(address[0] == null){
          //throw exception
          log.error("Notification address is null, can not register job ");
        }
        else{
          //cache the importer impl
          importerCache.put(address[0], importer);

          /*
           * register each address from each Importer impl on the event bus
           */
          MessageConsumer<Object> consumer = vertx.eventBus().consumer(address[0]);
          consumer.handler(message -> {
            log.debug("Received a message to " + address[0] + ": " + message.body());
            JobConf cObj = (JobConf) message.body();
            registerJob(cObj, message);
          });
          log.info("Import Job " + impls.get(i).getName() + " Initialized, registered on address " + address[0]);
        }
      }
    }
  }

  /**
   * register a job to run - saved in the folio_shared schema with institution id field.
   * online access should contain the inst id field to filter on only inst records
   * 1. check if there is a configuration for this job in the job configuration collection
   * 2. if there is none, add one
   * 3. once there is a config for the job , add a job entry in the job collection
   * This is done via mongo queries instead of using the REST API as external modules may use.
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

    String jobConfExistsQuery =
      "{\"$and\": [ { \"module\": \""+cObj.getModule()+"\"}, "
      + "{ \"name\": \""+cObj.getName()+"\"}, "
      + "{ \"inst_id\": { \"$exists\": true }},"
      + "{ \"inst_id\": \"" +instId+ "\"}]}";

    JsonObject j = new JsonObject(jobConfExistsQuery);
    // check if there is a job configuration of this type
/*    MongoCRUD.getInstance(vertx).get(
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
                log.info("Added job configuration entry for module: " + cObj.getModule() + " and name " + cObj.getName() + " inst name: " + cObj.getInstId());
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
      });*/
  }

  /**
   * Save an entry of the path to the uploaded file to mongo in pending state
   * This is done via mongo queries instead of using the REST API as external modules may use.
   * @param message - message to return to the runtime environment with status of the db save
   */
  private void saveAsPending2DB(JobConf cObj, String filename, Message<Object> message) {

/*    Job job = new Job();
    job.setJobConfId(cObj.getId());
    job.setStatus(RTFConsts.STATUS_PENDING);
    job.setModule(cObj.getModule());
    job.setName(cObj.getName());
    job.setInstId(cObj.getInstId());
    Parameter p2 = new Parameter();
    p2.setKey("file");
    p2.setValue(filename);
    job.setBulkSize(cObj.getBulkSize());
    job.setFailPercentage(cObj.getFailPercentage());
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
    });*/
  }

  @Override
  public String getModule() {
    return "IMPORTS";
  }

  @Override
  public String[] getName() {
    //all Importer implementations' addresses are returned here so that the
    //job runner knows to call the ProcessUploads class to handle when retrieving
    //a job from the DB associated with one of those addresses
    return importerCache.keySet().toArray(new String[]{});
  }

  @Override
  public void process(Job job, Handler<AsyncResult<Job>> replyHandler) {
    //start processing the uploaded file by getting props
    vertx.fileSystem().props( job.getParameters().get(0).getValue(),
      reply3 -> {
        if (reply3.result() != null) {
          long fileSize = reply3.result().size();
          parseFile(fileSize, job, replyHandler);
        } else {
          log.error("Unable to get properties of uploaded file, it will not be run, "
              + job.getParameters().get(0).getValue());
        }
      });
  }

  /**
   * Main work done by the FileDataHandler which reads in line by line and passes on that line
   * to the correct Importer implementation for line processing
   * @param fileSize
   * @param conf
   * @param replyHandler - the handler returns a job object with success and error counter parameters
   * to be persisted by the job runner
   */
  private void parseFile(long fileSize, Job conf, Handler<AsyncResult<Job>> replyHandler) {
    String file = conf.getParameters().get(0).getValue();
    vertx.fileSystem().open(file, new OpenOptions(), ar -> {
      if (ar.succeeded()) {
        AsyncFile rs = ar.result();
        rs.handler(new FileDataHandler(vertx, conf, fileSize, importerCache.get(conf.getName()), reply -> {
          if(reply.failed()){
            if(reply.cause().getMessage().contains(RTFConsts.STATUS_ERROR_THRESHOLD)){
              log.error("Stopping import... Error threshold exceeded for file " + file);
              try{
                //can throw an exception if the error threshold is met at
                //the last bulk where the endHandler is called before the stop on error is called
                rs.pause().close();
              }
              catch(Exception e){
                log.error("Error threshold hit on last block of data ", e);
              }
              replyHandler.handle(io.vertx.core.Future.failedFuture(RTFConsts.STATUS_ERROR_THRESHOLD));
            }
          }
          else{
            replyHandler.handle(io.vertx.core.Future.succeededFuture(reply.result()));
          }
        }));
        rs.exceptionHandler(t -> {
          log.error("Error reading from file " + file, t);
          replyHandler.handle(io.vertx.core.Future.failedFuture(RTFConsts.STATUS_ERROR));
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
        replyHandler.handle(io.vertx.core.Future.failedFuture(RTFConsts.STATUS_ERROR));
      }
    });
  }

  @Override
  public int getPriority() {
    return 10;
  }

  @Override
  public boolean getRunOffPeakOnly() {
    return false;
  }

  @Override
  public boolean isResumable() {
    return true;
  }
}
