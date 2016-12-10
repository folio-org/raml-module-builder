package org.folio.rest.resource.handlers;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import org.folio.rest.jaxrs.model.Job;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.resource.interfaces.Importer;
import org.folio.rest.tools.RTFConsts;

/**
 *
 * this handler is used by the importer mechanism to parse files and calls importer implementations
 * to process a single line in a file. the file should be transformed to an object by the implementations
 * and that object is saved to mongo
 *
 */
public class FileDataHandler implements io.vertx.core.Handler<Buffer> {

  private static final Logger log = LoggerFactory.getLogger(FileDataHandler.class);
  private static final String LINE_SEPS = System.getProperty("line.separator");

  private Vertx vertx = null;
  private int []successCount = new int[]{0};
  private int []errorCount = new int[]{0};
  private StringBuilder processingErrorIds = new StringBuilder();
  private String lastRowFromPreviousBuffer = null;
  private int processedBulks = 0;
  private long fileSize = 0;
  private long bytesRead = 0;
  private boolean stopWithError = false;
  private int []totalLines = new int[]{0};
  private double failPercentage = 100.00; //allow all records to fail without stopping the job by default
  private Job conf;
  private Importer importer;
  private Handler<AsyncResult<Job>> replyHandler;
  private double percentOfFileRead;
  private double avgRowSize;
  private boolean jobComplete = false;
  /**
   * 0 - more bytes to read, 1 - reading last buffer in file
   */
  private int status = 0;

  private int bulkSize = 0;
  private List<Object> bulks = new ArrayList<>();

  public FileDataHandler(Vertx vertx, Job conf, long fileSize, Importer importObj, Handler<AsyncResult<Job>> replyHandler){
    this.vertx = vertx;
    this.fileSize = fileSize;
    this.conf = conf;
    this.importer = importObj;
    this.replyHandler = replyHandler;
    this.bulkSize = Math.max(conf.getBulkSize() ,1);
    if(conf.getFailPercentage() > 0.1){
      this.failPercentage = conf.getFailPercentage();
    }
    log.info("Starting processing for file " + conf.getParameters().get(0).getValue() + "\nsize: " + fileSize +
      "\nBulk size: " + bulkSize + "\nCollection: " + importObj.getCollection() + "\nImport Address:"
        + importObj.getImportAddress() + "\nInstitution: " + conf.getInstId());
  }

  @Override
  public void handle(Buffer event) {

    long sizeOfBuffer = event.getBytes().length;

    //keep track of bytes read to know if we are finished reading
    bytesRead = bytesRead + sizeOfBuffer;
    if(fileSize <= bytesRead){
      status = 1;
    }

    //split buffer read into new line delimited rows - keeping the delimiters
    //so that we can keep track if we read a complete row or if it was cut off
    String del = importer.getLineDelimiter();
    if(del == null){
      del = LINE_SEPS;
    }
    String []rows = event.toString("UTF8").split("(?<="+del+")");
    totalLines[0] = totalLines[0] + rows.length;

    //this is a best effort - before we start processing the buffer
    //check errors so far after the bytes read so far and extrapulate
    //for entire file size - there may be still async inserts that could fail
    //while we check as well, hence this is only a best effort.
    //also assumes the rows are relatively of the same size or at least that the file
    //is distributed evenly across buffers
    percentOfFileRead = ((double) bytesRead / fileSize)*100;
    avgRowSize = sizeOfBuffer/rows.length;

    //iterate over the read rows
    for (int i = 0; i < rows.length; i++) {
      if(lastRowFromPreviousBuffer != null && i==0){
        //previous buffer had a partial row - add its content to the first row of the
        //next buffer
        rows[i] = lastRowFromPreviousBuffer + rows[i];
        lastRowFromPreviousBuffer = null;
        //remove one from the total lines count else this line will be counted twice
        totalLines[0] = totalLines[0]-1;
      }
      if(i == rows.length-1 && !rows[i].endsWith(del) && status != 1){
        //the last row of a buffer may not be complete - if it doesnt end with a new line
        //it has partial content. if we are on the last buffer and the line is not complete
        //process it as an error - hence we should not continue
        lastRowFromPreviousBuffer = rows[i];
        continue;
      }

      Object toSave = importer.processLine(rows[i].replaceAll(del, ""));

      if(toSave != null){
        bulks.add(toSave);
      }
      else{
        log.error("Error saving object for row " + rows[i]);
        errorCount[0]++;
        processingErrorIds.append(totalLines[0]-rows.length+i).append(",");
        if(status == 1 && totalLines[0] == (errorCount[0]+successCount[0])){
          updateStatus(conf);
        }
      }

      //if we've reached the bulk size - flush to db
      //if we are on the last buffer being read, and we are on the last line and we have content
      //in the bulks - flush even if the bulkSize has not been reached
      if(bulks.size() == bulkSize || (status == 1 && i+1==rows.length && bulks.size()>0)){
        List<Object> persistList = new ArrayList<>(bulks);
        bulks.clear();
        flush(persistList, processedBulks++);
      }
    }
  }

  private void flush(List<Object> persistList, int bulkId){
/*    MongoCRUD.getInstance(vertx, conf.getInstId()).bulkInsert(importer.getCollection(), persistList, reply -> {
      Bulk bInfo = new Bulk();
      bInfo.setInstId(conf.getInstId());
      bInfo.setJobId(conf.getId());
      bInfo.setBulkId(bulkId);
      if(reply.failed()){
        //bulk info
        bInfo.setErrorCount(persistList.size());
        bInfo.setSuccessCount(0);
        //global info
        errorCount[0] = errorCount[0]+persistList.size();
        log.error("Error saving object " + reply.cause().getMessage());
      }
      else{
        //how many records actually were persisted
        int inserted = reply.result().getInteger("n");
        bInfo.setSuccessCount(inserted);
        //add them to success counter
        successCount[0] = successCount[0] + inserted;
        log.info("#of inserted " + inserted + ", out of " + persistList.size());

        //if there were errors handle...
        if(inserted < persistList.size()){
          //add number of errors in this bulk
          bInfo.setErrorCount(persistList.size() - inserted);
          //update total error count
          errorCount[0] = errorCount[0] + bInfo.getErrorCount();
          //get errors from reply
          JsonArray jar = reply.result().getJsonArray("writeErrors");
          List<JsonObject> errors = jar.getList();

          //if more than half the bulk contains errors, print out a sample of the errors to the log
          int size = errors.size();
          boolean sample = false;
          if(size*2>=bulkSize){
            //too many errors, print sampling of errors to log
            sample = true;
          }
          StringBuilder sb = new StringBuilder();
          for (int j = 0; j < size; j++) {
            JsonObject errorLine = errors.get(j);
            if(sample){
              if(j%100==0){
                log.error("Error saving object " + errorLine.getString("errmsg"));
              }
            }
            else{
              log.error("Error saving object " + errorLine.getString("errmsg"));
            }
            //save line in file which failed
            sb.append(errorLine.getInteger("index")).append(",");
          }
          bInfo.setErrorIds(sb.toString());
        }
      }
      //add bulk entry into bulk collection with current bulk info
      JobsRunner.addBulkStatusDB(bInfo);

      //start checking after processing at least 20 percent
      if(percentOfFileRead > 20){
        double unitsInFile = fileSize/avgRowSize;
        //compare current failure percentage current total errors
        //compared to total estimated records to process
        if((errorCount[0]/unitsInFile)*100 > this.failPercentage){
          stopWithError = true;
          //processUploads class will get this callback and should close the stream to the file
          //immediately
          updateStatus(conf);
        }
      }

      //only way to verify all async call are complete is to compare totals lines to the
      //actual amount of lines processed asynchronously which can be gathered by adding the
      //success and error counts since these are incremented when the handlers are called
      //by the completion of the async calls
      if(status == 1 && totalLines[0] == (errorCount[0]+successCount[0])){
        updateStatus(conf);
      }
    });*/
  }

  /**
   * update after every bulk so that we can resume if a crash occurs
   */
  private void updateStatus(Job conf){
    //if we are in stopWithError - there is a chance this function
    //will get called twice, so just as a safety set jobComplete to only
    //allow one update call
    if(!jobComplete){
      jobComplete = true;
    }
    else{
      return;
    }

    Parameter p1 = new Parameter();
    p1.setKey("total_success");
    p1.setValue(String.valueOf(successCount[0]));

    Parameter p2 = new Parameter();
    p2.setKey("total_errors");
    p2.setValue(String.valueOf(errorCount[0]));

    Parameter p3 = new Parameter();
    p3.setKey("processing_error_ids");
    p3.setValue(processingErrorIds.toString());

    conf.getParameters().add(p1);
    conf.getParameters().add(p2);
    conf.getParameters().add(p3);
    if(stopWithError){
      replyHandler.handle(io.vertx.core.Future.failedFuture(RTFConsts.STATUS_ERROR_THRESHOLD));
    }
    else{
      replyHandler.handle(io.vertx.core.Future.succeededFuture(conf));
    }
  }
}
