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
import org.folio.rest.persist.MongoCRUD;
import org.folio.rest.resource.interfaces.Importer;


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

  private String lastRowFromPreviousBuffer = null;

  private long fileSize = 0;
  private long bytesRead = 0;

  private int []totalLines = new int[]{0};

  private Job conf;
  private Importer importer;
  private Handler<AsyncResult<Job>> replyHandler;
  /**
   * 0 - more bytes to read, 1 - reading last buffer in file, 2 - read last row in last buffer
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
    this.bulkSize = Math.max(importer.getBulkSize() ,1);

    log.info("Starting processing for file " + conf.getParameters().get(0).getValue() + "\nsize: " + fileSize +
      "\nBulk size: " + importObj.getBulkSize() + "\nCollection: " + importObj.getCollection() + "\nImport Address:"
        + importObj.getImportAddress());
  }

  @Override
  public void handle(Buffer event) {

    //keep track of bytes read to know if we are finished reading
    bytesRead = bytesRead + event.getBytes().length;
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
    //iterate over the read rows
    for (int i = 0; i < rows.length; i++) {
      if(lastRowFromPreviousBuffer != null && i==0){
        //previous buffer had a partial row - add its content to the first row of the
        //next buffer
        rows[i] = lastRowFromPreviousBuffer + rows[i];
        lastRowFromPreviousBuffer = null;
      }
      if(i == rows.length-1 && !rows[i].endsWith(LINE_SEPS)){
        //the last row of a buffer may not be complete - if it doesnt end with a new line
        //it has partial content
        lastRowFromPreviousBuffer = rows[i];
      }

      Object toSave = importer.processLine(rows[i]);

      if(toSave != null){
        bulks.add(toSave);
      }
      else{
        log.error("Error saving object for row " + rows[i]);
        errorCount[0]++;
        if(status == 1 && totalLines[0] == (errorCount[0]+successCount[0])){
          updateStatus(conf);
          replyHandler.handle(io.vertx.core.Future.succeededFuture(conf));
        }
      }

      if(bulks.size() == bulkSize || (status == 1 && totalLines[0] == (errorCount[0]+successCount[0]+bulks.size()))){
        List<Object> persistList = new ArrayList<>(bulks);
        bulks.clear();
        MongoCRUD.getInstance(vertx, conf.getInstId()).bulkInsert(importer.getCollection(), persistList, reply -> {
          if(reply.failed()){
            errorCount[0] = errorCount[0]+persistList.size(); // <-- this is not correct
            log.error("Error saving object " + reply.cause().getMessage());
          }
          else{
            successCount[0] = successCount[0] + persistList.size();
            log.debug("#" +successCount[0]+ " Saved object " + reply.result());
          }
          if(status == 1 && totalLines[0] == (errorCount[0]+successCount[0])){
            updateStatus(conf);
            replyHandler.handle(io.vertx.core.Future.succeededFuture(conf));
          }
        });
      }

    }
  }

  private void updateStatus(Job conf){

    Parameter p1 = new Parameter();
    p1.setKey("success");
    p1.setValue(String.valueOf(successCount[0]));
    Parameter p2 = new Parameter();
    p2.setKey("errors");
    p2.setValue(String.valueOf(errorCount[0]));

    conf.getParameters().add(p1);
    conf.getParameters().add(p2);
  }
}
