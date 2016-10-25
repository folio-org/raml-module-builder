package org.folio.rest.resource.handlers;

import org.apache.commons.lang3.StringEscapeUtils;
import org.folio.rest.jaxrs.model.Job;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.persist.MongoCRUD;
import org.folio.rest.resource.interfaces.Importer;
import org.folio.rest.tools.RTFConsts;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


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
  /**
   * 0 - more bytes to read, 1 - reading last buffer in file, 2 - read last row in last buffer
   */
  private int status = 0; 

  
  public FileDataHandler(Vertx vertx, Job conf, long fileSize, Importer importObj){
    this.vertx = vertx;
    this.fileSize = fileSize;
    this.conf = conf;
    this.importer = importObj;
    
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

      totalLines[0]++;
      
      Object toSave = importer.processLine(rows[i]);

      if(toSave != null){
        MongoCRUD.getInstance(vertx).save(importer.getCollection(), toSave, reply -> {
          if(reply.failed()){
            errorCount[0]++;
            log.error("Error saving object " + reply.cause().getMessage());
          }
          else{
            successCount[0]++;
            log.debug("#" +successCount[0]+ " Saved object " + reply.result());
          }
          if(status == 1 && totalLines[0] == (errorCount[0]+successCount[0])){
            System.out.println("in the if staement ---- ");
            updateStatus(conf);
          }
        });
      }
      else{
        log.error("Error saving object for row " + rows[i]);
        errorCount[0]++;
      }
    }
  }
  
  private void updateStatus(Job conf){
    //set this job to completed in DB
    String query = "{ \"parameters.value\": \""+StringEscapeUtils.escapeJava(conf.getParameters().get(0).getValue())+"\"}";

    conf.setStatus(RTFConsts.STATUS_COMPLETED);
    Parameter p1 = new Parameter();
    p1.setKey("success");
    p1.setValue(String.valueOf(successCount[0]));
    Parameter p2 = new Parameter();
    p2.setKey("errors");
    p2.setValue(String.valueOf(errorCount[0]));
    
    conf.getParameters().add(p1);
    conf.getParameters().add(p2);
    MongoCRUD.getInstance(vertx).update(RTFConsts.JOBS_COLLECTION, conf, new JsonObject(query), false, true, rep -> {
      if(rep.failed()){
        log.error("Unable to update status of job for file " + conf.getParameters().get(0).getValue() +
          " as completed, this should be fixed manually in the database");
      }
    });
  }
}
