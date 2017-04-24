package org.folio.rest.tools.client;

import java.util.HashMap;
import java.util.Map;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author shale
 *
 */
class Response {

  String endpoint;
  int code;
  JsonObject body;
  String statusMessage;
  JsonObject error;
  Throwable exception;

  public Response joinOn(String withField, Response response, String onField, String insertField) throws ResponseNullPointer {
    if(this.body == null || response == null || response.body == null){
      throw new ResponseNullPointer();
    }
    Map<Object, JsonObject> joinTable = new HashMap<>();
    response.body.fieldNames().forEach( entry -> {
      Object object = response.body.getValue(entry);
      if(object instanceof JsonArray){
        JsonArray rr = (JsonArray)object;
        int size = rr.size();
        for(int i=0; i<size; i++){
          joinTable.put(rr.getJsonObject(i).getValue(onField), rr.getJsonObject(i));
        }
      }
    });
    this.body.fieldNames().forEach( entry -> {
      Object object = this.body.getValue(entry);
      if(object instanceof JsonArray){
        JsonArray rr = (JsonArray)object;
        int size = rr.size();
        for(int i=0; i<size; i++){
          JsonObject jo = rr.getJsonObject(i);
          JsonObject values2join = joinTable.get(jo.getValue(withField));
          if(values2join != null){
            if(insertField == null){
              jo.put(withField, values2join);
            }
            else{
              jo.put(withField, values2join.getValue(insertField));
            }
          }
        }
      }
    });
    return this;
  }

  public Response joinOn(String withField, Response response, String insertField) throws ResponseNullPointer {
    return joinOn(withField, response, withField, insertField);

  }

  /**
   * join current response with response parameter using the same field name
   * from both response bodies json
   * @param field
   * @param response
   * @return
   */
  public Response joinOn(String withField, Response response) throws ResponseNullPointer {
    return joinOn(withField, response, withField, null);

  }

  public static boolean isSuccess(int statusCode){
    if(statusCode >= 200 && statusCode < 300){
      return true;
    }
    return false;
  }

  public void populateError(String endpoint, int statusCode, String errorMessage){
    error = new JsonObject();
    error.put("endpoint", endpoint);
    error.put("statusCode", statusCode);
    error.put("errorMessage", errorMessage);
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public int getCode() {
    return code;
  }

  public void setCode(int code) {
    this.code = code;
  }

  public JsonObject getBody() {
    return body;
  }

  public void setBody(JsonObject body) {
    this.body = body;
  }

  public String getStatusMessage() {
    return statusMessage;
  }

  public void setStatusMessage(String statusMessage) {
    this.statusMessage = statusMessage;
  }

  public JsonObject getError() {
    return error;
  }

  public void setError(JsonObject error) {
    this.error = error;
  }

  public Throwable getException() {
    return exception;
  }

  public void setException(Throwable exception) {
    this.exception = exception;
  }

}