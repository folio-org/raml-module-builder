package org.folio.rest.tools.client;

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

  public Response joinOn(String field1, Response response, String field2) throws ResponseNullPointer {
    if(this.body == null || response == null || response.body == null){
      throw new ResponseNullPointer();
    }
    body.forEach( entry -> {
      entry.getValue();
    });
    return null;
  }

  /**
   * join current response with response parameter using the same field name
   * from both response bodies json
   * @param field
   * @param response
   * @return
   */
  public Response joinOn(String field, Response response) throws ResponseNullPointer {

    if(this.body == null || response == null || response.body == null){
      throw new ResponseNullPointer();
    }
    body.forEach( entry -> {
      entry.getValue();
    });
    return null;

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
}