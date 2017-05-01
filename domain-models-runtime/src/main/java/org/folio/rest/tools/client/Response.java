package org.folio.rest.tools.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.folio.rest.tools.parser.JsonPathParser;

import io.vertx.core.json.JsonObject;

/**
 * @author shale
 *
 */
public class Response {

  String endpoint;
  int code;
  JsonObject body;
  String statusMessage;
  JsonObject error;
  Throwable exception;

  /**
   * join this response with response parameter using the withField field name on the current
   * response and the onField field from the response passed in as a parameter (Basically, merge
   * entries from the two responses when the values of the withField and onField match.
   * Replace the withField in the current response
   * with the value found in the insertField (from the Response parameter)
   * @param withField
   * @param response
   * @param onField
   * @param insertField
   * @param allowNulls - whether to place a null value into the json from the passed in response
   * json, in a case where there is no match in the join for a specific entry
   * @return
   * @throws ResponseNullPointer
   */
  public Response joinOn(String withField, Response response, String onField, String insertField, boolean allowNulls) throws ResponseNullPointer {
    if(this.body == null || response == null || response.body == null){
      throw new ResponseNullPointer();
    }
    JsonPathParser jpp = new JsonPathParser(response.body);
    //get list of paths within the json to join on
    List<StringBuilder> sbList = jpp.getAbsolutePaths(onField);
    if(sbList == null){
      //path does not exist in the json, nothing to join on, return response
      return this;
    }
    Map<Object, Object> joinTable = new HashMap<>();
    int size = sbList.size();
    for (int i = 0; i < size; i++) {
      Map<Object, Object> map = jpp.getValueAndParentPair(sbList.get(i));
      if(map != null){
        joinTable.putAll(map);
      }
    }
    jpp = new JsonPathParser(this.body);
    List<StringBuilder> list = jpp.getAbsolutePaths(withField);
    int size2 = list.size();
    for (int i = 0; i < size2; i++) {
      Object o = joinTable.get(jpp.getValueAt(list.get(i).toString()));
      if(o != null){
        if(insertField != null){
          if(o instanceof JsonObject){
            o = new JsonPathParser((JsonObject)o).getValueAt(insertField);
          }
          else{
            o = ((JsonObject)o).getValue(insertField);
          }
        }
        jpp.setValueAt(list.get(i).toString(), o);
      }
      else{
        if(allowNulls){
          jpp.setValueAt(list.get(i).toString(), o);
        }
      }
    }

    return this;
  }

  /**
   * join this response with response parameter using the withField field name on the current
   * response and the onField field from the response passed in as a parameter (Basically, merge
   * entries from the two responses when the values of the withField and onField match.
   * Replace the withField in the current response
   * with the value found in the insertField (from the Response parameter)
   * @param withField
   * @param response
   * @param onField
   * @param insertField
   * @return
   * @throws ResponseNullPointer
   */
  public Response joinOn(String withField, Response response, String onField, String insertField) throws ResponseNullPointer {
    return joinOn(withField, response, onField, insertField, true);
  }

  /**
   * join this response with response parameter using the withField field name on the current
   * response and the onField field from the response passed in as a parameter (Basically, merge
   * entries from the two responses when the values of the withField and onField match.
   * @param withField
   * @param response
   * @return
   * @throws ResponseNullPointer
   */
  public Response joinOn(String withField, Response response, String onField) throws ResponseNullPointer {
    return joinOn(withField, response, onField, null);
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

  public void populateRollBackError(JsonObject rbError){
    if(error == null){
      //should not be null since roll back called when there is an error
      //but just in case
      error = new JsonObject();
    }
    error.put("rbEndpoint", rbError.getString("endpoint"));
    error.put("rbStatusCode", rbError.getInteger("statusCode"));
    error.put("rbErrorMessage", rbError.getString("errorMessage"));
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
