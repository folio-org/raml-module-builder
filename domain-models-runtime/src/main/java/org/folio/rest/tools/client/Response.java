package org.folio.rest.tools.client;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.folio.rest.tools.parser.JsonPathParser;
import org.folio.rest.tools.parser.JsonPathParser.Pairs;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author shale
 *
 */
public class Response {

  String endpoint;
  int code;
  JsonObject body;
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
   * @param insertField - the path should be relative to the object itself. so if an array of
   * json objects is returned the path should will be evaluated on each json object and not on the
   * array as a whole - this is unlike the withField and the onField which refer to the array of
   * results as a whole so that if an array of results are returned withField and onField will
   * need to indicate something along the lines of a[*].field , while insertField will refer to
   * 'field' only without the a[*]
   * @param intoField - the field in the current Response's json to merge into
   * @param allowNulls
   * @return
   * @throws ResponseNullPointer
   */
  public Response joinOn(String withField, Response response, String onField, String insertField,
      String intoField, boolean allowNulls) throws ResponseNullPointer {
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
    Multimap<Object, Object> joinTable = ArrayListMultimap.create();
    //Map<Object, Object> joinTable = new HashMap<>();
    int size = sbList.size();
    for (int i = 0; i < size; i++) {
      Pairs map = jpp.getValueAndParentPair(sbList.get(i));
      if(map != null){
        joinTable.put(map.getRequestedValue(), map.getRootNode());
      }
    }
    jpp = new JsonPathParser(this.body);
    List<StringBuilder> list = jpp.getAbsolutePaths(withField);
    int size2 = list.size();
    for (int i = 0; i < size2; i++) {
      //get object for each requested absolute path (withField) needed to compare with the
      //join table values
      Object valueAtPath = jpp.getValueAt(list.get(i).toString());
      //check if the value at the requested path also exists in the join table.
      //if so, get the corresponding object from the join table that will replace a value
      //in the current json
      Collection<Object> o = joinTable.get(valueAtPath);
      if(o != null && o.size() > 0){
        Object toInsert = null;
        //get the value from the join table object to use as the replacement
        //this can be the entire object, or a value found at a path within the object aka insertField
        if(o.size() == 1 && insertField != null){
          toInsert = new JsonPathParser((JsonObject)o.iterator().next()).getValueAt(insertField);
        }
        else if(o.size() > 1 && insertField != null){
          //more then one of the same value mapped to different objects, create a jsonarray
          //with all the values and insert the jsonarray
          toInsert = new JsonArray();
          Iterator<?> it = o.iterator();
          while(it.hasNext()){
            Object object = new JsonPathParser((JsonObject)it.next()).getValueAt(insertField);
            if(object != null){
              ((JsonArray)toInsert).add(object);
            }
          }
        }
        else {
          toInsert = new JsonPathParser((JsonObject)o.iterator().next());
        }
        if(!allowNulls && toInsert == null){
          continue;
        }
        if(intoField != null){
          //get the path in the json to replace with the value from the join table
          Object into = jpp.getValueAndParentPair(list.get(i)).getRootNode();
          JsonPathParser jpp2 = new JsonPathParser((JsonObject)into);
          Object placeWhereReplacementShouldHappen = jpp2.getValueAt(intoField);
          if(placeWhereReplacementShouldHappen instanceof JsonArray){
            ((JsonArray)placeWhereReplacementShouldHappen).add(toInsert);
          }
          else{
            jpp2.setValueAt(intoField, toInsert);
          }
        }
        else{
          jpp.setValueAt(list.get(i).toString(), toInsert);
        }
      }
      else{
        if(allowNulls){
          jpp.setValueAt(list.get(i).toString(), null);
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
   * @param insertField - the path should be relative to the object itself. so if an array of
   * json objects is returned the path should will be evaluated on each json object and not on the
   * array as a whole - this is unlike the withField and the onField which refer to the array of
   * results as a whole so that if an array of results are returned withField and onField will
   * need to indicate something along the lines of a[*].field , while insertField will refer to
   * 'field' only without the a[*]
   * @param allowNulls - whether to place a null value into the json from the passed in response
   * json, in a case where there is no match in the join for a specific entry
   * @return
   * @throws ResponseNullPointer
   */
  public Response joinOn(String withField, Response response, String onField, String insertField, boolean allowNulls) throws ResponseNullPointer {
    return joinOn(withField, response, onField, insertField, null, allowNulls);
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
   * @param insertField - the path should be relative to the object itself. so if an array of
   * json objects is returned the path should will be evaluated on each json object and not on the
   * array as a whole - this is unlike the withField and the onField which refer to the array of
   * results as a whole so that if an array of results are returned withField and onField will
   * need to indicate something along the lines of a[*].field , while insertField will refer to
   * 'field' only without the a[*]
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
