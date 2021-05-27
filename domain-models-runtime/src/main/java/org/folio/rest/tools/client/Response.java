package org.folio.rest.tools.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.rest.tools.client.exceptions.ResponseNullPointer;
import org.folio.rest.tools.parser.JsonPathParser;
import org.folio.rest.tools.parser.JsonPathParser.Pairs;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Http client response handling.
 * @deprecated All material in org.folio.rest.tools.client is deprecated.
 */
@Deprecated
public class Response {

  private static final ObjectMapper MAPPER = ObjectMapperTool.getMapper();

  String endpoint;
  int code;
  JsonObject body;
  JsonObject error;
  Throwable exception;
  MultiMap headers;

  public Response mapFrom(Response response1, String extractField, String intoField, boolean allowNulls)
      throws ResponseNullPointer {
    return mapFrom(response1, extractField, intoField, null, allowNulls);
  }

  /**
   * pull a specific field / section from response1 and populate this response with that section
   * @param response1
   * @param extractField
   * @param intoField
   * @param allowNulls
   * @return
   */
  public Response mapFrom(Response response1, String extractField, String intoField, String nameOfInsert, boolean allowNulls)
      throws ResponseNullPointer {
    if(response1 == null || response1.body == null){
      throw new ResponseNullPointer();
    }
    if(this.body == null){
      this.body = new JsonObject();
    }
    JsonPathParser jpp = new JsonPathParser(response1.body);
    //get list of paths within the json to join on
    List<StringBuilder> sbList = jpp.getAbsolutePaths(extractField);
    if(sbList == null){
      //path does not exist in the json, nothing to join on, return response
      return this;
    }
    JsonPathParser result = new JsonPathParser(this.body);
    int size = sbList.size();
    boolean isArray = false;
    if(extractField.contains("[*]")){
      //probably the contains is enough and no need to check size
      isArray = true;
    }
    JsonObject ret = new JsonObject();
    for (int i = 0; i < size; i++) {
      Object val = jpp.getValueAt(sbList.get(i));
      if(val == null && !allowNulls){
        continue;
      }
      String fixedPath = intoField;
      if(isArray){
        fixedPath = fixedPath +"["+i+"]";
      }
      result.setValueAt(fixedPath, val, nameOfInsert);
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
   * @param extractField - the field to extract from the response to join on. Two options:
   * 1. an absolute path - such as a.b.c or a.b.c.d[0] - should be used when one field needs to be
   * extracted
   * 2. a relative path - such as ../../abc - should be used in cases where the join on is an array
   * of results, and we want to extract the specific field for each item in the array to push into the
   * response we are joining with.
   * @param intoField - the field in the current Response's json to merge into
   * @param allowNulls
   * @return
   * @throws ResponseNullPointer
   */
  public Response joinOn(Response response1, String withField, Response response2, String onField, String extractField,
      String intoField, boolean allowNulls) throws ResponseNullPointer {
    if((this.body == null && response1 == null) || response2 == null || response2.body == null){
      throw new ResponseNullPointer();
    }
    JsonPathParser output = null;
    JsonObject input = null;

    if(response1 != null && response1.body != null){
      input = response1.body;
      if(this.body == null){
        this.body = new JsonObject();
      }
      output =  new JsonPathParser(this.body);
    }
    else{
      input = this.body;
    }
    JsonPathParser jpp = new JsonPathParser(response2.body);
    //get list of paths within the json to join on
    List<StringBuilder> sbList = jpp.getAbsolutePaths(onField);
    if(sbList == null){
      //path does not exist in the json, nothing to join on, return response
      return this;
    }
    Multimap<Object, ArrayList<Object>> joinTable = ArrayListMultimap.create();
    //Map<Object, Object> joinTable = new HashMap<>();
    int size = sbList.size();
    for (int i = 0; i < size; i++) {
      Pairs map = jpp.getValueAndParentPair(sbList.get(i));
      if(map != null && map.getRequestedValue() != null){
        ArrayList<Object> a = new ArrayList<>();
        a.add(sbList.get(i));
        a.add(map.getRootNode());
        joinTable.put(map.getRequestedValue(), a);
      }
    }
    jpp = new JsonPathParser(input);
    List<StringBuilder> list = jpp.getAbsolutePaths(withField);
    int size2 = 0;
    if(list != null){
      //if withField path does not exist in json the list will be null
      size2 = list.size();
    }
    for (int i = 0; i < size2; i++) {
      //get object for each requested absolute path (withField) needed to compare with the
      //join table values
      Object valueAtPath = jpp.getValueAt(list.get(i).toString());
      //check if the value at the requested path also exists in the join table.
      //if so, get the corresponding object from the join table that will replace a value
      //in the current json
      Collection<ArrayList<Object>> o = joinTable.get(valueAtPath);
      if(o != null && o.size() > 0){
        //there is a match between the two jsons, can either be a single match or a match to multiple
        //matches
        Object toInsert = null;
        int arrayPlacement = 0;
        //get the value from the join table object to use as the replacement
        //this can be the entire object, or a value found at a path within the object aka insertField
        if(o.size() == 1 && extractField != null){
          String tempEField = extractField;
          ArrayList<Object> b = o.iterator().next();
          if(extractField.contains("../")){
            tempEField = backTrack(b.get(0).toString(), extractField);
          }
          toInsert = new JsonPathParser((JsonObject)b.get(1)).getValueAt(tempEField);
        }
        else if(o.size() > 1 && extractField != null){
          //more then one of the same value mapped to different objects, create a jsonarray
          //with all the values and insert the jsonarray
          toInsert = new JsonArray();
          Iterator<?> it = o.iterator();
          while(it.hasNext()){
            String tempEField = extractField;
            ArrayList<Object> b = (ArrayList)it.next();
            if(extractField.contains("../")){
              tempEField = backTrack(b.get(0).toString(), extractField);
            }
            Object object = new JsonPathParser((JsonObject)b.get(1)).getValueAt(tempEField);
            if(object != null){
              ((JsonArray)toInsert).add(object);
            }
          }
        }
        else {
          toInsert = o.iterator().next();
        }
        if(!allowNulls && toInsert == null){
          continue;
        }
        if(intoField != null){
          //get the path in the json to replace with the value from the join table
          Object into = null;
          String tempIntoField = intoField;
          if(output != null){
            Pairs p = output.getValueAndParentPair(list.get(i));
            if(p != null){
              //there is an existing value in this jsons path
              into = p.getRootNode();
            } else {
              output.setValueAt(tempIntoField, toInsert);
            }
          }else{
            into = jpp.getValueAndParentPair(list.get(i)).getRootNode();
          }
          if(intoField.contains("../")){
            tempIntoField = backTrack(list.get(i).toString(), intoField);
          }
          JsonPathParser jpp2 = new JsonPathParser((JsonObject)into);
          Object placeWhereReplacementShouldHappen = jpp2.getValueAt(tempIntoField);
          if(placeWhereReplacementShouldHappen instanceof JsonArray){
            ((JsonArray)placeWhereReplacementShouldHappen).add(toInsert);
          }
          else{
            if(output != null){
              output.setValueAt(tempIntoField, toInsert);
            }
            else{
              jpp2.setValueAt(tempIntoField, toInsert);
            }
          }
        }
        else{
          if(output != null){
            //update new json
            output.setValueAt(list.get(i).toString(), toInsert);
          }
          else{
            //update this.body
            jpp.setValueAt(list.get(i).toString(), toInsert);
          }
        }
      }
      else{
        if(allowNulls){
          if(intoField != null){
            String tempIntoField = intoField;
            if(intoField.contains("../")){
              tempIntoField = backTrack(list.get(i).toString(), intoField);
            }
            jpp.setValueAt(tempIntoField, null);
          }
          else{
            jpp.setValueAt(list.get(i).toString(), null);
          }
        }
      }
    }

    if(output != null){
      Response r = new Response();
      r.setBody(output.getJsonObject());
      return r;
    }
    return this;
  }

  private String backTrack(String path, String backtrack){
    String a[] = path.split("\\.");
    int backTrackCount = backtrack.split("../").length-1;
    if(backTrackCount == -1){
      //in case ../ was passed in, the split returns []
      backTrackCount = 1;
    }
    int removeFrom = (a.length-backTrackCount);
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < removeFrom; i++) {
      sb.append(a[i]).append(".");
    }
    int i = backtrack.lastIndexOf("../");
    sb.append(backtrack.substring(i+3));
    return sb.toString();
  }

  public Response joinOn(String withField, Response response2, String onField, String insertField, String intoField, boolean allowNulls) throws ResponseNullPointer {
    return joinOn(null, withField, response2, onField, insertField, intoField, allowNulls);
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
    return joinOn(null, withField, response, onField, insertField, null, allowNulls);
  }

  public Response joinOn(Response response1, String withField, Response response2, String onField, String insertField, boolean allowNulls) throws ResponseNullPointer {
    return joinOn(response1, withField, response2, onField, insertField, null, allowNulls);
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

  public Response joinOn(Response response1, String withField, Response response2, String onField, String insertField) throws ResponseNullPointer {
    return joinOn(response1, withField, response2, onField, insertField, null, true);
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

  public Response joinOn(Response response1, String withField, Response response2, String onField) throws ResponseNullPointer {
    return joinOn(response1, withField, response2, onField, null, null, true);
  }

  public static boolean isSuccess(int statusCode){
    if(statusCode >= 200 && statusCode < 300){
      return true;
    }
    return false;
  }

  public Object convertToPojo(Class<?> type) throws Exception {
    return MAPPER.readValue(body.encode(), type) ;
  }

  public static Object convertToPojo(JsonObject j, Class<?> type) throws Exception {
    return MAPPER.readValue(j.encode(), type) ;
  }

  public static Object convertToPojo(JsonArray j, Class<?> type) throws Exception {
    return MAPPER.readValue(j.encode(), MAPPER.getTypeFactory().constructCollectionType(List.class, type));
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

  public MultiMap getHeaders() {
    return headers;
  }

  public void setHeaders(MultiMap headers) {
    this.headers = headers;
  }

}
