package org.folio.rest.tools.parser;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Based on the vertx json object implementation which is mapped based.
 * <p>
 * Allow setting / getting nested paths.
 */
public class JsonPathParser {

  JsonObject j;
  boolean isSchema = false;

  public JsonPathParser(JsonObject j, boolean isSchema){
    this.j = j;
    this.isSchema = isSchema;
  }

  public JsonPathParser(JsonObject j){
    this.j = j;
  }

  /**
   * Function may return a value / jsonobject / jsonarray / null / List - depending on the path
   * passed in and the value of the path requested
   *
   * @param path
   * <br>
   * Path can be
   * <br>
   * a.b -> get value of field 'b' which is nested within a jsonobject called 'a'
   * <br>
   * a.c[1].d
   * <br>
   * a.c -> get json array c
   * <br>
   * a.'bb.cc' -> get field called bb.cc - use '' when '.' in name
   * <br>
   * a.c[*].a2 -> get all a2 values as a <b>List</b> for each entry in the c array
   *  <br>
   *  For example:
   *    <br>passing in a usergroups[*].id will return a List of the ids (a wildcard in the path will result
   *    in a returned List of values.
   * @return either a jsonobject / jsonarray when a specific value is requested or a List when a
   * wildcard is included in the path
   */
  public Object getValueAt(String path){
    return getValueAt(path, false, false);
  }

  /**
   * Function may return a value / jsonobject / jsonarray / null / List - depending on the path
   * passed in and the value of the path requested
   * @param path
   * <br>
   * Path can be
   * <br>
   * a.b -> get value of field 'b' which is nested within a jsonobject called 'a'
   * <br>
   * a.c[1].d - get 'd' which appears in array c[1]
   * <br>
   * a.c -> get json array c
   * <br>
   * a.'bb.cc' -> get field called bb.cc - use '' when '.' in name
   * <br>
   * a.c[*].a2 -> get all a2 values as a <b>List</b> for each entry in the c array
   *  <br>
   *  For example:
   *    <br>passing in a usergroups[*].id will return a List of the ids (a wildcard in the path will result
   *    in a returned List of values.
   * @return either a jsonobject / jsonarray when a specific value is requested or a List when a
   * wildcard is included in the path
   */
  public Object getValueAt(StringBuilder path){
    return getValueAt(path.toString(), false, false);
  }

  /**
   * can receive a path with wildcards
   * for example:
   * <br>
   * c.arr[*].a2.arr2[*]
   * <br>
   * and will return a list of all existing paths conforming to this virtual path, for example
   * c.arr[0].a2.arr2[0]
   * c.arr[1].a2.arr2[0]
   * c.arr[2].a2.arr2[0]
   * c.arr[0].a2.arr2[1]
   * c.arr[0].a2.arr2[2]
   * <br>
   * Paths within the virtual path that do not contain entries will not be returned
   * <br>
   * ...
   * c.arr[*].a2
   * ...
   * @param path
   * @return
   */
  @SuppressWarnings("unchecked")
  public List<StringBuilder> getAbsolutePaths(String path){
    return (List<StringBuilder>)getValueAt(path, false, true);
  }

  /**
   * @param path
   * <br>
   * Path can be
   * <br>
   * a.b -> get value of field 'b' which is nested within a jsonobject called 'a'
   * <br>
   * a.c[1].d
   * <br>
   * a.c -> get json array c
   * <br>
   * a.'bb.cc' -> get field called bb.cc - use '' when '.' in name
   * <br>
   * a.c[*].a2 -> get all a2 values as a List for each entry in the c array
   * @param returnParent - can return the value of the direct parent
   *  <br>
   *  For example:
   *    <br>passing in a usergroups[*].id will return a List of the ids (a wildcard in the path will result
   *    in a returned List of values.
   * @return either a jsonobject / jsonarray when a specific value is requested or a List when a
   * wildcard is included in the path
   */
  Object getValueAt(String path, boolean returnParent, boolean returnPaths){
    String []subPathsList = getPathAsList(path);
    Object o = j;
    List<StringBuilder> currentPaths = new ArrayList<>();
    Pairs rParent = new Pairs();
    boolean returnRoot = false;
      StringBuilder sb = new StringBuilder();
      currentPaths.add(sb);
      for (int i = 0; i < subPathsList.length; i++) {
        if(i>1 && rParent == null){
          //if this was a list, then the rParent was reset, set it to a new Pairs so that it is
          //re-populated with the json object from the list and not the list itself
          rParent = new Pairs();
        }
        if("[*]".equals(subPathsList[i])){
          List<Object> l = new ArrayList<>();
          if(o instanceof List && o != null){
            List<StringBuilder> prefixes = currentPaths;
            currentPaths = new ArrayList<>();
            int s = ((List)o).size();
            int fix = 0;
            for(int j2=0; j2<s; j2++){
              //loop over all json array entries, if the requested field was not found in the in
              //the json array, there will be a null value in that index in the list below
              Object o1 = ((List<Object>)o).get(j2);
              if(o1 != null){
              int s1 = ((JsonArray)o1).size();
                for(int j1=0; j1<s1; j1++){
                  ////////////////////////////
                  StringBuilder sb1 = new StringBuilder(prefixes.get(j2)).append("["+j1+"]");
                  currentPaths.add(sb1);
                  ////////////////////////////
                  l.add(getValueAt("["+j1+"]", o1, null, returnRoot));
                }
                ///////////////
                //currentPaths = currentPaths1;
                //////////////
              }
            }
          }
          else{
            if(o==null){
              return null;
            }
            int s = ((JsonArray)o).size();
            for(int j=0; j<s; j++){
              StringBuilder sb1 = new StringBuilder(sb).append("["+j+"]");
              currentPaths.add(sb1);
              l.add(getValueAt("["+j+"]", o, null, returnRoot));
            }
            currentPaths.remove(sb);
          }
          o = l;
        }
        else{
          for (int j1 = 0; j1 < currentPaths.size(); j1++) {
            if(i>0 && i<subPathsList.length){
              if(!subPathsList[i].matches("\\[.*?\\]")){
                currentPaths.get(j1).append(".");
              }
              //else{
                //if(i==1){
                  //if [*] is the second or 3rd value - this means the root node requested was something like
                  //a[*] - which means the request is for a list of objects - in such a case
                  //set the root node to each object and not to the list itself. so reset the  rParent
                  //object to null - it will be repopulated in the next iteration.
                  //rParent = null;
                //}
             // }
            }
            currentPaths.get(j1).append(wrapIfNeeded(subPathsList[i]));
          }
          if(i==subPathsList.length-1){
            if(returnParent){
              returnRoot = true;
            }
            if(o instanceof List){
              int s = currentPaths.size()-1;
              for(int j1 =s ; j1 >= 0 ; j1--){
                if(((List<?>)o).get(j1) == null){
                  currentPaths.remove(j1);
                  s--;
                }
              }
            }
          }
          try {
            o = getValueAt(subPathsList[i], o, rParent, returnRoot);
          } catch (Exception e) {
            return null;
          }

        }
      }
    if(returnPaths){
      return currentPaths;
    }
    return o;
  }

  private String wrapIfNeeded(String str){
    if(str.contains(".")){
      return "'"+str+"'";
    }
    return str;
  }

  /**
   * Should be used with a path to a single value - no wildcards (ex [*] should be used)
   * Will return the value of the passed in path and the json object / json array where the path
   * is nested in.
   * <br>
   * For example:
   * {"a": { "b" : "c" , "d" : "e"}}
   * <br>
   * Passing in <b>a.d</b> as the path will return a map of
   * e={ "b" : "c" , "d" : "e"}
   * <br>
   * Where 'e' is the value found at the requested path, and { "b" : "c" , "d" : "e"} is the
   * object containing the requested path
   * <br>
   * If an array is requested for example: a.b[0] - then a json object will be returned containing
   * the content of the array only (will behave like getValueAt())
   * @param path
   * @return
   */
  public Pairs getValueAndParentPair(String path){
    return ( Pairs )getValueAt(path, true, false);
  }

  public Pairs getValueAndParentPair(StringBuilder path){
    return ( Pairs )getValueAt(path.toString(), true, false);
  }

  private Object getValueAt(String path, Object o, Pairs parent, boolean returnRoot){
    if(o instanceof JsonObject){
      JsonObject props = ((JsonObject) o).getJsonObject("properties");
      if(props != null && isSchema){
        return getValueAt(path, props, parent, returnRoot);
      }
      if(parent != null){
        if(parent.rootNode == null){
          //dirty, but this checks to make sure we only put the root node into the parent map
          parent.rootNode = o;
        }
      }
      if(returnRoot && parent != null){
        parent.requestedValue = ((JsonObject) o).getValue(path);
        return parent;
      }
      else{
        return ((JsonObject) o).getValue(path);
      }
    }
    else if(o instanceof JsonArray){
      Object o1 =
          ((JsonArray)o).getValue(Integer.parseInt(
            path.substring(1, path.length()-1))/*remove the []*/);
      if(parent != null){
        if(parent.rootNode == null){
          parent.rootNode = o;
        }
      }
      if (returnRoot && parent != null) {
        parent.requestedValue = o1;
        return parent;
      }
      else{
        return o1;
      }
    }
    else if(o instanceof List){
      @SuppressWarnings("unchecked")
      List<Object> temp = (List<Object>)o;
      int s = temp.size();
      for (int i = 0; i < s; i++) {
        if(temp.get(i) instanceof JsonObject){
          Object obj = ((JsonObject) temp.get(i)).getValue(path);
          if(parent != null){
            if(parent.rootNode == null){
              parent.rootNode = o;
            }
            if(returnRoot){
              parent.requestedValue = ((JsonObject) o).getValue(path);
              return parent;
            }
          }
          temp.set(i, obj);
        }
        else if(temp.get(i) instanceof JsonArray){
          Object obj = ((JsonArray)temp.get(i)).getValue(Integer.parseInt(
            path.substring(1, path.length()-1)));
          if(parent != null){
            if(parent.rootNode == null){
              parent.rootNode = o;
            }
            if(returnRoot){
              parent.requestedValue = ((JsonObject) o).getValue(path);
              return parent;
            }
          }
          temp.set(i, obj/*removed the []*/);
        }
        //temp.set(i, temp.get(i));
      }
      return temp;
    }
    return null;
  }

  public void setValueAt(String path, Object value){
    setValueAt(path, value, null);
  }

  /**
   * Sets a value to the element at the specificed path
   * wildcards are not allowed in the path - for example [*] is NOT allowed
   * @param path
   * @param value
   */
  @SuppressWarnings("unchecked")
  public void setValueAt(String path, Object value, String nameForNewArray){

    String []subPathsList = getPathAsList(path);
    Object o = j;
    if(subPathsList.length == 1){
      j.put(path, value);
    }
    else{
      for (int i = 0; i < subPathsList.length-1; i++) {
        Object parent = o;
        boolean isArray = false;
        o = getValueAt(subPathsList[i], parent, null, false);
        if(o == null){
          String fieldName = subPathsList[i];
          if(subPathsList[i+1].matches(".*\\[.*?\\].*")){
            fieldName = subPathsList[i];
            //create an array
            o = new JsonArray();
            isArray = true;
          }
          else{
            //create json object
            o = new JsonObject();
          }
          if(parent instanceof JsonArray){
            ((JsonArray)parent).add(o);
          }
          else{
            ((JsonObject)parent).put(fieldName, o);
/*            if(isArray){
              JsonObject j = new JsonObject();
              j.put(fieldName, o);
              ((JsonObject)parent).put(fieldName, o);
            }
            if(nameForNewArray == null || i < subPathsList.length-2){
              ((JsonObject)parent).put(fieldName, o);
            }
            else if(nameForNewArray != null){
              JsonObject j = new JsonObject();
              ((JsonObject)parent).put(fieldName, j);
              j.put(nameForNewArray, o);
            }*/
          }

        }
      }
    }
    if(o instanceof JsonObject){
      ((JsonObject) o).put(subPathsList[subPathsList.length-1], value);
    }
    else if(o instanceof JsonArray){
      String pos = "-1";
      try {
        pos = subPathsList[subPathsList.length-1].substring(
          1, subPathsList[subPathsList.length-1].length()-1);
        ((JsonArray)o).getList().set(Integer.parseInt(pos), value/*remove the []*/);
      } catch (NumberFormatException e) {
        e.getMessage(); /* do nothing */
      }
      catch(IndexOutOfBoundsException ioob){
        //tried setting a value in the jsonarray that has no values
        //assume this is because the path doesnt exist and add
        JsonObject j = new JsonObject();
        if(nameForNewArray != null){
          j.put(nameForNewArray, value);
          ((JsonArray)o).getList().add(j);
        }
        else{
          ((JsonArray)o).getList().add(value);
        }
      }
    }
  }

  protected String[] getPathAsList(String path){
    int len = path.length();
    List<String> res = new ArrayList<>();
    StringBuilder token = new StringBuilder();
    boolean inQuotes = false;
    boolean inParen = false;

    for(int j=0; j< len; j++){
      char t = path.charAt(j);
      if(t=='\''){
        inQuotes = !inQuotes;
        //token.append(t);
        if(!inQuotes){
          //finished quotes
          res.add(token.toString());
          token = new StringBuilder();
        }
      }
      else if(t=='['){
        inParen = !inParen;
        res.add(token.toString());
        token = new StringBuilder();
        token.append(t);
      }
      else if(t==']'){
        token.append(t);
        res.add(token.toString());
        token = new StringBuilder();
        inParen = !inParen;
        continue;
      }
      else if(t=='.'){
        if(!inQuotes){
          if(token.length()>0){
            res.add(token.toString());
            token = new StringBuilder();
            continue; //dont add the '.' to any token
          }
        }
        else{
          token.append(t);
        }
      }
      else{
        token.append(t);
      }
    }
    if(token.length() > 0){
      res.add(token.toString());
    }
    return res.toArray(new String[0]);
  }

  public class Pairs {
    Object rootNode;
    Object requestedValue;
    public Object getRootNode() {
      return rootNode;
    }
    public void setRootNode(Object rootNode) {
      this.rootNode = rootNode;
    }
    public Object getRequestedValue() {
      return requestedValue;
    }
    public void setRequestedValue(Object requestedValue) {
      this.requestedValue = requestedValue;
    }
  }

  public JsonObject getJsonObject(){
    return j;
  }
}
