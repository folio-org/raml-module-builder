package org.folio.rest.tools.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author shale
 * Based on the vertx json object implementation which is mapped based
 * allow setting / getting nested paths
 *
 */
public class JsonPathParser {

  JsonObject j;

  public JsonPathParser(JsonObject j){
    this.j = j;
  }

  public Object getValueAt(String path){
    return getValueAt(path, false, false);
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
   * @return either a jsonobject / jsonarray / List
   */
  private Object getValueAt(String path, boolean returnParent, boolean returnPaths){
    String []subPathsList = getPathAsList(path);
    Object o = j;
    List<StringBuilder> currentPaths = new ArrayList<>();
    Map<Object, Object> rParent = null;

      StringBuilder sb = new StringBuilder();
      currentPaths.add(sb);
      for (int i = 0; i < subPathsList.length; i++) {
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
              if(o1 == null){
                //currentPaths.remove(j2);
                //fix = 1;
              }
              else {
              int s1 = ((JsonArray)o1).size();
                for(int j1=0; j1<s1; j1++){
                  ////////////////////////////
                  StringBuilder sb1 = new StringBuilder(prefixes.get(j2)).append("["+j1+"]");
                  currentPaths.add(sb1);
                  ////////////////////////////
                  l.add(getValueAt("["+j1+"]", o1, null));
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
              l.add(getValueAt("["+j+"]", o, null));
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
            }
            currentPaths.get(j1).append(wrapIfNeeded(subPathsList[i]));
          }
          if(i==subPathsList.length-1){
            if(returnParent){
              rParent = new HashMap<>();
            }
            if(o instanceof List){
              int s = currentPaths.size()-1;
              for(int j1 =s ; j1 >= 0 ; j1--){
                if(((List)o).get(j1) == null){
                  currentPaths.remove(j1);
                  s--;
                }
              }
            }
          }
          try {
            o = getValueAt(subPathsList[i], o, rParent);
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
  public Map<Object, Object> getValueAndParentPair(String path){
    return ( Map<Object, Object> )getValueAt(path, true, false);
  }

  public  Map<Object, Object>  getValueAndParentPair(StringBuilder path){
    return ( Map<Object, Object> )getValueAt(path.toString(), true, false);
  }

  private Object getValueAt(String path, Object o, Map<Object, Object> parent){
    if(o instanceof JsonObject){
      if(parent != null){
        parent.put(((JsonObject) o).getValue(path), o);
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
        parent.put(o1, o);
        return parent;
      }
      else{
        return o1;
      }
    }
    else if(o instanceof List){
      List<Object> temp = (List<Object>)o;
      int s = temp.size();
      for (int i = 0; i < s; i++) {
        if(temp.get(i) instanceof JsonObject){
          Object obj = ((JsonObject) temp.get(i)).getValue(path);
          if(parent != null){
            parent.put(obj, o);
            return parent;
          }
          temp.set(i, obj);
        }
        else if(temp.get(i) instanceof JsonArray){
          Object obj = ((JsonArray)temp.get(i)).getValue(Integer.parseInt(
            path.substring(1, path.length()-1)));
          if(parent != null){
            parent.put(obj, o);
            return parent;
          }
          temp.set(i, obj/*removed the []*/);
        }
        //temp.set(i, temp.get(i));
      }
      return temp;
    }
    return null;
  }

  /**
   * Sets a value to the element at the specificed path
   * @param path
   * @param value
   */
  public void setValueAt(String path, Object value){

    String []subPathsList = getPathAsList(path);
    Object o = j;
    if(subPathsList.length == 1){
      j.put(path, value);
    }
    else{
      for (int i = 0; i < subPathsList.length-1; i++) {
        o = getValueAt(subPathsList[i], o, null);
      }
    }
    if(o instanceof JsonObject){
      ((JsonObject) o).put(subPathsList[subPathsList.length-1], value);
    }
    else if(o instanceof JsonArray){
      ((JsonArray)o).getList().set(Integer.parseInt(
        subPathsList[subPathsList.length-1].substring(
          1, subPathsList[subPathsList.length-1].length()-1)), value/*remove the []*/);
    }
  }

  private String[] getPathAsList(String path){
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

  public static void main(String args[]) throws IOException{

    //for (int i = 0; i < 3; i++) {
      long start = System.currentTimeMillis();

      JsonObject j1 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("pathTest.json"), "UTF-8"));
      JsonPathParser jp = new JsonPathParser(j1);

      List<StringBuilder> o5 = jp.getAbsolutePaths("c.arr[0].a2.'aaa.ccc'");

      List<StringBuilder> o6 = jp.getAbsolutePaths("c.arr[*].a2.'aaa.ccc'");

      List<StringBuilder> o7 = jp.getAbsolutePaths("c.arr[*].a2.arr2[*].arr3[*]");

      List<StringBuilder> o8 = jp.getAbsolutePaths("c.arr[*].a2.arr2[*].arr3[*].a32");

      Object o = jp.getValueAndParentPair("c.arr[0].a2.'aaa.ccc'"); //fix

      Object o3a = jp.getValueAndParentPair("c.arr[1].a2.arr2[1]");

      Object o3 = jp.getValueAndParentPair("c.arr[3].a2.arr2[2].arr3[1]");

      Object o11 = jp.getValueAndParentPair("c.arr[0]");

      Object o1 = jp.getValueAndParentPair("c.arr[0].a2.'aaa.ccc'");

      Object o2 = jp.getValueAndParentPair("c.a1");

      Object o4 = jp.getValueAndParentPair("c.arr[*].a2.arr2[*].arr3[*].a32");

      System.out.println("c.arr[*].a2.'aaa.ccc': (with parent) " + jp.getValueAt("c.arr[*].a2.'aaa.ccc'", true, false));

      System.out.println("c.arr[*].a2: (with parent) " + jp.getValueAt("c.arr[*].a2", true, false));

      System.out.println("c.arr[0].a2: (with parent) " + jp.getValueAt("c.arr[0].a2", true, false));

      System.out.println("c.arr[*].a2.arr2[*].arr3[*]: " + jp.getValueAt("c.arr[*].a2.arr2[*].arr3[*]"));

      System.out.println("c.arr[*].a2.arr2[*].arr3[*]: " + jp.getValueAt("c.arr[*].a2.arr2[*].arr3[*]"));

      System.out.println("c.arr[*].a2.'aaa.ccc': " + jp.getValueAt("c.arr[*].a2.'aaa.ccc'"));
      jp.setValueAt("c.arr[0].a2.'aaa.ccc'", "aaa.ccc");
      jp.setValueAt("c.arr[2].a2.'aaa.ccc'", "aaa.ddd");

      System.out.println("c.arr[*].a2.'aaa.ccc': " +jp.getValueAt("c.arr[*].a2.'aaa.ccc'"));

      System.out.println("c.arr[*].a2.arr2[*]: " + jp.getValueAt("c.arr[*].a2.arr2[*]"));
      jp.setValueAt("c.arr[0].a2.arr2[0]", "yyy");
      System.out.println("c.arr[*].a2.'aaa.ccc': " +jp.getValueAt("c.arr[*].a2.arr2[*]"));

      System.out.println("c.arr[1].a2.'aaa.ccc': " + jp.getValueAt("c.arr[1].a2.'aaa.ccc'"));
      jp.setValueAt("c.arr[1].a2.'aaa.ccc'", "aaa.ddd");
      System.out.println("c.arr[1].a2.'aaa.ccc': " +jp.getValueAt("c.arr[1].a2.'aaa.ccc'"));

      System.out.println("c.arr[1].a2 " + ((JsonObject)jp.getValueAt("c.arr[0].a2.arr2[2]")).getJsonArray("arr3"));
      jp.setValueAt("c.arr[1].a2", new JsonObject("{\"xqq\":\"xaa\"}"));
      System.out.println("c.arr[1].a2 " + jp.getValueAt("c.arr[1].a2"));

      System.out.println("c.arr[1] " + jp.getValueAt("c.arr[1]"));
      jp.setValueAt("c.arr[1]", new JsonArray("[{\"xqq\":\"xaa\"}]"));
      System.out.println("c.arr[1] " + jp.getValueAt("c.arr[1]"));

      System.out.println("c.arr " + jp.getValueAt("c.arr"));
      jp.setValueAt("c.arr", new JsonArray("[{\"xqq\":\"xaa\"}]"));
      System.out.println("c.arr " + jp.getValueAt("c.arr"));

      System.out.println("c " + jp.getValueAt("c"));
      jp.setValueAt("c", new JsonObject("{\"xz\":\"xc\"}"));
      System.out.println("c " + jp.getValueAt("c"));

      long end = System.currentTimeMillis();
      System.out.println("time: " + (end - start));

    //}
  }
}
