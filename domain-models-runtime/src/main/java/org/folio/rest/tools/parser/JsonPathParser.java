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
    return getValueAt(path, false);
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
   *    <br>passing in a usergroups[*].id will return a List of the ids
   *    <br>passing in a (usergroups[*].id , true) will return a List of the json object containing
   *    the id field - this is only relevant when a specific field is requested in the path -
   *    if the path ends with [1] or [*] - this value is ignored
   * @return either a jsonobject / jsonarray / List
   */
  private Object getValueAt(String path, boolean returnParent){
    String []subPathsList = getPathAsList(path);
    Object o = j;
    List<StringBuilder> currentPaths = new ArrayList<>();
    Map<Object, Object> rParent = null;

    if(subPathsList.length == 1){
      return j.getValue(subPathsList[0]);
    }
    else{
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
              currentPaths.get(j1).append(".");
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
          o = getValueAt(subPathsList[i], o, rParent);

        }
      }
    }
    System.out.println("currentPaths: " + currentPaths);
    return o;
  }

  private String wrapIfNeeded(String str){
    if(str.contains(".")){
      return "'"+str+"'";
    }
    return str;
  }
  /**
   * Should be used with a path to a single value - no wildcards
   * Will return the value of the passed in path and the json object / json array which
   * holds the passed in path
   * for example:
   * {"a": { "b" : "c" , "d" : "e"}}
   * passing in a.d will return a map of
   * e={ "b" : "c" , "d" : "e"}
   * @param path
   * @return
   */
  public Object getValueParentPairs(String path){
    return getValueAt(path, true);
  }

  public Object getPath2ValuePairs(String path){
    return getValueAt(path, true);
  }

  private String calculateCurrentPath(StringBuilder previousPath, String []subPathsList,
      int currentField, int currentArrayIndex){
    if("[*]".equals(subPathsList[currentField])){
      previousPath.append("[").append(currentArrayIndex).append("]");
    }
    else{
      if(currentField>0 && currentField<subPathsList.length){
        previousPath.append(".");
      }
      previousPath.append(subPathsList[currentField]);
    }
    return previousPath.toString();
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
      return ((JsonArray)o).getValue(Integer.parseInt(
        path.substring(1, path.length()-1))/*remove the []*/);
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

  private class Pairs {
    Object object;
    String path;
    public Pairs(Object object, String path) {
      super();
      this.object = object;
      this.path = path;
    }
  }


  public static void main(String args[]) throws IOException{

    //for (int i = 0; i < 3; i++) {
      long start = System.currentTimeMillis();

      JsonObject j1 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("pathTest.json"), "UTF-8"));
      JsonPathParser jp = new JsonPathParser(j1);

      Object o = jp.getValueParentPairs("c.arr[*].a2.'aaa.ccc'"); //fix

      Object o3a = jp.getValueParentPairs("c.arr[*].a2.arr2[*]");

      Object o3 = jp.getValueParentPairs("c.arr[*].a2.arr2[*].arr3[*]");

      Object o11 = jp.getValueParentPairs("c.arr[*]");

      Object o1 = jp.getValueParentPairs("c.arr[0].a2.'aaa.ccc'");

      Object o2 = jp.getValueParentPairs("c.a1");

      Object o4 = jp.getValueParentPairs("c.arr[*].a2.arr2[*].arr3[*].a32");

      System.out.println("c.arr[*].a2.'aaa.ccc': (with parent) " + jp.getValueAt("c.arr[*].a2.'aaa.ccc'", true));

      System.out.println("c.arr[*].a2: (with parent) " + jp.getValueAt("c.arr[*].a2", true));

      System.out.println("c.arr[0].a2: (with parent) " + jp.getValueAt("c.arr[0].a2", true));

      System.out.println("c.arr[*].a2.arr2[*].arr3[*]: " + jp.getValueAt("c.arr[*].a2.arr2[*].arr3[*]"));

      System.out.println("c.arr[*].a2.arr2[*].arr3[*]: " + jp.getValueAt("c.arr[*].a2.arr2[*].arr3[*]"));

      System.out.println("c.arr[*].a2.'aaa.ccc': " + jp.getValueAt("c.arr[*].a2.'aaa.ccc'"));
      jp.setValueAt("c.arr[1].a2.'aaa.ccc'", "aaa.ccc");
      System.out.println("c.arr[*].a2.'aaa.ccc': " +jp.getValueAt("c.arr[*].a2.'aaa.ccc'"));

      System.out.println("c.arr[*].a2.arr2[*]: " + jp.getValueAt("c.arr[*].a2.arr2[*]"));
      jp.setValueAt("c.arr[0].a2.arr2[0]", "yyy");
      System.out.println("c.arr[*].a2.'aaa.ccc': " +jp.getValueAt("c.arr[*].a2.arr2[*]"));

      System.out.println("c.arr[1].a2.'aaa.ccc': " + jp.getValueAt("c.arr[1].a2.'aaa.ccc'"));
      jp.setValueAt("c.arr[1].a2.'aaa.ccc'", "aaa.ddd");
      System.out.println("c.arr[1].a2.'aaa.ccc': " +jp.getValueAt("c.arr[1].a2.'aaa.ccc'"));

      System.out.println("c.arr[1].a2 " + jp.getValueAt("c.arr[1].a2"));
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
