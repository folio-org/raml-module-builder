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
  public Object getValueAt(String path, boolean returnParent){
    String []subPathsList = getPathasList(path);
    Object o = j;
    if(subPathsList.length == 1){
      return j.getValue(subPathsList[0]);
    }
    else{
      for (int i = 0; i < subPathsList.length; i++) {
        if("[*]".equals(subPathsList[i])){
          List<Object> l = new ArrayList<>();
          if(o instanceof List && o != null){
            int s = ((List)o).size();
            for(int j2=0; j2<s; j2++){
              Object o1 = ((List<Object>)o).get(j2);
              if(o1 != null){
              int s1 = ((JsonArray)o1).size();
                for(int j1=0; j1<s1; j1++){
                  l.add(getValueAt("["+j1+"]", o1, null));
                }
              }
            }
          }
          else{
            int s = ((JsonArray)o).size();
            for(int j=0; j<s; j++){
              l.add(getValueAt("["+j+"]", o, null));
            }
          }
          o = l;
        }
        else{
          Map<Object, Object> rParent = null;
          if(returnParent && i==subPathsList.length-1){
            rParent = new HashMap<>();
          }
          o = getValueAt(subPathsList[i], o, rParent);
        }
      }
    }
    return o;
  }

  public Object getValueParentPairs(String path){
    return getValueAt(path, true);
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
        temp.set(i, temp.get(i));
      }
      return temp;
    }
    return null;
  }

  public void setValueAt(String path, Object value){

    String []subPathsList = getPathasList(path);
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

  private String[] getPathasList(String path){
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

      Object o = jp.getValueParentPairs("c.arr[*].a2.'aaa.ccc'");

      Object o1 = jp.getValueParentPairs("c.arr[0].a2.'aaa.ccc'");

      Object o2 = jp.getValueParentPairs("c.a1");

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
