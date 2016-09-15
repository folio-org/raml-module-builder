package org.folio.rest;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import io.vertx.core.json.JsonObject;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class MappedClasses {

  //store data as a triplet -
  //url path - http method <- these two are the look up key
  //jsonobject is the function mapped to the key pair - the
  //jsonobject also includes all params to the function and their type
  private Table<String, String, JsonObject> path2method = null;

  public MappedClasses() {
    this.path2method = HashBasedTable.create();
  }

  public void addPath(String path, JsonObject obj) {
    String method = obj.getString("method");
    method = method.substring(method.lastIndexOf(".") + 1);
    path2method.put(path, method, obj);
  }

  public JsonObject getMethodbyPath(String path, String method) {
    return path2method.get(path, method);
  }

  public Table<String, String, JsonObject> getPath2method() {
    return path2method;
  }

  public Set<String> getAvailURLs() {
    return path2method.rowKeySet();
  }

  public Map<String, Pattern> buildURLRegex() {
    Map<String, Pattern> regex2Pattern = new HashMap<String, Pattern>();
    // available url paths in the api
    Set<String> urlPaths = path2method.rowKeySet();
    Iterator<String> urlPathsIter = urlPaths.iterator();
    // pre-compile the regex expression keys to regex patterns to match against
    while (urlPathsIter.hasNext()) {
      String regexURL = urlPathsIter.next();
      regex2Pattern.put(regexURL, Pattern.compile(regexURL));
    }
    return regex2Pattern;
  }

}
