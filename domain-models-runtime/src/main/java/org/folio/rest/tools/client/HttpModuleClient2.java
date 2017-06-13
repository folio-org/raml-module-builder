package org.folio.rest.tools.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.folio.rest.tools.parser.JsonPathParser;
import org.folio.rest.tools.utils.VertxUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * @author shale
 *
 */
public class HttpModuleClient2 {

  private static final String CTYPE = "Content-Type";
  private static final String ACCEPT = "Accept";
  private static final String APP_JSON_CTYPE = "application/json";
  private static final String APP_JSON_ACCEPT = "application/json";
  private static final String X_OKAPI_HEADER = "x-okapi-tenant";
  private static final Pattern TAG_REGEX = Pattern.compile("\\{(.+?)\\}");

  private static final Logger log = LoggerFactory.getLogger(HttpModuleClient.class);

  private String tenantId;
  private HttpClientOptions options;
  private HttpClient httpClient;
  private LoadingCache<String, CompletableFuture<Response>> cache = null;
  private Vertx vertx;
  private boolean autoCloseConnections = true;
  private Map<String, String> headers = new HashMap<>();
  private long cacheTO = 30; //minutes
  private int connTO = 2000;
  private int idleTO = 5000;
  private boolean absoluteHostAddr = false;

  public HttpModuleClient2(String host, int port, String tenantId, boolean keepAlive, int connTO,
      int idleTO, boolean autoCloseConnections, long cacheTO) {

    this.tenantId = tenantId;
    this.cacheTO = cacheTO;
    this.connTO = connTO;
    this.idleTO = idleTO;
    options = new HttpClientOptions().setLogActivity(true).setKeepAlive(keepAlive)
        .setConnectTimeout(connTO).setIdleTimeout(idleTO);
    options.setDefaultHost(host);

    if(port == -1){
      absoluteHostAddr = true;
    }
    else{
      options.setDefaultPort(port);
    }
    this.autoCloseConnections = autoCloseConnections;
    vertx = VertxUtils.getVertxFromContextOrNew();
    setDefaultHeaders();
  }

  public HttpModuleClient2(String host, int port, String tenantId) {
    this(host, port, tenantId, true, 2000, 5000, true, 30);
  }

  /**
   *
   * @param absHost - ex. http://localhost:8081
   * @param tenantId
   */
  public HttpModuleClient2(String absHost, String tenantId) {
    this(absHost, -1, tenantId, true, 2000, 5000, true, 30);
  }

  public HttpModuleClient2(String host, int port, String tenantId, boolean autoCloseConnections) {
    this(host, port, tenantId, true, 2000, 5000, autoCloseConnections, 30);
  }

  /**
   *
   * @param absHost - ex. http://localhost:8081
   * @param tenantId
   * @param autoCloseConnections
   */
  public HttpModuleClient2(String absHost, String tenantId, boolean autoCloseConnections) {
    this(absHost, -1, tenantId, true, 2000, 5000, autoCloseConnections, 30);
  }

  private void setDefaultHeaders(){
    headers.put(X_OKAPI_HEADER, tenantId);
    headers.put(CTYPE, APP_JSON_CTYPE);
    headers.put(ACCEPT, APP_JSON_ACCEPT);
  }

  private void request(HttpMethod method, String endpoint, Map<String, String> headers,
      boolean cache, Handler<HttpClientResponse> responseHandler, CompletableFuture<Response> cf2){

    try {
      httpClient = vertx.createHttpClient(options);
      HttpClientRequest request = null;
      if(absoluteHostAddr){
        request = httpClient.requestAbs(method, options.getDefaultHost() + endpoint);
      } else {
        request = httpClient.request(method, endpoint);
      }
      request.exceptionHandler(error -> {
        Response r = new Response();
        r.populateError(endpoint, -1, error.getMessage());
        cf2.complete(r);
      })
      .handler(responseHandler);
      if(headers != null){
        this.headers.putAll(headers);
      }
      request.headers().setAll(this.headers);
      request.end();
    } catch (Exception e) {
      Response r = new Response();
      r.populateError(endpoint, -1, e.getMessage());
      r.exception = e;
      cf2.complete(r);
    }
  }

  public CompletableFuture<Response> request(HttpMethod method, String endpoint, Map<String, String> headers, RollBackURL rollbackURL,
      boolean cachable, BuildCQL bCql) throws Exception {

    if(bCql != null){
      endpoint = endpoint + bCql.buildCQL();
    }
    if(cachable){
      initCache();
      CompletableFuture<Response> j = cache.get(endpoint);
      if(j != null && !j.isCompletedExceptionally() && j.isDone()){
        return j;
      }
    }
    CompletableFuture<Response> cf = new CompletableFuture<>();
    HTTPJsonResponseHandler handler = new HTTPJsonResponseHandler(endpoint, cf);
    if(cachable) {
      handler.cache = cache;
    }
    if(autoCloseConnections){
      handler.httpClient = httpClient;
    }
    if(rollbackURL != null){
      handler.rollbackURL = rollbackURL;
    }

    request(method, endpoint, headers, cachable, handler, cf);

    return cf;
  }

  public CompletableFuture<Response> request(String endpoint, Map<String, String> headers, boolean cache, BuildCQL cql)
      throws Exception {
    return request(HttpMethod.GET, endpoint, headers, null, cache, cql);
  }

  public CompletableFuture<Response> request(String endpoint, Map<String, String> headers, boolean cache)
      throws Exception {
    return request(HttpMethod.GET, endpoint, headers, null, cache, null);
  }

  public CompletableFuture<Response> request(String endpoint, Map<String, String> headers, BuildCQL cql)
      throws Exception {
    return request(HttpMethod.GET, endpoint, headers, null, true, cql);
  }

  public CompletableFuture<Response> request(String endpoint, Map<String, String> headers)
      throws Exception {
    return request(HttpMethod.GET, endpoint, headers, null, true, null);
  }

  public CompletableFuture<Response> request(String endpoint, boolean cache, BuildCQL cql) throws Exception {
    return request(HttpMethod.GET, endpoint, null, null, cache, cql);
  }

  public CompletableFuture<Response> request(String endpoint, boolean cache) throws Exception {
    return request(HttpMethod.GET, endpoint, null, null, cache, null);
  }

  public CompletableFuture<Response> request(String endpoint, RollBackURL rbURL) throws Exception {
    return request(HttpMethod.GET, endpoint, null, rbURL, true, null);
  }

  public CompletableFuture<Response> request(String endpoint, BuildCQL cql) throws Exception {
    return request(HttpMethod.GET, endpoint, null, null, true, cql);
  }

  public CompletableFuture<Response> request(String endpoint) throws Exception {
    return request(HttpMethod.GET, endpoint, null, null, true, null);
  }

  public Function<Response, CompletableFuture<Response>> chainedRequest(
      String urlTempate, Map<String, String> headers, BuildCQL cql, Consumer<Response> completionHandler){
    try {
      List<String> replace = getTagValues(urlTempate);
      return (resp) -> {
        //once future completes we enter this section//
        //create a new request based on the content of the passed in response (resp)//
        try {
          int size = replace.size();
          String newURL = null;
          if(size > 0){
            JsonPathParser jpp = new JsonPathParser(resp.getBody());
            for (int i = 0; i < size; i++) {
              String val = (String)jpp.getValueAt(replace.get(i));
              newURL = urlTempate.replace("{"+replace.get(i)+"}", val);
            }
          }
          //call back to the passed in consumer, this function should analyze the returned//
          //response for errors / exceptions and return accordingly if found//
          completionHandler.accept(resp);
          if(cql != null){
            cql.setResponse(resp);
          }
          if(resp.getError() != null || resp.getException() != null){
            return null;
          }
          else{
            //call request//
            return request(newURL, headers, cql);
          }
        } catch (Exception e) {
          resp.exception = e;
          resp.endpoint = urlTempate;
          resp.body = null;
          resp.code = -1;
          completionHandler.accept(resp);
        }
        return null;
      };
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  private static List<String> getTagValues(final String str) {
      final List<String> tagValues = new ArrayList<>();
      final Matcher matcher = TAG_REGEX.matcher(str);
      while (matcher.find()) {
          tagValues.add(matcher.group(1));
      }
      return tagValues;
  }

  public void setDefaultHeaders(Map<String, String> headersForAllRequests){
    if(headersForAllRequests != null){
      headers.putAll(headersForAllRequests);
    }
  }

  public void closeClient(){
    httpClient.close();
    cache.invalidateAll();
  }

  public void clearCache(){
    cache.invalidateAll();
  }

  public CacheStats getCacheStats(){
    return cache.stats();
  }

  private void initCache(){
    if(cache == null){
      cache = CacheBuilder.newBuilder()
        .concurrencyLevel(4)
        .maximumSize(1000)
        .expireAfterWrite(cacheTO, TimeUnit.MINUTES)
        .build(
          new CacheLoader<String, CompletableFuture<Response>>() {
            @Override
            public CompletableFuture<Response> load(String key) throws Exception {
              return new CompletableFuture<>();
            }
          });
    }
  }

  public static void main(String args[]) throws Exception {

    String f=  "{users[0].username}".
    replace("{"+"users[0].username"+"}", "jhandy");

    f.toCharArray();
/*    JsonObject j11 = new JsonObject(
      IOUtils.toString(JsonPathParser.class.getClassLoader().
        getResourceAsStream("pathTest.json"), "UTF-8"));

    JsonObject j12 = new JsonObject(
      IOUtils.toString(JsonPathParser.class.getClassLoader().
        getResourceAsStream("pathTest.json"), "UTF-8"));

    Response test11 = new Response();
    test11.setBody(j11);
    Response test12 = new Response();
    test12.setBody(j12);

    System.out.println(test11.joinOn("c.a1", test12, "a", "c.arr[1]").getBody());

    System.out.println(test11.joinOn("c.a1", test12, "a", "c.arr", false).getBody());


    System.out.println(test11.joinOn("c.a1", test12, "a", "c.arr[0].a3").getBody());*/

    HttpModuleClient hc = new HttpModuleClient("localhost", 8083, "trigger_test2", false);

    JsonObject j = new JsonObject();
    JsonArray j22 = new JsonArray();
    JsonArray j33 = new JsonArray();

    j.put("arr", j22);
    j.put("arr2", j33);

    j22.add("librarian3");
    j22.add("librarian2");

    JsonObject j44 = new JsonObject();
    j44.put("o", new JsonObject("{\"bbb\":\"aaa\"}"));
    j33.add(j44);
    Response rr = new Response();
    rr.setBody(j);

    Response bb0 = hc.request("/groups", false, new BuildCQL(rr, "arr2[0]", "group"));
    System.out.println(bb0.body);
    Response bb = hc.request("/groups", false, new BuildCQL(rr, "arr", "group"));
    System.out.println(bb.body);
    Response bb1 = hc.request("/groups", false, new BuildCQL(rr, "arr[0]", "group"));
    System.out.println(bb1.body);
    Response bb2 = hc.request("/groups", false, new BuildCQL(rr, "arr[*]", "group"));
    System.out.println(bb2.body);


    for (int i = 0; i < 2; i++) {
      boolean cache = true;
      if(i==1){
        cache = false;
      }
      Response a = hc.request("/users", cache);
      System.out.println(a.body);
      Response b = hc.request("/groups", cache, new BuildCQL(a, "users[*].patron_group", "group"));
      a.joinOn("users[*].patron_group", b, "usergroups[*].id", "group");
      hc.request("/users").joinOn("users[*].patron_group", hc.request("/groups"), "usergroups[*].id");
    }

    Response a = hc.request("/users");
    Response b = hc.request("/groups");
    a.joinOn("users[*].patron_group2", b, "usergroups[*].id2", "group2");

    Response a1 = hc.request("/users");
    Response b1 = hc.request("/abc", new RollBackURL("/users", HttpMethod.GET));
    a1.joinOn("users[*].patron_group", b1, "usergroups[*].id", "group");
    hc.closeClient();

    JsonObject j1 = new JsonObject();
    j1.put("a", "1");
    j1.put("b", "2");
    JsonObject jo = new JsonObject();
    jo.put("a1", "1");
    j1.put("c", jo);
    JsonObject j2 = new JsonObject();
    j2.put("z", "1");
    j2.put("zz", "2");
    Response test1 = new Response();
    test1.body = j1;
    Response test2 = new Response();
    test2.body = j2;
    Response end = test1.joinOn("c.a1", test2, "z", "zz");
    System.out.println(end.getBody());
    j2.put("z", "2");
    Response end3 = test1.joinOn("c.a1", test2, "z", "zz");
    System.out.println(end3.getBody());
    Response end2 = test1.joinOn("c.a1", test2, "z");
    System.out.println(end2.getBody());
  }

}
