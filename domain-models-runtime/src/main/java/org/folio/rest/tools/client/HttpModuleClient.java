package org.folio.rest.tools.client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.IOUtils;
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
public class HttpModuleClient {

  private static final String CTYPE = "Content-Type";
  private static final String ACCEPT = "Accept";
  private static final String APP_JSON_CTYPE = "application/json";
  private static final String APP_JSON_ACCEPT = "application/json";
  private static final String X_OKAPI_HEADER = "x-okapi-tenant";

  private static final Logger log = LoggerFactory.getLogger(HttpModuleClient.class);

  private String tenantId;
  private HttpClientOptions options;
  private HttpClient httpClient;
  private LoadingCache<String, Response> cache = null;
  private Vertx vertx;
  private boolean autoCloseConnections = true;
  private Map<String, String> headers = new HashMap<>();
  private long cacheTO = 30; //minutes
  private int connTO = 2000;
  private int idleTO = 5000;
  private boolean absoluteHostAddr = false;

  public HttpModuleClient(String host, int port, String tenantId, boolean keepAlive, int connTO,
      int idleTO, boolean autoCloseConnections, long cacheTO) {

    this.tenantId = tenantId;
    this.cacheTO = cacheTO;
    this.connTO = connTO;
    this.idleTO = idleTO;
    options = new HttpClientOptions().setLogActivity(true).setKeepAlive(keepAlive)
        .setConnectTimeout(connTO).setIdleTimeout(idleTO);

    if(port == -1){
      absoluteHostAddr = true;
    }
    else{
      options.setDefaultPort(port).setDefaultHost(host);
    }
    this.autoCloseConnections = autoCloseConnections;
    vertx = VertxUtils.getVertxFromContextOrNew();
    setDefaultHeaders();
  }

  public HttpModuleClient(String host, int port, String tenantId) {
    this(host, port, tenantId, true, 2000, 5000, true, 30);
  }

  public HttpModuleClient(String absHost, String tenantId) {
    this(absHost, -1, tenantId, true, 2000, 5000, true, 30);
  }

  public HttpModuleClient(String host, int port, String tenantId, boolean autoCloseConnections) {
    this(host, port, tenantId, true, 2000, 5000, autoCloseConnections, 30);
  }

  public HttpModuleClient(String absHost, String tenantId, boolean autoCloseConnections) {
    this(absHost, -1, tenantId, true, 2000, 5000, autoCloseConnections, 30);
  }

  private void setDefaultHeaders(){
    headers.put(X_OKAPI_HEADER, tenantId);
    headers.put(CTYPE, APP_JSON_CTYPE);
    headers.put(ACCEPT, APP_JSON_ACCEPT);
  }

  private void request(HttpMethod method, String endpoint, Map<String, String> headers,
      boolean cache, Handler<HttpClientResponse> responseHandler, CompletableFuture<Response> cf2){

    if(responseHandler == null){
      CompletableFuture<Response> cf = new CompletableFuture<>();
      responseHandler = new HTTPJsonResponseHandler(endpoint, cf);
    }
    httpClient = vertx.createHttpClient(options);
    HttpClientRequest request = httpClient.request(method, endpoint);
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
  }

  public Response request(HttpMethod method, String endpoint, Map<String, String> headers, RollBackURL rollbackURL,
      boolean cachable, BuildCQL bCql) throws Exception {

    if(bCql != null){
      endpoint = endpoint + bCql.buildCQL();
    }
    if(cachable){
      initCache();
      Response j = cache.get(endpoint);
      if(j.body != null){
        return j;
      }
    }
    CompletableFuture<Response> cf = new CompletableFuture<>();
    request(method, endpoint, headers, cachable, new HTTPJsonResponseHandler(endpoint, cf), cf);
    Response response = new Response();
    try {
      response = cf.get((idleTO/1000)+1, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      response.populateError(endpoint, -1, e.toString());
    }
    catch(Throwable t){
      response.endpoint = endpoint;
      response.exception = t;
      response.populateError(endpoint, -1, t.getMessage());
    }
    if(cachable && response.body != null) {
      cache.put(endpoint, response);
    }
    if(autoCloseConnections){
      httpClient.close();
    }
    if(response.error != null && rollbackURL != null){
      Response rb = request(rollbackURL.method, rollbackURL.endpoint, null, null, false, rollbackURL.cql);
      if(rb.error != null){
        response.populateRollBackError(rb.error);
      }
      else{
        response.body = rb.body;
      }
    }
    return response;
  }

  public Response request(String endpoint, Map<String, String> headers, boolean cache, BuildCQL cql)
      throws Exception {
    return request(HttpMethod.GET, endpoint, headers, null, cache, cql);
  }

  public Response request(String endpoint, Map<String, String> headers, boolean cache)
      throws Exception {
    return request(HttpMethod.GET, endpoint, headers, null, cache, null);
  }

  public Response request(String endpoint, Map<String, String> headers, BuildCQL cql)
      throws Exception {
    return request(HttpMethod.GET, endpoint, headers, null, true, cql);
  }

  public Response request(String endpoint, Map<String, String> headers)
      throws Exception {
    return request(HttpMethod.GET, endpoint, headers, null, true, null);
  }

  public Response request(String endpoint, boolean cache, BuildCQL cql) throws Exception {
    return request(HttpMethod.GET, endpoint, null, null, cache, cql);
  }

  public Response request(String endpoint, boolean cache) throws Exception {
    return request(HttpMethod.GET, endpoint, null, null, cache, null);
  }

  public Response request(String endpoint, RollBackURL rbURL) throws Exception {
    return request(HttpMethod.GET, endpoint, null, rbURL, true, null);
  }

  public Response request(String endpoint, BuildCQL cql) throws Exception {
    return request(HttpMethod.GET, endpoint, null, null, true, cql);
  }

  public Response request(String endpoint) throws Exception {
    return request(HttpMethod.GET, endpoint, null, null, true, null);
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
          new CacheLoader<String, Response>() {
            @Override
            public Response load(String key) throws Exception {
              return new Response();
            }
          });
    }
  }

  public static void main(String args[]) throws Exception {

    JsonObject j11 = new JsonObject(
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


    System.out.println(test11.joinOn("c.a1", test12, "a", "c.arr[0].a3").getBody());

    HttpModuleClient hc = new HttpModuleClient("localhost", 8083, "myuniversity_new2", false);

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

    Response bb = hc.request("/groups", false, new BuildCQL(rr, "arr", "group"));

    Response bb1 = hc.request("/groups", false, new BuildCQL(rr, "arr[0]", "group"));

    Response bb2 = hc.request("/groups", false, new BuildCQL(rr, "arr[*]", "group"));


    for (int i = 0; i < 2; i++) {
      boolean cache = true;
      if(i==1){
        cache = false;
      }
      Response a = hc.request("/users", cache);
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
