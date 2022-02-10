package org.folio.rest.tools.client.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.tools.client.BuildCQL;
import org.folio.rest.tools.client.Response;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;

import com.google.common.cache.CacheStats;

import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Mock HTTP client.
 * @deprecated Use {@link io.vertx.core.Vertx#createHttpServer()} for mocking a server or use mocking utility.
 */
@Deprecated
public class HttpClientMock2 implements HttpClientInterface {

  public static final String MOCK_MODE = "mock.httpclient";
  public static final String MOCK_FILE = "mock_content.json";

  private static final Logger log = LogManager.getLogger(HttpClientMock2.class);

  private static JsonObject mockJson;

  public HttpClientMock2(String host, int port, String tenantId, boolean keepAlive, int connTO,
      int idleTO, boolean autoCloseConnections, long cacheTO) {
  }

  public HttpClientMock2(String host, int port, String tenantId) {
  }

  public HttpClientMock2(String absHost, String tenantId) {
  }

  public HttpClientMock2(String host, int port, String tenantId, boolean autoCloseConnections) {
  }

  public HttpClientMock2(String absHost, String tenantId, boolean autoCloseConnections) {
  }

  static {
    //by default read mock_content.json from class path and load
    try {
      InputStream is = HttpClientMock2.class.getClassLoader().getResourceAsStream(MOCK_FILE);
      if(is != null){
        mockJson = new JsonObject( IOUtils.toString(is, "UTF-8") );
      }
      else{
        mockJson = new JsonObject();
      }
    } catch (Throwable e) {
      log.error("unable to read in mock_content.json file, mocking will not work unless setMockContent(String) is called with relevant content");
    }
  }

  public void setMockJsonContent(String fileName) throws IOException {
    mockJson = new JsonObject( IOUtils.toString(HttpClientMock2.class.getClassLoader().getResourceAsStream(fileName), "UTF-8") );
  }

  private CompletableFuture<Response> getCF(HttpMethod method, String endpoint) throws Exception {
    log.info("MOCKING URL: " + endpoint);
    JsonArray jar = mockJson.getJsonArray("mocks");
    int size = jar.size();
    for (int i = 0; i < size; i++) {
      JsonObject j = jar.getJsonObject(i);
      String url = j.getString("url");
      String method1 = j.getString("method");

      if(endpoint.equalsIgnoreCase(url) &&
          method.toString().equalsIgnoreCase(method1) ){
        Response r = new Response();
        JsonObject body = null;
        String path2data = j.getString("receivedPath");
        try {
          if(path2data != null && path2data.trim().length() > 0){
            body =
                new JsonObject( IOUtils.toString(HttpClientMock2.class.getClassLoader().getResourceAsStream(path2data), "UTF-8") );
          }
        } catch (IOException e) {
          log.warn("unable to read in json content at " + path2data);
        }
        if(body == null || body.isEmpty()){
          try {
            body = j.getJsonObject("receivedData");
          } catch (Exception e) {
            log.warn("unable to read in json content from receivedData field for endpoint" + endpoint);
          }
        }
        r.setBody(body);
        Integer status = j.getInteger("status");
        if(status != null){
          r.setCode(status);
        }
        r.setEndpoint(endpoint);
        JsonArray headers = j.getJsonArray("headers");
        if(headers != null){
          MultiMap mm = MultiMap.caseInsensitiveMultiMap();
          //List<JsonObject> hList = headers.getList();
          int size2 = headers.size();
          for (int j1 = 0; j1 < size2; j1++) {
            String headerName = headers.getJsonObject(j1).getString("name");
            String headerValue = headers.getJsonObject(j1).getString("value");
            mm.add(headerName, headerValue);
          }
          r.setHeaders(mm);
        }
        CompletableFuture<Response> cf = new CompletableFuture<>();
        cf.complete(r);
        return cf;
      }
    }
    CompletableFuture<Response> cf = new CompletableFuture<>();
    Response r = new Response();
    r.setCode(0);
    r.setEndpoint(endpoint);
    r.setError(new JsonObject("{\"endpoint\":\""+endpoint+"\",\"statusCode\": 0 , \"errorMessage\":\"mocked error\"}"));
    cf.complete(r);
    return cf;
  }

  @Override
  public CompletableFuture<Response> request(HttpMethod method, Buffer data, String endpoint,
      Map<String, String> headers) throws Exception {
    return getCF(method, endpoint);
  }

  @Override
  public CompletableFuture<Response> request(HttpMethod method, Object pojo, String endpoint,
      Map<String, String> headers) throws Exception {
    return getCF(method, endpoint);
  }

  @Override
  public CompletableFuture<Response> request(HttpMethod method, String endpoint,
      Map<String, String> headers) throws Exception {
    return getCF(method, endpoint);
  }

  @Override
  public CompletableFuture<Response> request(String endpoint, Map<String, String> headers,
      boolean cache, BuildCQL cql) throws Exception {
    return getCF(HttpMethod.GET, endpoint);
  }

  @Override
  public CompletableFuture<Response> request(String endpoint, Map<String, String> headers,
      boolean cache) throws Exception {
    return getCF(HttpMethod.GET, endpoint);
  }

  @Override
  public CompletableFuture<Response> request(String endpoint, Map<String, String> headers,
      BuildCQL cql) throws Exception {
    return getCF(HttpMethod.GET, endpoint);
  }

  @Override
  public CompletableFuture<Response> request(String endpoint, Map<String, String> headers)
      throws Exception {
    return getCF(HttpMethod.GET, endpoint);
  }

  @Override
  public CompletableFuture<Response> request(String endpoint, boolean cache, BuildCQL cql)
      throws Exception {
    return getCF(HttpMethod.GET, endpoint);

  }

  @Override
  public CompletableFuture<Response> request(String endpoint, boolean cache) throws Exception {
    return getCF(HttpMethod.GET, endpoint);

  }

  @Override
  public CompletableFuture<Response> request(String endpoint, BuildCQL cql) throws Exception {
    return getCF(HttpMethod.GET, endpoint);
  }

  @Override
  public CompletableFuture<Response> request(String endpoint) throws Exception {
    return getCF(HttpMethod.GET, endpoint);
  }

  @Override
  public Function<Response, CompletableFuture<Response>> chainedRequest(String urlTempate,
      Map<String, String> headers, boolean inheritOkapiHeaders, BuildCQL cql,
      Consumer<Response> processPassedInResponse) {
    return (resp) -> {
      try {
        if(cql != null){
          cql.setResponse(resp);
        }
        return getCF(HttpMethod.GET, urlTempate + cql.buildCQL());
      } catch (Exception e) {
        return null;
      }
    };
  }

  @Override
  public Function<Response, CompletableFuture<Response>> chainedRequest(String urlTempate,
      Map<String, String> headers, BuildCQL cql, Consumer<Response> processPassedInResponse) {
    return (resp) -> {
      try {
        if(cql != null){
          cql.setResponse(resp);
        }
        return getCF(HttpMethod.GET, urlTempate + cql.buildCQL());
      } catch (Exception e) {
        return null;
      }
    };
  }

  @Override
  public Function<Response, CompletableFuture<Response>> chainedRequest(String urlTempate,
      Map<String, String> headers, boolean inheritOkapiHeaders, boolean cache, BuildCQL cql,
      Consumer<Response> processPassedInResponse) {
    return chainedRequest(urlTempate, headers, inheritOkapiHeaders, cql, processPassedInResponse);
  }

  @Override
  public void setDefaultHeaders(Map<String, String> headersForAllRequests) {
    // TODO Auto-generated method stub
  }

  @Override
  public void closeClient() {
    // TODO Auto-generated method stub
  }

  @Override
  public void clearCache() {
    // TODO Auto-generated method stub
  }

  @Override
  public CacheStats getCacheStats() {
    // TODO Auto-generated method stub
    return null;
  }

}
