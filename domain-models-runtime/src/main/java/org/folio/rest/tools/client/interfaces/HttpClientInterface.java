package org.folio.rest.tools.client.interfaces;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import org.folio.rest.tools.client.BuildCQL;
import org.folio.rest.tools.client.Response;

import com.google.common.cache.CacheStats;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;

/**
 * @author shale
 *
 */
public interface HttpClientInterface {

  CompletableFuture<Response> request(HttpMethod method, Buffer data, String endpoint,
      Map<String, String> headers) throws Exception;

  CompletableFuture<Response> request(HttpMethod method, Object pojo, String endpoint,
      Map<String, String> headers) throws Exception;

  CompletableFuture<Response> request(HttpMethod method, String endpoint,
      Map<String, String> headers) throws Exception;

  CompletableFuture<Response> request(String endpoint, Map<String, String> headers, boolean cache,
      BuildCQL cql) throws Exception;

  CompletableFuture<Response> request(String endpoint, Map<String, String> headers, boolean cache)
      throws Exception;

  CompletableFuture<Response> request(String endpoint, Map<String, String> headers, BuildCQL cql)
      throws Exception;

  CompletableFuture<Response> request(String endpoint, Map<String, String> headers)
      throws Exception;

  CompletableFuture<Response> request(String endpoint, boolean cache, BuildCQL cql)
      throws Exception;

  CompletableFuture<Response> request(String endpoint, boolean cache) throws Exception;

  CompletableFuture<Response> request(String endpoint, BuildCQL cql) throws Exception;

  CompletableFuture<Response> request(String endpoint) throws Exception;

  /**
   * A request that should be used within a thenCompose() completable future call and receive as
   * input a Response object from the previous request.
   * The chainedRequest will
   * 1. callback to the passed in Consumer with the Response of the previous request for handling
   * 2. if the passed in Response contains errors - the current request will not be sent and a
   * completable future will be returned indicating that the request was not run
   * 3. replace placeholder in the url {a.b[0]} with actual values appearing in the json passed in
   * by the thenCompose(). For example: passing in:
   * http://localhost:9130/users/{users[0].username}
   * will look in the passed in json for the value found in the first user in a json array[].username
   * NOTE that if a template has a placeholder, and the path indicated in the placeholder does not
   * exist in the passed in Response - the current request will not be sent and a completeable future
   * with a Response (containing the error) will be returned
   * 4. build a cql via values in the passed in json
   * 5. Send the request
   * @param urlTempate
   * @param headers
   * @param inheritOkapiHeaders - take all okapi headers in passed in Response and over write / add to this request
   * @param cql
   * @param processPassedInResponse
   * @return
   */
  Function<Response, CompletableFuture<Response>> chainedRequest(String urlTempate,
      Map<String, String> headers, boolean inheritOkapiHeaders, boolean cache, BuildCQL cql,
      Consumer<Response> processPassedInResponse);

  Function<Response, CompletableFuture<Response>> chainedRequest(String urlTempate,
      Map<String, String> headers, boolean inheritOkapiHeaders, BuildCQL cql,
      Consumer<Response> processPassedInResponse);

  Function<Response, CompletableFuture<Response>> chainedRequest(String urlTempate,
      Map<String, String> headers, BuildCQL cql, Consumer<Response> processPassedInResponse);

  void setDefaultHeaders(Map<String, String> headersForAllRequests);

  void closeClient();

  void clearCache();

  CacheStats getCacheStats();

}
