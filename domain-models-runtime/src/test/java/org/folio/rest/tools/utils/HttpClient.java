package org.folio.rest.tools.utils;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.net.MalformedURLException;

import java.net.URL;

public class HttpClient {
  private static final Logger log = LoggerFactory.getLogger(HttpClient.class);

  private static final String TENANT_HEADER = "X-Okapi-Tenant";

  private final io.vertx.core.http.HttpClient client;

  public HttpClient(Vertx vertx) {
    client = vertx.createHttpClient();
  }

  private void stdHeaders(HttpClientRequest request, String url, String tenantId) {
    URL tmp = null;
    try {
      tmp = new URL(url);
    } catch (MalformedURLException ex) {
      log.warn("Malformed url:" + url + " message: " + ex.getMessage());
    }
    stdHeaders(request, tmp, tenantId);
  }

  private void stdHeaders(HttpClientRequest request, URL url, String tenantId) {
    if (tenantId != null) {
      request.headers().add(TENANT_HEADER, tenantId);
    }
    if (url != null) {
      request.headers().add("X-Okapi-Url", url.getProtocol() + "://" + url.getHost() + ":" + url.getPort());
      request.headers().add("X-Okapi-Url-to", url.getProtocol() + "://" + url.getHost() + ":" + url.getPort());
    }
    request.headers().add("Accept","application/json, text/plain");
  }

  public void post(
    URL url,
    Object body,
    String tenantId,
    Handler<HttpClientResponse> responseHandler) {

    HttpClientRequest request = client.postAbs(url.toString(), responseHandler);
    request.headers().add("Content-type","application/json");
    stdHeaders(request, url, tenantId);

    if (body == null) {
      request.end();
      return;
    }

    String encodedBody = Json.encodePrettily(body);
    log.debug("POST {0}, Request: {1}", url.toString(), encodedBody);
    request.end(encodedBody);
  }

  public void put(
    URL url,
    Object body,
    String tenantId,
    Handler<HttpClientResponse> responseHandler) {

    HttpClientRequest request = client.putAbs(url.toString(), responseHandler);

    request.headers().add("Content-type","application/json");

    stdHeaders(request, url, tenantId);
    request.end(Json.encodePrettily(body));
  }

  public void get(
    URL url,
    String tenantId,
    Handler<HttpClientResponse> responseHandler) {

    get(url.toString(), tenantId, responseHandler);
  }

  public void get(
    String url,
    String tenantId,
    Handler<HttpClientResponse> responseHandler) {

    HttpClientRequest request = client.getAbs(url, responseHandler);
    stdHeaders(request, url, tenantId);
    request.end();
  }

  public void delete(
    URL url,
    String tenantId,
    Handler<HttpClientResponse> responseHandler) {

    delete(url.toString(), tenantId, responseHandler);
  }

  public void delete(
    String url,
    String tenantId,
    Handler<HttpClientResponse> responseHandler) {

    HttpClientRequest request = client.deleteAbs(url, responseHandler);
    stdHeaders(request, url, tenantId);
    request.end();
  }
}
