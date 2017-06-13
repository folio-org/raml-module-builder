package org.folio.rest.tools.client;

import java.util.concurrent.CompletableFuture;

import com.google.common.cache.LoadingCache;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;

/**
 * @author shale
 *
 */
class HTTPJsonResponseHandler implements Handler<HttpClientResponse> {

  CompletableFuture<Response> cf;
  String endpoint;
  RollBackURL rollbackURL;
  LoadingCache<String, CompletableFuture<Response>> cache;
  HttpClient httpClient;

  public HTTPJsonResponseHandler(String endpoint, CompletableFuture<Response> cf){
    this.cf = cf;
    this.endpoint = endpoint;
  }

  @Override
  public void handle(HttpClientResponse hcr) {
    hcr.bodyHandler( bh -> {
      Response r = new Response();
      r.code = hcr.statusCode();
      r.endpoint = this.endpoint;
      if(Response.isSuccess(r.code)){
        r.body = bh.toJsonObject();
      }
      else{
        r.populateError(this.endpoint, r.code, hcr.statusMessage());
      }
      cf.complete(r);
      if(httpClient != null){
        httpClient.close();
      }
      if(cache != null && r.body != null) {
        cache.put(endpoint, cf);
      }
/*      if(r.error != null && rollbackURL != null){

      }*/
    });
    hcr.exceptionHandler( eh -> {
      cf.completeExceptionally(eh);
    });
  }

}

