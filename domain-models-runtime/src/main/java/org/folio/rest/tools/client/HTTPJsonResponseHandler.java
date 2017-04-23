package org.folio.rest.tools.client;

import java.util.concurrent.CompletableFuture;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpClientResponse;

/**
 * @author shale
 *
 */
class HTTPJsonResponseHandler implements Handler<HttpClientResponse> {

  CompletableFuture<Response> cf;
  String endpoint;
  public HTTPJsonResponseHandler(String endpoint, CompletableFuture<Response> cf){
    this.cf = cf;
    this.endpoint = endpoint;
  }
  @Override
  public void handle(HttpClientResponse hcr) {
    hcr.bodyHandler( bh -> {
      Response r = new Response();
      r.code = hcr.statusCode();
      if(Response.isSuccess(r.code)){
        r.body = bh.toJsonObject();
      }
      else{
        r.populateError(this.endpoint, r.code, hcr.statusMessage());
      }
      cf.complete(r);
    });
    hcr.exceptionHandler( eh -> {
      cf.completeExceptionally(eh);
    });
  }

}