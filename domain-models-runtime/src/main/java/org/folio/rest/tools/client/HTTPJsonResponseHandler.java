package org.folio.rest.tools.client;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author shale
 */
class HTTPJsonResponseHandler implements Handler<AsyncResult<HttpResponse<Buffer>>> {

  private static final Logger log = LogManager.getLogger(HTTPJsonResponseHandler.class);

  CompletableFuture<Response> cf;
  String endpoint;
  RollBackURL rollbackURL;
  WebClient webClient;

  public HTTPJsonResponseHandler(String endpoint, CompletableFuture<Response> cf) {
    this.cf = cf;
    this.endpoint = endpoint;
  }

  @Override
  public void handle(AsyncResult<HttpResponse<Buffer>> res) {
    if (webClient != null) {
      webClient.close();
    }
    if (res.failed()) {
      Response r = new Response();
      r.populateError(endpoint, -1, res.cause().getMessage());
      cf.complete(r);
      return;
    }
    HttpResponse<Buffer> hcr = res.result();

    //needed in cases where there is no body content
    Response r = new Response();
    r.code = hcr.statusCode();
    r.endpoint = this.endpoint;
    r.headers = hcr.headers();
    Buffer bh = hcr.bodyAsBuffer();
    if (Response.isSuccess(r.code)) {
      handleSuccess(bh, r);
    } else {
      String message = hcr.statusMessage();
      if (bh != null) {
        message = bh.toString();
      }
      r.populateError(this.endpoint, r.code, message);
      cf.complete(r);
    }

    if (HttpModuleClient2.cache != null && r.body != null) {
      try {
        HttpModuleClient2.cache.put(endpoint, cf.get());
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    }
  }

  private void handleSuccess(Buffer bh, Response r) {
    if (r.code == 204 || bh == null || bh.length() == 0) {
      r.body = null;
      cf.complete(r);
    } else {
      try {
        r.body = bh.toJsonObject();
        cf.complete(r);
      } catch (DecodeException decodeException) {
        cf.completeExceptionally(decodeException);
      }
    }
  }

}

