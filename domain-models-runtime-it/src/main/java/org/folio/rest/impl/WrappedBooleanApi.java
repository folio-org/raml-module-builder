package org.folio.rest.impl;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.WrappedBoolean;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public class WrappedBooleanApi implements WrappedBoolean {

  @Validate
  @Override
  public void getWrappedBoolean(Boolean value, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    String result = String.format("%s", value);
    Response response = WrappedBoolean.GetWrappedBooleanResponse.respond200WithTextPlain(result);
    asyncResultHandler.handle(Future.succeededFuture(response));
  }

}
