package org.folio.rest.impl;

import java.util.Date;
import java.util.Map;
import javax.ws.rs.core.Response;

import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.Datetime;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public class DatetimeApi implements org.folio.rest.jaxrs.resource.Datetime {
  @Validate
  @Override
  public void getDatetime(Date startDate, Date endDate, Date requestedDate, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    String result =
        "startDate=" + startDate +
        " endDate=" + endDate +
        " requestedDate=" + requestedDate;
    Response response = Datetime.GetDatetimeResponse.respond200WithTextPlain(result);
    asyncResultHandler.handle(Future.succeededFuture(response));
  }
}
