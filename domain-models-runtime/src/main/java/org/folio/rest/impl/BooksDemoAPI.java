package org.folio.rest.impl;

import java.io.Reader;
import java.math.BigDecimal;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.Book;
import org.folio.rest.jaxrs.resource.RmbtestsResource;
import org.folio.rest.tools.utils.OutStream;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

/**
 * This is a demo class for unit testing - and to serve as an examle only!
 */
public class BooksDemoAPI implements RmbtestsResource {


  /**
   * validate to test the validation aspect
   */
  @Validate
  @Override
  public void getRmbtestsBooks(String author, BigDecimal publicationYear, BigDecimal rating,
      String isbn, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetRmbtestsBooksResponse.withJsonOK(new Book())));

  }
  @Validate
  @Override
  public void putRmbtestsBooks(BigDecimal accessToken, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(null));

  }

  @Validate
  @Override
  public void postRmbtestsBooks(Book entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(null));

  }

  @Override
  public void postRmbtestsTest(Reader entity, RoutingContext routingContext,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) throws Exception {

    OutStream os = new OutStream();
    try {
      os.setData(routingContext.getBodyAsJson());
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostRmbtestsTestResponse.withJsonCreated(os)));
    } catch (Exception e) {
      e.printStackTrace();
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(null));
    }
  }

}
