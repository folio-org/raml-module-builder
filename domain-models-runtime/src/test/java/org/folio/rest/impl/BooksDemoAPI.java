package org.folio.rest.impl;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.Rmbtests;
import org.folio.rest.tools.utils.OutStream;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import java.io.InputStream;

/**
 * This is a demo class for unit testing - and to serve as an examle only!
 */
public class BooksDemoAPI implements Rmbtests {

  private static final io.vertx.core.logging.Logger log = LoggerFactory.getLogger(BooksDemoAPI.class);

  /**
   * validate to test the validation aspect
   */
  @Validate
  @Override
  public void getRmbtestsBooks(String author, Number publicationYear, Number rating, String isbn,
      List<String> facets, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetRmbtestsBooksResponse.respond200WithApplicationJson(new
      org.folio.rest.jaxrs.model.Book())));

  }

  @Validate
  @Override
  public void putRmbtestsBooks(Number accessToken, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext)   {

    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(null));

  }

  @Validate
  @Override
  public void postRmbtestsBooks(org.folio.rest.jaxrs.model.Book entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext)  {
    OutStream stream = new OutStream();
    PostRmbtestsBooksResponse.HeadersFor201 headers =
        PostRmbtestsBooksResponse.headersFor201().withLocation("/dummy/location");
    stream.setData(entity);
    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(PostRmbtestsBooksResponse.
      respond201WithApplicationJson( stream , headers)));
  }

  @Validate
  @Override
  public void postRmbtestsTest(Object entity, RoutingContext routingContext,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    OutStream os = new OutStream();
    try {
      os.setData(routingContext.getBodyAsJson());
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostRmbtestsTestResponse.respond200WithApplicationJson(os)));
    } catch (Exception e) {
      log.error( e.getMessage(),  e );
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(null));
    }
  }

  @Validate
  @Override
  public void postRmbtestsTestStream(InputStream inputStream, Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
  }

}
