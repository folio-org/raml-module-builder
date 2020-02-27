package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.folio.SlimBook;
import org.folio.rest.annotations.Stream;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.Book;
import org.folio.rest.jaxrs.resource.Rmbtests;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.tools.utils.OutStream;

/**
 * This is a demo class for unit testing - and to serve as an example only!
 */
public class BooksDemoAPI implements Rmbtests {

  private static final Logger log = LoggerFactory.getLogger(BooksDemoAPI.class);
  private static final String TABLE = "test_tenantapi";

  /**
   * validate to test the validation aspect
   */
  @Validate
  @Override
  public void getRmbtestsBooks(String author, Number publicationYear, Number rating, String isbn,
      List<String> facets, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    asyncResultHandler.handle(Future.succeededFuture(GetRmbtestsBooksResponse.respond200WithApplicationJson(new
      org.folio.rest.jaxrs.model.Book())));
  }

  @Validate
  @Override
  public void putRmbtestsBooks(Number accessToken, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext)   {

    asyncResultHandler.handle(Future.succeededFuture(null));
  }

  @Validate
  @Override
  public void postRmbtestsBooks(org.folio.rest.jaxrs.model.Book entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext)  {
    OutStream stream = new OutStream();
    PostRmbtestsBooksResponse.HeadersFor201 headers =
        PostRmbtestsBooksResponse.headersFor201().withLocation("/dummy/location");
    stream.setData(entity);
    asyncResultHandler.handle(Future.succeededFuture(PostRmbtestsBooksResponse.
      respond201WithApplicationJson( stream , headers)));
  }

  @Validate
  @Override
  public void getRmbtestsTest(String query, RoutingContext routingContext,
    Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
    Context vertxContext) {

    switch (query == null ? "null" : query) {
      case "badclass=true":
        PgUtil.streamGet(TABLE, /* can not be deserialized */ StringBuilder.class,
          null, 0, 10, new LinkedList<String>(), "books", routingContext, okapiHeaders, vertxContext);
        break;
      case "nullpointer=true":
        PgUtil.streamGet(TABLE, Book.class, null, 0, 10, null, "books",
          routingContext, /* okapiHeaders is null which results in exception */ null, vertxContext);
        break;
      case "slim=true":
        PgUtil.streamGet(TABLE, SlimBook.class, null, 0, 10, new LinkedList<String>(), "books",
          routingContext, okapiHeaders, vertxContext);
        break;
      default:
        PgUtil.streamGet(TABLE, Book.class, query, 0, 10, new LinkedList<String>(), "books",
          routingContext, okapiHeaders, vertxContext);
    }
  }

  @Validate
  @Override
  public void postRmbtestsTest(Book entity, RoutingContext routingContext,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.post(TABLE, entity, okapiHeaders, vertxContext, PostRmbtestsTestResponse.class, asyncResultHandler);
  }

  @Validate
  @Override
  @Stream
  public void postRmbtestsTestStream(InputStream inputStream, Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    JsonObject jo = new JsonObject();
    jo.put("complete", okapiHeaders.containsKey("COMPLETE"));
    asyncResultHandler.handle(Future.succeededFuture(
      PostRmbtestsTestStreamResponse.respond200WithApplicationJson(jo.encodePrettily())));
  }
}
