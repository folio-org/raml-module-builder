package org.folio.rest.impl;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.rest.jaxrs.model.Myitem;
import org.folio.rest.jaxrs.model.Myitems;
import org.folio.rest.persist.PgUtil;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;

public class MyitemsApi implements org.folio.rest.jaxrs.resource.Myitems {
  private static final String TABLE = "myitems";

  @Override
  public void getMyitems(String query, int offset, int limit, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PgUtil.get(TABLE, Myitem.class, Myitems.class, query, offset, limit, okapiHeaders, vertxContext,
        GetMyitemsResponse.class, asyncResultHandler);
  }

  @Override
  public void postMyitems(String lang, Myitem entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PgUtil.post(TABLE, entity, okapiHeaders, vertxContext,
        PostMyitemsResponse.class, asyncResultHandler);
  }

  @Override
  public void getMyitemsByMyitemId(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PgUtil.getById(TABLE, Myitem.class, id, okapiHeaders, vertxContext,
        GetMyitemsByMyitemIdResponse.class, asyncResultHandler);
  }

  @Override
  public void deleteMyitemsByMyitemId(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PgUtil.deleteById(TABLE, id, okapiHeaders, vertxContext,
        DeleteMyitemsByMyitemIdResponse.class, asyncResultHandler);
  }

  @Override
  public void putMyitemsByMyitemId(String id, String lang, Myitem entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PgUtil.put(TABLE, entity, id, okapiHeaders, vertxContext,
        PutMyitemsByMyitemIdResponse.class, asyncResultHandler);
  }
}
