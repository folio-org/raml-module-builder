package org.folio.rest.impl;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.rest.jaxrs.model.Bee;
import org.folio.rest.jaxrs.model.BeeHistory;
import org.folio.rest.jaxrs.model.Beehistories;
import org.folio.rest.jaxrs.model.Bees;
import org.folio.rest.persist.PgUtil;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;

public class BeesApi implements org.folio.rest.jaxrs.resource.Bees {
  private static final String TABLE = "bees";
  private static final String HISTORY_TABLE = "bee_history";

  @Override
  public void getBeesBees(String query, String totalRecords, int offset, int limit, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PgUtil.get(TABLE, Bee.class, Bees.class, query, totalRecords, offset, limit, okapiHeaders, vertxContext,
        GetBeesBeesResponse.class, asyncResultHandler);
  }

  @Override
  public void postBeesBees(Bee entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PgUtil.post(TABLE, entity, okapiHeaders, vertxContext,
        PostBeesBeesResponse.class, asyncResultHandler);
  }

  @Override
  public void getBeesBeesByBeeId(String id, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PgUtil.getById(TABLE, Bee.class, id, okapiHeaders, vertxContext,
        GetBeesBeesByBeeIdResponse.class, asyncResultHandler);
  }

  @Override
  public void deleteBeesBeesByBeeId(String id, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PgUtil.deleteById(TABLE, id, okapiHeaders, vertxContext,
        DeleteBeesBeesByBeeIdResponse.class, asyncResultHandler);
  }

  @Override
  public void putBeesBeesByBeeId(String id, Bee entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PgUtil.put(TABLE, entity, id, okapiHeaders, vertxContext,
        PutBeesBeesByBeeIdResponse.class, asyncResultHandler);
  }

  @Override
  public void getBeesHistory(String totalRecords, int offset, int limit, String query, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
   PgUtil.get(HISTORY_TABLE, BeeHistory.class, Beehistories.class, query, offset, limit, okapiHeaders, vertxContext,
       GetBeesHistoryResponse.class, asyncResultHandler);
  }
}
