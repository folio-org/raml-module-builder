package org.folio.rest.tools.utils;

import io.vertx.core.json.JsonObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.rest.persist.MongoCRUD;


public class RestUtils {

  public static JsonObject createMongoObject(String collection, String op, String authorization, JsonObject query, String orderBy, Object order, int offset, int limit, Object entity, String entityId){
    JsonObject jObj = new JsonObject();
    jObj.put(MongoCRUD.JSON_PROP_COLLECTION, collection);
    jObj.put(MongoCRUD.JSON_PROP_AUTHORIZATION, authorization);
    jObj.put(MongoCRUD.JSON_PROP_QUERY, query);
    jObj.put(MongoCRUD.JSON_PROP_ORDERBY, orderBy);
    jObj.put(MongoCRUD.JSON_PROP_OFFSET, offset);
    jObj.put(MongoCRUD.JSON_PROP_LIMIT, limit);
    jObj.put(MongoCRUD.JSON_PROP_ENTITY_ID, entityId);
    jObj.put(MongoCRUD.JSON_PROP_OPS, op);
    if(order != null){
      jObj.put(MongoCRUD.JSON_PROP_ORDER, order.toString());
    }
    if(entity != null){
      ObjectMapper mapper = new ObjectMapper();
      if(entity instanceof JsonObject){
        jObj.put(MongoCRUD.JSON_PROP_ENTITY, ((JsonObject) entity).encode());
      }
      else{
        try {
          jObj.put(MongoCRUD.JSON_PROP_ENTITY, mapper.writeValueAsString(entity));
        } catch (JsonProcessingException e) {
          e.printStackTrace();
        }
      }
    }
    return jObj;
  }

/* public static OutStream sendRequest(JsonObject jObj) throws ExecutionException, InterruptedException{

    OutStream stream = new OutStream();

    try {

      RestVerticle.EVENTBUS.send(RestVerticle.API_BUS_ADDRESS, jObj, reply -> {
        stream.setData(reply.result().body());
      });

      //Message<String> reply = Sync.awaitResult(h -> RestVerticle.EVENTBUS.send(RestVerticle.API_BUS_ADDRESS, jObj, h), (EVENT_BUS_REQUEST_TIMEOUT_SEC*1000));
        //stream.setData(reply.body());

    } catch (Throwable e) {
      e.printStackTrace();
    }
    return stream;
  }
   */
/* public void sendRequest(JsonObject jObj, OutStream stream) throws SuspendExecution, ExecutionException, InterruptedException{
    //OutStream stream =  new OutStream();
    join( res -> {

      try {

        RestVerticle.EVENTBUS.send(RestVerticle.API_BUS_ADDRESS, jObj, reply -> {
          stream.setData(reply.result().body());
        });

        //Message<String> reply = Sync.awaitResult(h -> RestVerticle.EVENTBUS.send(RestVerticle.API_BUS_ADDRESS, jObj, h), (EVENT_BUS_REQUEST_TIMEOUT_SEC*1000));
          //stream.setData(reply.body());

      } catch (Throwable e) {
        e.printStackTrace();
      }
    });

    //return stream;
  }

  public void join(Handler<AsyncResult<Void>> resultHandler) {
      Context context = RestVerticle.VERTX.getOrCreateContext();
      context.runOnContext(v -> {
        resultHandler.handle(io.vertx.core.Future.succeededFuture());
      });
  }*/
}
