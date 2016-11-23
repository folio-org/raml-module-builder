package org.folio.rest.persist;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.MongoClientDeleteResult;
import io.vertx.ext.mongo.MongoClientUpdateResult;
import io.vertx.ext.mongo.UpdateOptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.folio.rest.persist.mongo.GroupBy;
import org.folio.rest.tools.utils.JsonUtils;
import org.folio.rest.tools.utils.NetworkUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class MongoCRUD {

  public static final String        JSON_PROP_COLLECTION    = "collection";
  public static final String        JSON_PROP_AUTHORIZATION = "authorization";
  public static final String        JSON_PROP_QUERY         = "query";
  public static final String        JSON_PROP_ORDERBY       = "orderBy";
  public static final String        JSON_PROP_OFFSET        = "offset";
  public static final String        JSON_PROP_LIMIT         = "limit";
  public static final String        JSON_PROP_ENTITY_ID     = "entityId";
  public static final String        JSON_PROP_OPS           = "_op";
  public static final String        JSON_PROP_ORDER         = "order";
  public static final String        JSON_PROP_ENTITY        = "entity";
  public static final String        JSON_PROP_SORT          = "sort";
  public static final String        JSON_PROP_CLASS         = "clazz";
  public static final MongodStarter MONGODB                 = MongodStarter.getDefaultInstance();
  public static final String        DEFAULT_SCHEMA          = "folio_shared";
  private static MongodProcess      mongoProcess;
  private static ObjectMapper       mapper                  = new ObjectMapper();
  private static boolean            embeddedMode            = false;
  private static String             configPath;
  private static MongoCRUD          instance;
  private static Map<String, MongoCRUD> connectionPool      = new HashMap<>();
  private static final Logger       log                     = LoggerFactory.getLogger(MongoCRUD.class);
  private static int                mongoPort               = 27017;

  //the only non static field - the client is tenant sensitive as  each client
  //represents a connection to a specific schema
  private MongoClient client;

  private MongoCRUD(Vertx vertx, String tenantId) throws Exception {
    init(vertx, tenantId);
  }

  /**
   * must be called before getInstance() for this to take affect
   * @param embed - whether to use an embedded mongo instance
   */
  public static void setIsEmbedded(boolean embed){
    embeddedMode = embed;
  }

  public static boolean isEmbedded(){
    return embeddedMode;
  }

  /**
   * must be called before getInstance() for this to take affect
   * @param path
   */
  public static void setConfigFilePath(String path){
    configPath = path;
  }

  // will return null on exception
  public static MongoCRUD getInstance(Vertx vertx) {
    // assumes a single thread vertx model so no sync needed
    if (instance == null) {
      try {
        instance = new MongoCRUD(vertx, DEFAULT_SCHEMA);
        //do not fail the mapping between json and objects if there is a missing property in the
        //object at this point
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      } catch (Exception e) {
         log.error(e.getMessage(), e);
      }
    }
    return instance;
  }

  // will return null on exception
  public static MongoCRUD getInstance(Vertx vertx, String tenantId) {
    // assumes a single thread vertx model so no sync needed
    if(!connectionPool.containsKey(tenantId)){
      try {
        connectionPool.put(tenantId, new MongoCRUD(vertx, tenantId));
        //do not fail the mapping between json and objects if there is a missing property in the
        //object at this point
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    }
    return connectionPool.get(tenantId);
  }

  private MongoClient init(Vertx vertx, String tenantId) throws Exception {
    if (embeddedMode) {
      if(mongoProcess == null || !mongoProcess.isProcessRunning()){
        mongoPort = NetworkUtils.nextFreePort();
      }
      JsonObject jsonConf = new JsonObject();
      jsonConf.put("db_name", tenantId);
      jsonConf.put("host", "localhost");
      jsonConf.put("port", mongoPort);
      client = MongoClient.createNonShared(vertx, jsonConf);
      log.info("created embedded mongo config on port " + mongoPort);
    } else {
      String path = "/mongo-conf.json";
      if(configPath != null){
        path = configPath;
        System.out.println("Loading mongo-conf.json from " + configPath);
      }
      else{
        System.out.println("Loading mongo-conf.json from default " + path);
      }
      JsonObject jsonConf = new LoadConfs().loadConfig(path);
      if(jsonConf == null){
        JsonObject example = new JsonObject(" {  \"db_name\": \"indexd_test\",  \"host\" : \"SERVER_NAME\",\"port\" : 1234,"+
        "\"maxPoolSize\" : 3, \"minPoolSize\" : 1, \"maxIdleTimeMS\" : 300000,\"maxLifeTimeMS\" : 3600000,"+
        "\"waitQueueMultiple\"  : 100, \"waitQueueTimeoutMS\" : 10000,\"maintenanceFrequencyMS\" : 2000, "+
        "\"maintenanceInitialDelayMS\" : 500,\"connectTimeoutMS\" : 300000, \"socketTimeoutMS\"  : 100000,"+
        "\"sendBufferSize\" : 8192, \"receiveBufferSize\" : 8192, \"keepAlive\" : true}");
        //not in embedded mode but there is no conf file found
        throw new Exception("\nNo mongo-conf.json file found at "+path+
          ". You need to run with either \n"
          + "1. embed_mongo=true \n"
          + "2. mongo_connection=<path_to_mongo-conf.json>\n"
          + "3. place mongo-conf.json in the resources dir\n"
          + " can not connect to any db store\n"
          + "EXAMPLE FILE:\n"
          + example.encodePrettily());
      }
      else{
        jsonConf.put("db_name", tenantId);
        mongoPort = jsonConf.getInteger("port");
        client = MongoClient.createNonShared(vertx, jsonConf);
      }
    }
    return client;
  }

  /**
   * Save entity into the collection. The id is the _id value of the entity if it is not null,
   * otherwise a new id is created.
   * If the document has no _id field, it is inserted, otherwise, it is _upserted_.
   * Upserted means it is inserted if it doesn’t already exist, otherwise it is updated.
   * @param collection - where to save into
   * @param entity - the entity to save
   * @param replyHandler - on success the result value is the id of the inserted entity
   */
  public void save(String collection, Object entity, Handler<AsyncResult<String>> replyHandler) {

    long start = System.nanoTime();

    try {
      JsonObject jsonObject;
      if (entity instanceof JsonObject) {
        jsonObject = (JsonObject) entity;
      } else {
        String obj = JsonUtils.entity2String(entity);
        jsonObject = new JsonObject(obj);
      }
      client.save(collection, jsonObject, res1 -> {
        if (res1.succeeded()) {
          String id = res1.result();
          if(id == null){
            //_id was passed as part of the object - if save completes successfully, return the passed _id
            id = jsonObject.getString("_id");
          }
          log.debug("Saved to "+collection+" with id " + id);
          replyHandler.handle(io.vertx.core.Future.succeededFuture(id));

        } else {
          res1.cause().printStackTrace();
          replyHandler.handle(io.vertx.core.Future.failedFuture(res1.cause().toString()));
        }
        if(log.isDebugEnabled()){
          elapsedTime("save() to " + collection, start);
        }
      });
    } catch (Throwable e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getLocalizedMessage()));
    }
  }

  /**
   *
   * @param collection
   * @param entity
   * @param failIfExist - if true - if an _id is passed in the entity and that _id already exists then the save will fail
   * @param replyHandler
   */
  public void save(String collection, Object entity, boolean failIfExist, Handler<AsyncResult<String>> replyHandler) {
    if(!failIfExist){
      save(collection, entity, replyHandler);
    }
    else{
      long start = System.nanoTime();
      JsonObject jsonObject;
      if (entity instanceof JsonObject) {
        jsonObject = (JsonObject) entity;
      } else {
        String obj = JsonUtils.entity2String(entity);
        jsonObject = new JsonObject(obj);
      }
      client.insert(collection, jsonObject, res -> {
        if (res.succeeded()) {
          String id = res.result();
          replyHandler.handle(io.vertx.core.Future.succeededFuture(id));
        } else {
          replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
          log.warn(res.cause().getMessage(), res.cause());
        }
        if(log.isDebugEnabled()){
          elapsedTime("save[insert]() to " + collection, start);
        }
      });
    }
  }

  /**
   * save an object that also has a binary field - the binary field content and the fields name are passed as
   * separate parameters and not within the object itself
   * @param collection - collection to save object to
   * @param entity - the entity to save not including the binary field's value
   * @param binaryObj - the binary value of the binary field (as a Buffer)
   * @param binaryObjFieldName - binary field name
   * @param replyHandler
   */
  public void saveBinary(String collection, Buffer buffer, String binaryObjFieldName,
      Handler<AsyncResult<String>> replyHandler){
    saveBinary(collection, buffer.getBytes(), binaryObjFieldName, replyHandler);
  }

  /**
   * save binary data to a field - the binary field content and the fields name are passed as
   * separate parameters
   * @param collection - collection to save object to
   * @param binaryObj - the binary value of the binary field
   * @param binaryObjFieldName - binary field name
   * @param replyHandler
   */
  public void saveBinary(String collection, byte[] binaryObj, String binaryObjFieldName,
      Handler<AsyncResult<String>> replyHandler){

    long start = System.nanoTime();
    JsonObject jsonObject = new JsonObject();
    try {
      jsonObject.put(binaryObjFieldName, new JsonObject().put("$binary", binaryObj));
    } catch (Exception e) {
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
      log.warn(e.getMessage(), e);
    }
    client.save(collection, jsonObject, res -> {
      if (res.succeeded()) {
        String id = res.result();
        replyHandler.handle(io.vertx.core.Future.succeededFuture(id));
      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
        log.warn(res.cause().getMessage(), res.cause());
      }
      if(log.isDebugEnabled()){
        elapsedTime("saveBinary() to " + collection, start);
      }
    });
  }

  /**
   * get binary data in mongo
   * @param collection - collection to query
   * @param from
   * @param to
   * @param binaryField - the field with the binary data (since a single collection can potentially be used to store binary data
   * of different types existing in different columns
   * @param replyHandler
   */
  public void getBinary(String collection, String binaryField, Integer from, Integer to, Handler<AsyncResult<List<?>>> replyHandler) {

    long start = System.nanoTime();

    try {
      final JsonObject query = new JsonObject();
      //get records who have content existing in the requested field
      query.put(binaryField, new JsonObject("{ \"$exists\": true } "));

      FindOptions fo = new FindOptions();

      if(to != null){
        fo.setLimit(to);
      }
      if(from != null){
        fo.setSkip(from);
      }
      client.findWithOptions(collection, query, fo, res -> {
        if (res.succeeded()) {
          List<byte[]> reply = new ArrayList<>();
          try {
            List <JsonObject> binaryList = res.result();
            int size = binaryList.size();
            for (int i = 0; i < size; i++) {
              reply.add(binaryList.get(i).getJsonObject(binaryField).getBinary("$binary"));
            }
            replyHandler.handle(io.vertx.core.Future.succeededFuture(reply));
          } catch (Exception e) {
            log.error(e);
            replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
          }
        } else {
          log.error(res.cause());
          replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().toString()));
        }
        if(log.isDebugEnabled()){
          elapsedTime("get() " + collection + " " + query.encode(), start);
        }
      });
    } catch (Throwable e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getLocalizedMessage()));
    }
  }

  /**
   *
   * @param collection
   * @param entities - list of pojos
   * @param replyHandler - will return a json object where the field 'n' can be checked for amount of records inserted
   */
  public void bulkInsert(String collection, List<Object> entities, Handler<AsyncResult<JsonObject>> replyHandler) {

    JsonObject command = new JsonObject()
      .put("insert", collection)
      .put("documents", new JsonArray(JsonUtils.entity2String(entities)))
      .put("ordered", false);

      client.runCommand("insert", command, res -> {
      if (res.succeeded()) {
        replyHandler.handle(io.vertx.core.Future.succeededFuture(res.result()));
      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
        log.error(res.cause().getMessage(), res.cause());
      }
    });
  }

  /**
   * delete a specific record
   * @param collection
   * @param id - id of the record
   * @param replyHandler
   */
  public void delete(String collection, String id, Handler<AsyncResult<MongoClientDeleteResult>> replyHandler) {
    long start = System.nanoTime();

    try {
      JsonObject jobj = new JsonObject();
      jobj.put("_id", id);
      client.removeDocument(collection, jobj, res -> {
        if (res.succeeded()) {
          System.out.println("deleted item");
          replyHandler.handle(io.vertx.core.Future.succeededFuture(res.result()));
        } else {
          log.error(res.cause());
          replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().toString()));
        }
        if(log.isDebugEnabled()){
          elapsedTime("delete() " + collection + " " + jobj.encode(), start);
        }
      });
    } catch (Throwable e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getLocalizedMessage()));
    }
  }

  /**
   * delete all records matching native mongo query
   * @param collection
   * @param query - mongo query
   * @param replyHandler
   */
  public void delete(String collection, JsonObject query, Handler<AsyncResult<MongoClientDeleteResult>> replyHandler) {
    long start = System.nanoTime();

    try {
      client.removeDocuments(collection, query, res -> {
        if (res.succeeded()) {
          replyHandler.handle(io.vertx.core.Future.succeededFuture(res.result()));
        } else {
          log.error(res.cause());
          replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().toString()));
        }
        if(log.isDebugEnabled()){
          elapsedTime("delete() " + collection + " " + query.encode(), start);
        }
      });
    } catch (Throwable e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getLocalizedMessage()));
    }
  }

  public void delete(String collection, Object pojo, Handler<AsyncResult<MongoClientDeleteResult>> replyHandler) {
    long start = System.nanoTime();

    try {
      client.removeDocuments(collection, JsonUtils.entity2Json(pojo), res -> {
        if (res.succeeded()) {
          replyHandler.handle(io.vertx.core.Future.succeededFuture(res.result()));
        } else {
          log.error(res.cause());
          replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().toString()));
        }
        if(log.isDebugEnabled()){
          elapsedTime("delete() " + collection + " " + JsonUtils.entity2String(pojo), start);
        }
      });
    } catch (Throwable e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getLocalizedMessage()));
    }
  }

  /**
   * find data in mongo
   * @param json - call the buildJson function so build a json object to pass to this function
   * @param replyHandler
   */
  public void get(JsonObject json, Handler<AsyncResult<List<?>>> replyHandler) {

    long start = System.nanoTime();

    try {
      String clazz = json.getString(JSON_PROP_CLASS);
      Integer limit = json.getInteger(JSON_PROP_LIMIT);
      Integer offset = json.getInteger(JSON_PROP_OFFSET);
      final String collection = json.getString(JSON_PROP_COLLECTION);
      final JsonObject query = new JsonObject();
      FindOptions fo = new FindOptions();

      Class<?> cls = Class.forName(clazz);

      if(limit != null){
        fo.setLimit(limit);
      }
      if(offset != null){
        fo.setSkip(offset);
      }
      if(json.getJsonObject(JSON_PROP_QUERY) != null){
        query.mergeIn(json.getJsonObject(JSON_PROP_QUERY));
      }
      JsonObject sort = json.getJsonObject(JSON_PROP_SORT);
      if(sort != null){
        fo.setSort(sort);
      }
      client.findWithOptions(collection, query, fo, res -> {
        if (res.succeeded()) {
          List<JsonObject> reply = null;
          try {
            reply = mapper.readValue(res.result().toString(),
              mapper.getTypeFactory().constructCollectionType(List.class, cls));
            if(reply == null){
              replyHandler.handle(io.vertx.core.Future.failedFuture("Seems like a mapping issue between requested objects and returned objects"));
            }else{
              replyHandler.handle(io.vertx.core.Future.succeededFuture(reply));
            }
          } catch (Exception e) {
            log.error(e);
            replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
          }
        } else {
          log.error(res.cause().getMessage(), res.cause());
          replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().toString()));
        }
        if(log.isDebugEnabled()){
          elapsedTime("get() " + collection + " " + query.encode(), start);
        }
      });
    } catch (Throwable e) {
      log.error(e);
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getLocalizedMessage()));
    }
  }

  /**
   * find data in mongo
   * @param clazz - class of the results
   * @param collection - collection to query
   * @param from
   * @param to
   * @param mongoQueryString - native mongo query to query with
   * @param replyHandler
   */
  public void get(String clazz, String collection, Integer from, Integer to, JsonObject mongoQueryString, Handler<AsyncResult<List<?>>> replyHandler) {

    long start = System.nanoTime();

    try {
      final JsonObject query = new JsonObject();
      FindOptions fo = new FindOptions();

      Class<?> cls = Class.forName(clazz);

      if(to != null){
        fo.setLimit(to);
      }
      if(from != null){
        fo.setSkip(from);
      }
      if(mongoQueryString != null){
        query.mergeIn(mongoQueryString);
      }
      client.findWithOptions(collection, query, fo, res -> {
        if (res.succeeded()) {
          List<JsonObject> reply = null;
          try {
            reply = mapper.readValue(res.result().toString(),
              mapper.getTypeFactory().constructCollectionType(List.class, cls));
            if(reply == null){
              replyHandler.handle(io.vertx.core.Future.failedFuture("Seems like a mapping issue between requested objects and returned objects"));
            }else{
              replyHandler.handle(io.vertx.core.Future.succeededFuture(reply));
            }
          } catch (Exception e) {
            log.error(e.getMessage(),e);
            replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
          }
        } else {
          log.error(res.cause().getMessage(),res.cause());
          replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().toString()));
        }
        if(log.isDebugEnabled()){
          elapsedTime("get() " + collection + " " + query.encode(), start);
        }
      });
    } catch (Throwable e) {
      log.error(e.getMessage(),e);
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getLocalizedMessage()));
    }
  }

  /**
   * Get using a pojo as a query
   * @param clazz
   * @param collection
   * @param from
   * @param to
   * @param pojoAsQuery - pojo will be converted into a json object and queried with. A partial pojo can be passed in.
   * @param replyHandler
   */
  public void get(String clazz, String collection, Integer from, Integer to, Object pojoAsQuery, Handler<AsyncResult<List<?>>> replyHandler) {
    get(clazz, collection, from, to, JsonUtils.entity2JsonNoLastModified(pojoAsQuery), replyHandler);
  }

  public void get(String collection, Object pojoAsQuery, Handler<AsyncResult<List<?>>> replyHandler) {
    get(pojoAsQuery.getClass().getName(), collection, null, null, JsonUtils.entity2JsonNoLastModified(pojoAsQuery), replyHandler);
  }

  public void get(String clazz, String collection, Integer from, Integer to, String mongoQueryString, Handler<AsyncResult<List<?>>> replyHandler) {
    get(clazz, collection, from, to, new JsonObject(mongoQueryString), replyHandler);
  }

  /**
   * Convenience get to retrieve a specific record via id from mongo
   * @param clazz - class of object to be returned
   * @param collection - collection to query
   * @param id - id of the object to return
   * @param replyHandler
   */
  public void get(String clazz, String collection, String id, Handler<AsyncResult<Object>> replyHandler) {

    long start = System.nanoTime();

    try {
      final JsonObject query = new JsonObject();
      FindOptions fo = new FindOptions();

      query.put("_id", id);
      Class<?> cls = Class.forName(clazz);

      client.findWithOptions(collection, query, fo, res -> {
        if (res.succeeded()) {
          Object reply = null;
          try {
            reply = mapper.readValue(res.result().get(0).toString(), cls);
            if(reply == null){
              replyHandler.handle(io.vertx.core.Future.failedFuture("Seems like a mapping issue between requested objects and returned objects"));
            }else{
              replyHandler.handle(io.vertx.core.Future.succeededFuture(reply));
            }
          } catch (Exception e) {
            log.error(e.getMessage(), e);
            replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
          }
        } else {
          log.error(res.cause().getMessage(), res.cause());
          replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().toString()));
        }
        if(log.isDebugEnabled()){
          elapsedTime("get() " + collection + " " + query.encode(), start);
        }
      });
    } catch (Throwable e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getLocalizedMessage()));
    }
  }


  /**
   *
   * @param collection - collection of entity to update
   * @param entity - entity to update
   * @param query - mongo native query
   * @param addUpdateDate - whether to have mongo add a date value when the update occurs automatically
   * @param replyHandler
   */
  public void update(String collection, Object entity, JsonObject query,  boolean addUpdateDate, Handler<AsyncResult<MongoClientUpdateResult>> replyHandler) {

    update(collection, entity, query, false, addUpdateDate, replyHandler);
  }

  /**
   *
   * @param collection
   * @param entity
   * @param queryAsPojo - can pass a pojo as a query - all the populated fields will be converted into a json object and passed into mongoDB
   * @param replyHandler
   */
  public void update(String collection, Object entity, Object queryAsPojo, Handler<AsyncResult<MongoClientUpdateResult>> replyHandler) {

    update(collection, entity, JsonUtils.entity2JsonNoLastModified(queryAsPojo), false, false, replyHandler);
  }

  public void update(String collection, Object entity, JsonObject query, Handler<AsyncResult<MongoClientUpdateResult>> replyHandler) {

    update(collection, entity, query, false, false, replyHandler);
  }

  /**
   *
   * @param collection
   * @param entity
   * @param queryAsPojo - can pass a pojo as a query - all the populated fields will be converted into a json object and passed into mongoDB
   * @param upsert - if the query does not match any records - hence nothing to update - should the entity be inserted
   * @param addUpdateDate - the object must have a last_modified field which will be populated with current timestamp by mongo
   * the entity must have a last_modified field
   * @param replyHandler - returns MongoClientUpdateResult which can be used to check amount of updated records
   */
  public void update(String collection, Object entity, Object queryAsPojo,  boolean upsert, boolean addUpdateDate, Handler<AsyncResult<MongoClientUpdateResult>> replyHandler) {
    update(collection, entity, JsonUtils.entity2JsonNoLastModified(queryAsPojo), upsert, addUpdateDate, replyHandler);
  }


  /**
   *
   * @param collection
   * @param entity
   * @param query
   * @param upsert - if the query does not match any records - hence nothing to update - should the entity be inserted
   * @param addUpdateDate - the object must have a last_modified field which will be populated with current timestamp by mongo
   * the entity must have a last_modified field
   * @param replyHandler - returns MongoClientUpdateResult which can be used to check amount of updated records
   */
  public void update(String collection, Object entity, JsonObject query,  boolean upsert, boolean addUpdateDate, Handler<AsyncResult<MongoClientUpdateResult>> replyHandler) {

    long start = System.nanoTime();

    JsonObject ret = new JsonObject();
    try {
      UpdateOptions options = new UpdateOptions().setUpsert(upsert);
      JsonObject update = new JsonObject();

      JsonObject jsonObject;
      if (entity instanceof JsonObject) {
        jsonObject = (JsonObject) entity;
      } else {
        String obj = JsonUtils.entity2String(entity);
        jsonObject = new JsonObject(obj);
      }

      if(addUpdateDate){
        //remove previous last_modified if exists
        jsonObject.remove("last_modified");
      }

      update.put("$set", jsonObject);

      if(addUpdateDate){
        //tell mongo to create the field with current date
        update.put("$currentDate", new JsonObject("{\"last_modified\": true}"));
      }
      if (entity == null){
        ret.put("error", "entity is null");
      }
      if(query == null) {
        ret.put("error", "query is null");
      }

      client.updateCollectionWithOptions(collection, query, update, options, res -> {

        if (res.succeeded()) {
          if(res.result().getDocModified() == 0){
            log.debug("No records updated for " + jsonObject.encode());
          }
          replyHandler.handle(io.vertx.core.Future.succeededFuture(res.result()));
        } else {
          log.error(res.cause().getMessage(), res.cause());
          replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().toString()));
        }
        if(log.isDebugEnabled()){
          elapsedTime("update() " + collection + " " + query.encode(), start);
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getLocalizedMessage()));

    }
  }

  /**
   * append items to an array of objects matching the query argument
   * @param collection - collection to update
   * @param arrayName - name of the array object - for example - a book object with a List of authors as a field will pass the
   * name of the authors field here - for example "authors"
   * @param arrayEntry - a List of items to append to the existing List - for example - if a book object has a list of authors,
   * adding additional authors would have us pass a <List> of authors objects here
   * @param query - native mongo query to get objects to add the array ob objects to - for example, a book collection update
   * may pass a title of a book as a query and therefore add authors to all books with a specific title
   * @param replyHandler
   */
  public void addToArray(String collection, String arrayName, Object arrayEntry, JsonObject query, Handler<AsyncResult<Void>> replyHandler) {

    long start = System.nanoTime();

    JsonObject ret = new JsonObject();
    try {
      UpdateOptions options = new UpdateOptions();
      JsonObject update = new JsonObject();
      JsonObject array = new JsonObject();

      if (arrayEntry == null){
        ret.put("error", "arrayEntry is null");
        replyHandler.handle(io.vertx.core.Future.failedFuture("arrayEntry to update is null"));
      }
      else if(query == null) {
        ret.put("error", "query is null");
        replyHandler.handle(io.vertx.core.Future.failedFuture("query to update is null"));
      }
      else{
        if(((List)arrayEntry).size() == 1){
          array.put(arrayName, new JsonObject(JsonUtils.entity2String(((List)arrayEntry).get(0))));
        }
        else{
          JsonObject each = new JsonObject();
          each.put("$each", new JsonArray( JsonUtils.entity2String(arrayEntry)));
          array.put(arrayName,  each );
        }

        update.put("$push", array);

        client.updateCollectionWithOptions(collection, query, update, options, res -> {

          if (res.succeeded()) {
            System.out.println(" replaced !");
            replyHandler.handle(io.vertx.core.Future.succeededFuture());
          } else {
            log.error(res.cause().getMessage(), res.cause());
            replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().toString()));
          }
          if(log.isDebugEnabled()){
            elapsedTime("addToArray() " + collection + " " + query.encode(), start);
          }
        });
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getLocalizedMessage()));

    }
  }

  /**
   * return statistics for a specific collection - such as:
   * amount of records, size of collection in kb, average object size, index size, cache info, etc...
   * @param collection - name of collection to get stats for
   * @param replyHandler
   */
  public void getStatsForCollection(String collection, Handler<AsyncResult<JsonObject>> replyHandler){
    JsonObject command = new JsonObject()
    .put("collStats", collection)
    .put("scale", 1024);

    client.runCommand("collStats", command, res -> {
      if (res.succeeded()) {
        replyHandler.handle(io.vertx.core.Future.succeededFuture(res.result()));
      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  /**
   * pass in an aggQuery - for example:
   *<pre>
   *  {
   *     _id : { month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } },
   *     totalPrice: { $sum: { $multiply: [ "$price", "$quantity" ] } },
   *     averageQuantity: { $avg: "$quantity" },
   *     count: { $sum: 1 }
   *  }
   *</pre>
   * @param collection
   * @param clazz
   * @param aggrQuery
   * @param replyHandler
   */
  public void groupBy(String collection, JsonObject aggrQuery, Handler<AsyncResult<Object>> replyHandler){

    JsonObject group = new JsonObject();
    group.put("$group", aggrQuery);

    JsonArray jar = new JsonArray();
    jar.add(group);

    JsonObject command = new JsonObject()
    .put("aggregate", collection)
    .put("pipeline", jar);

    client.runCommand("aggregate", command, res -> {
      if (res.succeeded()) {
        replyHandler.handle(io.vertx.core.Future.succeededFuture(res.result()));
      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
        log.error(res.cause().getMessage(), res.cause());
      }
    });
  }

  public void groupBy(String collection, GroupBy groupBy, Object pojoAsQuery, Handler<AsyncResult<Object>> replyHandler){

    JsonArray jar = new JsonArray();
    if(pojoAsQuery != null){
      JsonObject matching = new JsonObject().put("$match" , JsonUtils.entity2JsonNoLastModified(pojoAsQuery));
      jar.add(matching);
    }
    jar.add(groupBy.toJson());

    JsonObject command = new JsonObject()
    .put("aggregate", collection)
    .put("pipeline", jar);

    client.runCommand("aggregate", command, res -> {
      if (res.succeeded()) {
        replyHandler.handle(io.vertx.core.Future.succeededFuture(res.result()));
      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
        log.error(res.cause().getMessage(), res.cause());
      }
    });
  }

  public void groupBy(String collection, GroupBy groupBy, Handler<AsyncResult<Object>> replyHandler){

    JsonArray jar = new JsonArray();
    jar.add(groupBy.toJson());

    JsonObject command = new JsonObject()
    .put("aggregate", collection)
    .put("pipeline", jar);

    client.runCommand("aggregate", command, res -> {
      if (res.succeeded()) {
        replyHandler.handle(io.vertx.core.Future.succeededFuture(res.result()));
      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
        log.error(res.cause().getMessage(), res.cause());
      }
    });
  }

  /**
   * Get the Vert.x Mongo client to run additional apis exposed by the Vert.x client
   * @return MongoClient
   */
  public MongoClient getVertxMongoClient(){
    return client;
  }

  public void getListOfCollections(Handler<AsyncResult<List<String>>> replyHandler){
    client.getCollections( reply -> {
      if(reply.succeeded()){
        replyHandler.handle(io.vertx.core.Future.succeededFuture(reply.result()));
      }
      else{
        log.error(reply.cause().getMessage());
      }
    });
  }

  public void startEmbeddedMongo() throws Exception {

    if(mongoProcess == null || !mongoProcess.isProcessRunning()){
      IMongodConfig mongodConfig = new MongodConfigBuilder()
          .version(Version.Main.PRODUCTION)
          .net(new Net("localhost", mongoPort, Network.localhostIsIPv6()))
          .build();
      mongoProcess = MONGODB.prepare(mongodConfig).start();
    }
    else{
      log.info("Embedded Mongo on port " + mongoPort + " is already running");
    }
  }

  public static void stopEmbeddedMongo() {
    if (mongoProcess != null) {
      mongoProcess.stop();
    }
  }

  public static JsonObject buildJson(String returnClazz, String collection, String query, String orderBy, Object order, int offset, int limit){
    JsonObject q = null;
    if(query != null){
      try {
        q = new JsonObject(query);
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }
    }
    return buildJson(returnClazz, collection, q, orderBy, order, offset, limit );
  }

  /**
   * helper class that can build a json object to be passed to the get() function
   * @param returnClazz - class of objects expected to be returned - for example passing a Fines class to get()
   * fine objects
   * @param collection - the collection to query from
   * @param query - a valid mongodb json query
   * @param orderBy
   * @param order
   * @param offset
   * @param limit
   * @return
   */
  public static JsonObject buildJson(String returnClazz, String collection, JsonObject query, String orderBy, Object order, int offset, int limit){
    try {
      JsonObject req = new JsonObject();
      if(query != null){
        try {
          req.put(MongoCRUD.JSON_PROP_QUERY, query);
        } catch (Exception e) {
          log.error( "Unable to parse query param to json " + e.getLocalizedMessage() );
        }
      }

      req.put(MongoCRUD.JSON_PROP_COLLECTION, collection);
      if(offset != -1){
        req.put(MongoCRUD.JSON_PROP_OFFSET, offset);
      }
      if(limit != -1){
        req.put(MongoCRUD.JSON_PROP_LIMIT, limit);
      }
      req.put(MongoCRUD.JSON_PROP_CLASS, returnClazz);
      if(orderBy != null){
        int ord = -1;
        if("asc".equals(order.toString())){
          ord = 1;
        }
        req.put(MongoCRUD.JSON_PROP_SORT, new JsonObject("{\""+orderBy+"\" : "+ord+"}"));
      }
      return req;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public static JsonObject buildJson(String returnClazz, String collection, JsonObject query, String orderBy, Object order){
    return buildJson(returnClazz, collection, query, orderBy, order, -1, -1);
  }

  public static JsonObject buildJson(String returnClazz, String collection, String query){
    return buildJson(returnClazz, collection, query, null, null, -1 ,-1);
  }

  public static JsonObject buildJson(String returnClazz, String collection, JsonObject query){
    return buildJson(returnClazz, collection, query, null, null, -1 ,-1);
  }

  private void elapsedTime(String info, long start){
      log.debug(new StringBuffer(info).append(" ").append((System.nanoTime() - start)/1000000).append(" ms"));
  }

}
