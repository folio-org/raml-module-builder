package org.folio.rest.persist;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.sql.SQLConnection;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.UpdateSection;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.LogUtil;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import ru.yandex.qatools.embed.postgresql.Command;
import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig;
import ru.yandex.qatools.embed.postgresql.config.DownloadConfigBuilder;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;
import ru.yandex.qatools.embed.postgresql.config.RuntimeConfigBuilder;
import ru.yandex.qatools.embed.postgresql.distribution.Version;
import ru.yandex.qatools.embed.postgresql.ext.ArtifactStoreBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.flapdoodle.embed.process.config.IRuntimeConfig;

/**
 * @author shale currently does not support binary data unless base64 encoded
 *
 */
public class PostgresClient {

  public static final String     DEFAULT_SCHEMA           = "folio_shared";
  public static final String     DEFAULT_JSONB_FIELD_NAME = "jsonb";
  public static final String     ID_FIELD                 = "_id";

  private static final String    COUNT_CLAUSE             = " count(_id) OVER() AS count, ";
  private static final String    RETURNING_IDS            = " RETURNING _id ";

  private static final String    POSTGRES_LOCALHOST_CONFIG = "/postgres-conf.json";
  private static final int       EMBEDDED_POSTGRES_PORT   = 6000;

  private static final String   UPDATE = "UPDATE ";
  private static final String   SET = " SET ";
  private static final String   PASSWORD = "password";
  private static final String   USERNAME = "username";

  private static PostgresProcess postgresProcess          = null;
  private static boolean         embeddedMode             = false;
  private static String          configPath               = null;
  private static PostgresClient  instance                 = null;
  private static ObjectMapper    mapper                   = new ObjectMapper();
  private static Map<String, PostgresClient> connectionPool = new HashMap<>();

  private static final Logger log = LoggerFactory.getLogger(PostgresClient.class);

  private Vertx vertx                       = null;
  private JsonObject postgreSQLClientConfig = null;
  private final Messages messages           = Messages.getInstance();

  private AsyncSQLClient         client;

  private PostgresClient(Vertx vertx, String tenantId) throws Exception {
    init(vertx, tenantId);
  }

  /**
   * must be called before getInstance() for this to take affect
   * @param embed - whether to use an embedded postgres instance
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

  public static String getConfigFilePath(){
    if(configPath == null){
      configPath = POSTGRES_LOCALHOST_CONFIG;
    }
    return configPath;
  }

  // will return null on exception
  public static PostgresClient getInstance(Vertx vertx) {
    // assumes a single thread vertx model so no sync needed
    if (instance == null) {
      try {
        instance = new PostgresClient(vertx, DEFAULT_SCHEMA);
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    }
    return instance;
  }

  // will return null on exception
  public static PostgresClient getInstance(Vertx vertx, String tenantId) {
    // assumes a single thread vertx model so no sync needed
    if(!connectionPool.containsKey(tenantId)){
      try {
        connectionPool.put(tenantId, new PostgresClient(vertx, tenantId));
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    }
    return connectionPool.get(tenantId);
  }

  private void init(Vertx vertx, String tenantId) throws Exception {
    this.vertx = vertx;
    log.info("Loading PostgreSQL configuration from " + getConfigFilePath());
    postgreSQLClientConfig = new LoadConfs().loadConfig(getConfigFilePath());
    if(postgreSQLClientConfig == null){
      if (embeddedMode) {
        //embedded mode, if no config passed use defaults
        postgreSQLClientConfig = new JsonObject();
        postgreSQLClientConfig.put(USERNAME, USERNAME);
        postgreSQLClientConfig.put(PASSWORD, PASSWORD);
        postgreSQLClientConfig.put("host", "127.0.0.1");
        postgreSQLClientConfig.put("port", 6000);
        postgreSQLClientConfig.put("database", DEFAULT_SCHEMA);
      }
      else{
        //not in embedded mode but there is no conf file found
        throw new Exception("No postgres-conf.json file found and not in embedded mode, can not connect to any database");
      }
    }
    else{
      postgreSQLClientConfig.put("database", tenantId);
    }
    client = io.vertx.ext.asyncsql.PostgreSQLClient.createNonShared(vertx, postgreSQLClientConfig);
  }

  public JsonObject getConnectionConfig(){
    return postgreSQLClientConfig;
  }

  public static String pojo2json(Object entity) throws Exception {
    // SimpleModule module = new SimpleModule();
    // module.addSerializer(entity.getClass(), new PoJoJsonSerializer());
    // mapper.registerModule(module);
    if (entity != null) {
      if (entity instanceof JsonObject) {
        return ((JsonObject) entity).encode();
      } else {
        try {
          return mapper.writeValueAsString(entity);
        } catch (JsonProcessingException e) {
          log.error(e);
        }
      }
    }
    throw new Exception("Entity can not be null");
  }

  /**
   * end transaction must be called or the connection will remain open
   *
   * @param done
   */
  //@Timer
  public void startTx(Handler<Object> done) {
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
          connection.setAutoCommit(false, res1 -> {
            if (res1.failed()) {
              if (connection != null) {
                connection.close();
              }
              done.handle(io.vertx.core.Future.failedFuture(res1.cause().getMessage()));
            } else {
              done.handle(io.vertx.core.Future.succeededFuture(connection));
            }
          });
        } catch (Exception e) {
          log.error(e);
          if (connection != null) {
            connection.close();
          }
          done.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        }
      }
    });
  }

  //@Timer
  @SuppressWarnings("unchecked")
  public void rollbackTx(Object conn, Handler<Object> done) {
    SQLConnection sqlConnection =  ((io.vertx.core.Future<SQLConnection>) conn).result();
    sqlConnection.rollback(res -> {
      if (res.failed()) {
        sqlConnection.close();
        throw new RuntimeException(res.cause());
      } else {
        sqlConnection.close();
      }
      done.handle(null);
    });
  }

  //@Timer
  @SuppressWarnings("unchecked")
  public void endTx(Object conn, Handler<Object> done) {
    SQLConnection sqlConnection = ((io.vertx.core.Future<SQLConnection>) conn).result();
    sqlConnection.commit(res -> {
      if (res.failed()) {
        sqlConnection.close();
        throw new RuntimeException(res.cause());
      } else {
        sqlConnection.close();
      }
      done.handle(null);
    });
  }

  /**
   *
   * @param table
   *          - schema.tablename to save to
   * @param json
   *          - this must be a json object
   * @param replyHandler
   * @throws Exception
   */
  public void save(String table, Object entity, Handler<AsyncResult<String>> replyHandler) throws Exception {

    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
          connection.queryWithParams("INSERT INTO " + table + " (" + DEFAULT_JSONB_FIELD_NAME + ") VALUES (?::JSON) RETURNING _id",
            new JsonArray().add(pojo2json(entity)), query -> {
              if (query.failed()) {
                replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
              } else {
                replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result().getResults().get(0).getValue(0).toString()));
              }
              connection.close();
            });
        } catch (Exception e) {
          log.error(e);
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        }
      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  @SuppressWarnings("unchecked")
  public void save(Object sqlConnection, String table, Object entity, Handler<AsyncResult<String>> replyHandler) throws Exception {
    log.debug("save called on " + table);
    // connection not closed by this FUNCTION ONLY BY END TRANSACTION call!
    SQLConnection connection = ((io.vertx.core.Future<SQLConnection>) sqlConnection).result();
    try {
      connection.queryWithParams("INSERT INTO " + table + " (" + DEFAULT_JSONB_FIELD_NAME + ") VALUES (?::JSON) RETURNING _id",
        new JsonArray().add(pojo2json(entity)), query -> {
          if (query.failed()) {
            replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
          } else {
            replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result().getResults().get(0).getValue(0).toString()));
          }
          connection.close();
        });
    } catch (Exception e) {
      log.error(e);
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
    }
  }

  /**
   * update a specific record associated with the key passed in the id arg
   * @param table - table to save to (must exist)
   * @param entity - pojo to save
   * @param id - key of the entitiy being updated
   * @param replyHandler
   * @throws Exception
   */
  public void update(String table, Object entity, String id, Handler<AsyncResult<String>> replyHandler) throws Exception {
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
          connection.update(UPDATE + table + SET + DEFAULT_JSONB_FIELD_NAME + " = '" + pojo2json(entity) + "' WHERE " + ID_FIELD
              + "=" + id, query -> {
            if (query.failed()) {
              replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
            } else {
              replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result().toJson().toString()));
            }
            connection.close();

          });
        } catch (Exception e) {
          log.error(e);
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        }
      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  /**
   * Update 1...n records matching the filter
   * <br>
   * Criterion Examples:
   * <br>
   * 1. can be mapped from a string in the following format [{"field":"''","value":"","op":""}]
   * <pre>
   *    Criterion a = json2Criterion("[{\"field\":\"'fund_distributions'->[]->'amount'->>'sum'\",\"value\":120,\"op\":\"<\"}]"); //denotes funds_distribution is an array of objects
   *    Criterion a = json2Criterion("[{"field":"'po_line_status'->>'value'","value":"SENT","op":"like"},{"field":"'owner'->>'value'","value":"MITLIBMATH","op":"="}, {"op":"AND"}]");
   *    (see postgres query syntax for more examples in the read.me
   * </pre>
   * 2. Simple Criterion
   * <pre>
   *    Criteria b = new Criteria();
   *    b.field.add("'note'");
   *    b.operation = "=";
   *    b.value = "a";
   *    b.isArray = true; //denotes that the queried field is an array with multiple values
   *    Criterion a = new Criterion(b);
   * </pre>
   * 3. For a boolean field called rush = false OR note[] contains 'a'
   * <pre>
   *    Criteria d = new Criteria();
   *    d.field.add("'rush'");
   *    d.operation = Criteria.OP_IS_FALSE;
   *    d.value = null;
   *    Criterion a = new Criterion();
   *    a.addCriterion(d, Criteria.OP_OR, b);
   * </pre>
   * 4. for the following json:
   * <pre>
   *      "price": {
   *        "sum": "150.0",
   *         "po_currency": {
   *           "value": "USD",
   *           "desc": "US Dollar"
   *         }
   *       },
   *
   *    Criteria c = new Criteria();
   *    c.addField("'price'").addField("'po_currency'").addField("'value'");
   *    c.operation = Criteria.OP_LIKE;
   *    c.value = "USD";
   *
   * </pre>
   * @param table - table to update
   * @param entity - pojo to set for matching records
   * @param filter - see example below
   * @param returnUpdatedIds - return ids of updated records
   * @param replyHandler
   * @throws Exception
   *
   */
  public void update(String table, Object entity, Criterion filter, boolean returnUpdatedIds, Handler<AsyncResult<String>> replyHandler)
      throws Exception {
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        StringBuilder sb = new StringBuilder();
        if (filter != null) {
          sb.append(filter.toString());
        }
        StringBuilder returning = new StringBuilder();
        if (returnUpdatedIds) {
          returning.append(RETURNING_IDS);
        }
        try {
          connection.update(UPDATE + table + SET + DEFAULT_JSONB_FIELD_NAME + " = '" + pojo2json(entity) + "' " + sb.toString()
              + " " + returning, query -> {
            if (query.failed()) {
              log.error(query.cause());
              replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
            } else {
              replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result().toJson().toString()));
            }
            connection.close();
          });
        } catch (Exception e) {
          log.error(e);
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        }

      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  /**
   * update a section / field / object in the pojo -
   * <br>
   * for example:
   * <br> if a json called po_line contains the following field
   * <pre>
   *     "po_line_status": {
   *       "value": "SENT",
   *       "desc": "sent to vendor"
   *     },
   * </pre>
   *  this translates into a po_line_status object within the po_line object - to update the entire object / section
   *  create an updateSection object pushing into the section the po line status as the field and the value (string / json / etc...) to replace it with
   *  <pre>
   *  a = new UpdateSection();
   *  a.addField("po_line_status");
   *  a.setValue(new JsonObject("{\"value\":\"SOMETHING_NEW4\",\"desc\":\"sent to vendor again\"}"));
   *  </pre>
   * Note that postgres does not update inplace the json but rather will create a new json with the
   * updated section and then reference the id to that newly created json
   * <br>
   * Queries generated will look something like this:
   * <pre>
   *
   * update test.po_line set jsonb = jsonb_set(jsonb, '{po_line_status}', '{"value":"SOMETHING_NEW4","desc":"sent to vendor"}') where _id = 19;
   * update test.po_line set jsonb = jsonb_set(jsonb, '{po_line_status, value}', '"SOMETHING_NEW5"', false) where _id = 15;
   * </pre>
   *
   * @param table - table to update
   * @param section - see UpdateSection class
   * @param when - Criterion object
   * @param replyHandler
   * @throws Exception
   *
   */
  public void update(String table, UpdateSection section, Criterion when, boolean returnUpdatedIdsCount,
      Handler<AsyncResult<String>> replyHandler) throws Exception {
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        StringBuilder sb = new StringBuilder();
        if (when != null) {
          sb.append(when.toString());
        }
        StringBuilder returning = new StringBuilder();
        if (returnUpdatedIdsCount) {
          returning.append(RETURNING_IDS);
        }
        try {
          connection.update(UPDATE + table + SET + DEFAULT_JSONB_FIELD_NAME + " = jsonb_set(" + DEFAULT_JSONB_FIELD_NAME + ","
              + section.getFieldsString() + ", '" + section.getValue() + "', false) " + sb.toString() + " " + returning, query -> {
            if (query.failed()) {
              replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
            } else {
              replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result().toJson().toString()));
            }
            connection.close();
          });
        } catch (Exception e) {
          log.error(e);
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        }
      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  /**
   * Delete based on id of record - the id is not in the json object but is a separate column
   * @param table
   * @param id
   * @param replyHandler
   * @throws Exception
   */
  public void delete(String table, String id, Handler<AsyncResult<String>> replyHandler) throws Exception {
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
          connection.update("DELETE FROM " + table + " WHERE " + ID_FIELD + "=" + id, query -> {
            if (query.failed()) {
              replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
            } else {
              replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result().toJson().toString()));
            }
            connection.close();
          });
        } catch (Exception e) {
          log.error(e);
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        }
      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  /**
   * Delete based on filter
   * @param table
   * @param filter
   * @param replyHandler
   * @throws Exception
   */
  public void delete(String table, Criterion filter, Handler<AsyncResult<String>> replyHandler) throws Exception {
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        StringBuilder sb = new StringBuilder();
        if (filter != null) {
          sb.append(filter.toString());
        }
        try {
          connection.update("DELETE FROM " + table + sb.toString(), query -> {
            if (query.failed()) {
              replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
            } else {
              replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result().toJson().toString()));
            }
            connection.close();

          });
        } catch (Exception e) {
          log.error(e);
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        }
      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  /**
   * pass in an entity that is fully / partially populated and the query will return all records matching the
   * populated fields in the entity
   *
   * @param table
   * @param entity
   * @param replyHandler
   * @throws Exception
   */
  //@Timer
  public void get(String table, Object entity, boolean returnCount, Handler<AsyncResult<Object[]>> replyHandler) throws Exception {
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
          String select = "SELECT ";
          if (returnCount) {
            select = select + COUNT_CLAUSE;
          }
          connection.query(select + DEFAULT_JSONB_FIELD_NAME + "," + ID_FIELD + " FROM " + table + " WHERE " + DEFAULT_JSONB_FIELD_NAME
              + "@>'" + pojo2json(entity) + "' ", query -> {
            if (query.failed()) {
              replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
            } else {
              replyHandler.handle(io.vertx.core.Future.succeededFuture(processResult(query.result(), entity.getClass(), returnCount)));
            }
          });
        } catch (Exception e) {
          log.error(e);
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        } finally {
          connection.close();
        }

      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  /**
   * select query
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - see Criterion class
   * @param returnCount - whether to return the amount of records matching the query
   * @param replyHandler
   * @throws Exception
   */
  public void get(String table, Class<?> clazz, Criterion filter, boolean returnCount, Handler<AsyncResult<Object[]>> replyHandler)
      throws Exception {
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        StringBuilder sb = new StringBuilder();
        StringBuilder fromClauseFromCriteria = new StringBuilder();
        if (filter != null) {
          sb.append(filter.toString());
          fromClauseFromCriteria.append(filter.from2String());
          if (fromClauseFromCriteria.length() > 0) {
            fromClauseFromCriteria.insert(0, ",");
          }
        }
        try {
          String select = "SELECT ";
          if (returnCount) {
            select = select + COUNT_CLAUSE;
          }
          connection.query(select + DEFAULT_JSONB_FIELD_NAME + "," + ID_FIELD + " FROM " + table + fromClauseFromCriteria + sb, query -> {
            if (query.failed()) {
              replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
            } else {
              replyHandler.handle(io.vertx.core.Future.succeededFuture(processResult(query.result(), clazz, returnCount)));
            }
          });
        } catch (Exception e) {
          log.error(e);
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        } finally {
          connection.close();
        }

      } else {
        log.error(res.cause().getMessage(), res.cause());
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  private Object[] processResult(io.vertx.ext.sql.ResultSet rs, Class<?> clazz, boolean count) {
    Object[] ret = new Object[2];
    List<Object> list = new ArrayList<>();
    List<JsonObject> tempList = rs.getRows();
    int rowCount = rs.getNumRows();
    if (rowCount > 0 && count) {
      rowCount = rs.getResults().get(0).getInteger(0);
    }
    for (int i = 0; i < tempList.size(); i++) {
      try {
        Object jo = tempList.get(i).getValue(DEFAULT_JSONB_FIELD_NAME);
        Object id = tempList.get(i).getValue(ID_FIELD);
        Object o = mapper.readValue(jo.toString(), clazz);
        o.getClass().getMethod("setId", new Class[] { String.class }).invoke(o, new String[] { id.toString() });
        list.add(o);
      } catch (Exception e) {
        log.error(e);
      }
    }
    ret[0] = list;
    ret[1] = rowCount;
    return ret;
  }

  /**
   * run a select query against postgres - to update see mutate
   * @param sql - the sql to run
   * @param replyHandler
   */
  public void select(String sql, Handler<AsyncResult<io.vertx.ext.sql.ResultSet>> replyHandler) {

    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
          connection.query(sql, query -> {
            if (query.failed()) {
              replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
            } else {
              replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result()));
            }
            connection.close();
          });
        } catch (Exception e) {
          log.error(e);
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        }
      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  /**
   * update table
   * @param sql - the sql to run
   * @param replyHandler
   */
  public void mutate(String sql, Handler<AsyncResult<String>> replyHandler)  {
    long s = System.nanoTime();
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
          connection.update(sql, query -> {
            if (query.failed()) {
              replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
            } else {
              replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result().toString()));
            }
            connection.close();
            log.debug("mutate timer: " + sql + " took " + (System.nanoTime()-s)/1000000);
          });
        } catch (Exception e) {
          log.error(e);
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        }
      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  /**
   * send a query to update within a transaction
   * @param conn - connection - see startTx
   * @param sql - the sql to run
   * @param replyHandler
   * Example:
   *  postgresClient.startTx(beginTx -> {
   *        try {
   *          postgresClient.mutate(beginTx, sql, reply -> {...
   */
  @SuppressWarnings("unchecked")
  public void mutate(Object conn, String sql, Handler<AsyncResult<String>> replyHandler){
    SQLConnection sqlConnection = ((io.vertx.core.Future<SQLConnection>) conn).result();
    try {
      sqlConnection.update(sql, query -> {
        if (query.failed()) {
          replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
        } else {
          replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result().toString()));
        }
        sqlConnection.close();
      });
    } catch (Exception e) {
      log.error(e);
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
    }
  }

  // JsonNode node =
  // mapper.readTree(PostgresJSONBCRUD.getInstance(vertxContext.owner()).pojo2json(entity));
  // printout(node.fields(), new StringBuilder("jsonb"));
  private void printout(Iterator<Entry<String, JsonNode>> node, StringBuilder parent) {
    while (node.hasNext()) {
      Map.Entry<String, JsonNode> entry = node.next();

      StringBuilder sb = new StringBuilder();
      sb.append(parent).append("->").append(entry.getKey());
      JsonNode jno = entry.getValue();

      if (jno.isContainerNode()) {
        printout(jno.fields(), sb);
      } else {
        int i = sb.lastIndexOf("->");
        String a = sb.substring(0, i);
        String b = sb.substring(i + 2);
        StringBuilder sb1 = new StringBuilder();
        if (jno.isTextual()) {
          sb1.append("'" + jno.textValue() + "'");
        } else if (jno.isNumber()) {
          sb1.append(jno.numberValue());
        } else if (jno.isNull()) {
          sb1.append("null");
        } else if (jno.isBoolean()) {
          sb1.append(jno.booleanValue());
        } else {
          // TODO handle binary data???
        }
        sb = new StringBuilder(a).append("->>").append(b);
        if (sb1.length() > 2) {
          sb.append("=").append(sb1);
          System.out.println(sb.toString());
        }
      }
    }
  }

  public void startEmbeddedPostgres() throws Exception {
    // starting Postgres
    embeddedMode = true;
    if (postgresProcess == null || !postgresProcess.isProcessRunning()) {
      // turns off the default functionality of unzipping on every run.
      IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
        .defaults(Command.Postgres)
        .artifactStore(
          new ArtifactStoreBuilder().defaults(Command.Postgres).download(new DownloadConfigBuilder().defaultsForCommand(Command.Postgres)
          // .progressListener(new LoggingProgressListener(logger, Level.ALL))
            .build())).build();
      PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getInstance(runtimeConfig);

      int port = postgreSQLClientConfig.getInteger("port");
      String username = postgreSQLClientConfig.getString(USERNAME);
      String password = postgreSQLClientConfig.getString(PASSWORD);
      String database = postgreSQLClientConfig.getString("database");

      final PostgresConfig config = new PostgresConfig(Version.V9_5_0, new AbstractPostgresConfig.Net("127.0.0.1", port),
        new AbstractPostgresConfig.Storage(database), new AbstractPostgresConfig.Timeout(20000), new AbstractPostgresConfig.Credentials(
          username, password));

      postgresProcess = runtime.prepare(config).start();

      log.info("embedded postgress started....");
    } else {
      log.info("embedded postgress is already running...");
    }
  }

  /**
   * .sql files
   * @param path
   * @throws Exception
   */
  public void importFileEmbedded(String path) throws Exception {
    // starting Postgres
    if (embeddedMode) {
      if (postgresProcess != null && postgresProcess.isProcessRunning()) {
        log.info("embedded postgress import starting....");

        postgresProcess.importFromFile(new File(path));

        log.info("embedded postgress import complete....");
      } else {
        log.info("embedded postgress is not running...");
      }
    } else {
      // TODO
    }

  }

  /**
   * import data in a tab delimited file into columns of an existing table
   * @param path - path to the file
   * @param tableName - name of the table to import the content into
   */
  public void importFile(String path, String tableName) {

   long recordsImported[] = new long[]{-1};
   vertx.<String>executeBlocking(dothis -> {

    try {
      String host = postgreSQLClientConfig.getString("host");
      int port = postgreSQLClientConfig.getInteger("port");
      String user = postgreSQLClientConfig.getString(USERNAME);
      String pass = postgreSQLClientConfig.getString(PASSWORD);
      String db = postgreSQLClientConfig.getString("database");

      log.info("Connecting to " + db);

      Connection con = DriverManager.getConnection(
        "jdbc:postgresql://"+host+":"+port+"/"+db, user , pass);

      log.info("Copying text data rows from stdin");

      CopyManager copyManager = new CopyManager((BaseConnection) con);

      FileReader fileReader = new FileReader(path);
      recordsImported[0] = copyManager.copyIn("COPY "+tableName+" FROM STDIN", fileReader );

    } catch (Exception e) {
      log.error(messages.getMessage("en", MessageConsts.ImportFailed), e.getMessage(), e);
      dothis.fail(e.getMessage());
    }
    dothis.complete("Done.");

  }, whendone -> {

    if(whendone.succeeded()){

      log.info("Done importing file: " + path + ". Number of records imported: " + recordsImported[0]);
    }
    else{
      log.info("Failed importing file: " + path);
    }

  });

  }

  public static void stopEmbeddedPostgres() {
    if (postgresProcess != null) {
      LogUtil.formatLogMessage(PostgresClient.class.getName(), "stopEmbeddedPostgres", "called stop on embedded postgress ...");
      postgresProcess.stop();
      embeddedMode = false;
    }
  }

}
