package org.folio.rest.persist;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.UpdateSection;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.LogUtil;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import de.flapdoodle.embed.process.config.IRuntimeConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.sql.SQLConnection;

/**
 * @author shale currently does not support binary data
 *
 */
public class PostgresClient {

  private static PostgresClient  instance;
  private AsyncSQLClient         client;
  private static ObjectMapper    mapper                   = new ObjectMapper();

  public final static String     DEFAULT_JSONB_FIELD_NAME = "jsonb";

  public final static String     ID_FIELD                 = "_id";

  private final static String    COUNT_CLAUSE             = " count(_id) OVER() AS count, ";
  private final static String    RETURNING_IDS            = " RETURNING _id ";

  private final static String    POSTGRES_LOCALHOST_CONFIG = "/postgres-conf.json";
  
  private static PostgresProcess postgresProcess          = null;

  private Vertx                  vertx                    = null;

  private static boolean         embeddedMode             = false;
  private static String          configPath               = null;

  private static int             EMBEDDED_POSTGRES_PORT   = 6000;
  
  private JsonObject postgreSQLClientConfig = null;
  
  private static final Logger log = LoggerFactory.getLogger(PostgresClient.class);

  private final Messages            messages = Messages.getInstance();

  private PostgresClient(Vertx vertx) throws Exception {
    init(vertx);
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
  
  // will return null on exception
  public static PostgresClient getInstance(Vertx vertx) {
    // assumes a single thread vertx model so no sync needed
    if (instance == null) {
      try {
        instance = new PostgresClient(vertx);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return instance;
  }

  private void init(Vertx vertx) throws Exception {
    this.vertx = vertx;
    if (!embeddedMode) {
      String path = POSTGRES_LOCALHOST_CONFIG;
      if(configPath != null){
        path = configPath;
        log.info("Loading PostgreSQL configuration from " + configPath);
      }
      postgreSQLClientConfig = new LoadConfs().loadConfig(path);
      if(postgreSQLClientConfig == null){
        //not in embedded mode but there is no conf file found
        throw new Exception("No postgres-conf.json file found and not in embedded mode, can not connect to any db store");
      }
      else{
        client = io.vertx.ext.asyncsql.PostgreSQLClient.createNonShared(vertx, postgreSQLClientConfig);
      }
    }
  }

  private String pojo2json(Object entity) throws Exception {
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
          e.printStackTrace();
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
          e.printStackTrace();
          if (connection != null) {
            connection.close();
          }
          done.handle(io.vertx.core.Future.failedFuture(e.getMessage()));

        }
      }
      ;
    });
  }

  //@Timer
  @SuppressWarnings("unchecked")
  public void rollbackTx(Object conn, Handler<Object> done) {
    SQLConnection sqlConnection = (SQLConnection) ((io.vertx.core.Future<SQLConnection>) conn).result();
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
    SQLConnection sqlConnection = (SQLConnection) ((io.vertx.core.Future<SQLConnection>) conn).result();
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
  //@Timer
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
          e.printStackTrace();
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        } finally {
        }

      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  //@Timer
  @SuppressWarnings("unchecked")
  public void save(Object sqlConnection, String table, Object entity, Handler<AsyncResult<String>> replyHandler) throws Exception {
    System.out.println("save on -----> " + table);
    // connection not closed by this FUNCTION ONLY BY END TRANSACTION call!
    SQLConnection connection = (SQLConnection) ((io.vertx.core.Future<SQLConnection>) sqlConnection).result();
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
      e.printStackTrace();
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
    }
  }
  
  //@Timer
  public void update(String table, Object entity, String id, Handler<AsyncResult<String>> replyHandler) throws Exception {
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
          connection.update("UPDATE " + table + " SET " + DEFAULT_JSONB_FIELD_NAME + " = '" + pojo2json(entity) + "' WHERE " + ID_FIELD
              + "=" + id, query -> {
            if (query.failed()) {
              replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
            } else {
              replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result().toJson().toString()));
            }
            connection.close();

          });
        } catch (Exception e) {
          e.printStackTrace();
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        } finally {
        }

      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }
  
  //@Timer
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
          connection.update("UPDATE " + table + " SET " + DEFAULT_JSONB_FIELD_NAME + " = '" + pojo2json(entity) + "' " + sb.toString()
              + " " + returning, query -> {
            if (query.failed()) {
              query.cause().printStackTrace();
              replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
            } else {
              replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result().toJson().toString()));
            }
            connection.close();
          });
        } catch (Exception e) {
          e.printStackTrace();
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        } finally {

        }

      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  /**
   * Note that postgrs does not update inplace the json but rather will create a new json with the updated section and then reference the id
   * to that newly created json
   * 
   * @param table
   * @param section
   * @param when
   * @param replyHandler
   * @throws Exception
   */
  //@Timer
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
          connection.update("UPDATE " + table + " SET " + DEFAULT_JSONB_FIELD_NAME + " = jsonb_set(" + DEFAULT_JSONB_FIELD_NAME + ","
              + section.getFieldsString() + ", '" + section.getValue() + "', false) " + sb.toString() + " " + returning, query -> {
            if (query.failed()) {
              replyHandler.handle(io.vertx.core.Future.failedFuture(query.cause().getMessage()));
            } else {
              replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result().toJson().toString()));
            }
            connection.close();
          });
        } catch (Exception e) {
          e.printStackTrace();
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        } finally {
        }

      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  //@Timer
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
          e.printStackTrace();
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        } finally {
        }

      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  //@Timer
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
          e.printStackTrace();
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        } finally {
        }

      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  /**
   * pass in an entity that is fully / partially populated and the query will return all records matching the populated fields in the entity
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
          e.printStackTrace();
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        } finally {
          connection.close();
        }

      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  //@Timer
  public void get(String table, Class clazz, Criterion filter, boolean returnCount, Handler<AsyncResult<Object[]>> replyHandler)
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
          e.printStackTrace();
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        } finally {
          connection.close();
        }

      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }

  private Object[] processResult(io.vertx.ext.sql.ResultSet rs, Class<?> clazz, boolean count) {
    Object[] ret = new Object[2];
    List<Object> list = new ArrayList<Object>();
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
        e.printStackTrace();
      }
    }
    ret[0] = list;
    ret[1] = rowCount;
    return ret;
  }
  
  //@Timer
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
          e.printStackTrace();
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        } finally {
        }

      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
  }
  
  //@Timer
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
              System.out.println("real timer: " + (System.nanoTime()-s)/1000000);

              replyHandler.handle(io.vertx.core.Future.succeededFuture(query.result().toString()));
            }
            connection.close();

          });
        } catch (Exception e) {
          e.printStackTrace();
          replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
        } finally {
        }

      } else {
        replyHandler.handle(io.vertx.core.Future.failedFuture(res.cause().getMessage()));
      }
    });
    long e = System.nanoTime();
    System.out.println("internal timer: " + (e-s)/1000000);
  }

  //@Timer
  @SuppressWarnings("unchecked")
  public void mutate(Object conn, String sql, Handler<AsyncResult<String>> replyHandler){
    SQLConnection sqlConnection = (SQLConnection) ((io.vertx.core.Future<SQLConnection>) conn).result();
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
      e.printStackTrace();
      replyHandler.handle(io.vertx.core.Future.failedFuture(e.getMessage()));
    } finally {
    }
  }

  // JsonNode node =
  // mapper.readTree(PostgresJSONBCRUD.getInstance(vertxContext.owner()).pojo2json(entity));
  // printout(node.fields(), new StringBuilder("jsonb"));
  private void printout(Iterator<Entry<String, JsonNode>> node, StringBuilder parent) {
    while (node.hasNext()) {
      Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) node.next();

      StringBuilder sb = new StringBuilder();
      sb.append(parent).append("->").append(entry.getKey());
      JsonNode jno = entry.getValue();

      if (jno.isContainerNode()) {
        printout(jno.fields(), sb);
      } else {
        int i = sb.lastIndexOf("->");
        String a = sb.substring(0, i);
        String b = sb.substring(i + 2);
        StringBuffer sb1 = new StringBuffer();
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

  //@Timer
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
      // PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getDefaultInstance();
      PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getInstance(runtimeConfig);
      // final PostgresConfig config = PostgresConfig.defaultWithDbName("postgres", "username", "password");

      final PostgresConfig config = new PostgresConfig(Version.V9_5_0, new AbstractPostgresConfig.Net("127.0.0.1", EMBEDDED_POSTGRES_PORT),
        new AbstractPostgresConfig.Storage("postgres"), new AbstractPostgresConfig.Timeout(20000), new AbstractPostgresConfig.Credentials(
          "username", "password"));

      postgresProcess = runtime.prepare(config).start();
      postgreSQLClientConfig = new JsonObject();

      postgreSQLClientConfig.put("host", postgresProcess.getConfig().net().host());
      postgreSQLClientConfig.put("port", postgresProcess.getConfig().net().port());
      postgreSQLClientConfig.put("username", postgresProcess.getConfig().credentials().username());
      postgreSQLClientConfig.put("password", postgresProcess.getConfig().credentials().password());
      postgreSQLClientConfig.put("database", postgresProcess.getConfig().storage().dbName());

      client = io.vertx.ext.asyncsql.PostgreSQLClient.createNonShared(vertx, postgreSQLClientConfig);

      LogUtil.formatLogMessage(this.getClass().getName(), "startEmbeddedPostgres", "embedded postgress started....");
    } else {
      LogUtil.formatLogMessage(this.getClass().getName(), "startEmbeddedPostgres", "embedded postgress is already running...");
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
        LogUtil.formatLogMessage(this.getClass().getName(), "startEmbeddedPostgres", "embedded postgress import starting....");

        postgresProcess.importFromFile(new File(path));

        LogUtil.formatLogMessage(this.getClass().getName(), "startEmbeddedPostgres", "embedded postgress import complete....");
      } else {
        LogUtil.formatLogMessage(this.getClass().getName(), "startEmbeddedPostgres", "embedded postgress is not running...");
      }
    } else {
      // TODO
    }

  }
  
  public void importFile(String path, String tableName) {
  
   vertx.<String>executeBlocking(dothis -> {
  
    try {
      String host = postgreSQLClientConfig.getString("host");
      int port = postgreSQLClientConfig.getInteger("port");
      String user = postgreSQLClientConfig.getString("username");
      String pass = postgreSQLClientConfig.getString("password");
      String db = postgreSQLClientConfig.getString("database");
      
      log.info("Connecting to " + db);
      
      Connection con = DriverManager.getConnection(
        "jdbc:postgresql://"+host+":"+port+"/"+db, user , pass);
 
      log.info("Copying text data rows from stdin");
 
      CopyManager copyManager = new CopyManager((BaseConnection) con);
 
      FileReader fileReader = new FileReader(path);
      copyManager.copyIn("COPY "+tableName+" FROM STDIN", fileReader );
      
    } catch (Exception e) {
      log.error(messages.getMessage("en", MessageConsts.ImportFailed), e.getMessage());
      dothis.fail(e.getMessage());
    }
    dothis.complete("Done.");
    
  }, whendone -> {
    
    if(whendone.succeeded()){
      
      log.info("Done importing file: " + path);
    }
    else{
      log.info("Failed importing file: " + path);
    }
    
  });

  }

  public static void stopEmbeddedPostgres() {
    LogUtil.formatLogMessage(PostgresClient.class.getName(), "stopEmbeddedPostgres", "called stop on embedded postgress ...");
    if (postgresProcess != null) {
      postgresProcess.stop();
      embeddedMode = false;
    }
  }

}
