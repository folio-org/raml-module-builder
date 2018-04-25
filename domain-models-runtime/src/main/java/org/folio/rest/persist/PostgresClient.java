package org.folio.rest.persist;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.SecretKey;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.folio.rest.jaxrs.model.ResultInfo;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.Criteria.UpdateSection;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.facets.FacetField;
import org.folio.rest.persist.facets.FacetManager;
import org.folio.rest.persist.facets.ParsedQuery;
import org.folio.rest.persist.helpers.JoinBy;
import org.folio.rest.persist.interfaces.Results;
import org.folio.rest.security.AES;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.monitor.StatsTracker;
import org.folio.rest.tools.utils.Envs;
import org.folio.rest.tools.utils.LogUtil;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.tools.utils.ResourceUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;

import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.store.NonCachedPostgresArtifactStoreBuilder;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import ru.yandex.qatools.embed.postgresql.Command;
import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;
import ru.yandex.qatools.embed.postgresql.config.PostgresDownloadConfigBuilder;
import ru.yandex.qatools.embed.postgresql.config.RuntimeConfigBuilder;
import ru.yandex.qatools.embed.postgresql.distribution.Version;

/**
 * @author shale
 *
 * currently does not support binary data unless base64 encoded
 */
public class PostgresClient {

  public static final String     DEFAULT_SCHEMA           = "public";
  public static final String     DEFAULT_JSONB_FIELD_NAME = "jsonb";

  private static final String    POSTGRES_LOCALHOST_CONFIG = "/postgres-conf.json";
  private static final int       EMBEDDED_POSTGRES_PORT   = 6000;

  private static final String   UPDATE = "UPDATE ";
  private static final String   SET = " SET ";
  private static final String   WHERE = " WHERE ";
  private static final String INSERT_CLAUSE = "INSERT INTO ";

  private static final String   _PASSWORD = "password"; //NOSONAR
  private static final String   _USERNAME = "username";
  private static final String   HOST     = "host";
  private static final String   PORT     = "port";
  private static final String   DATABASE = "database";
  private static final String   DEFAULT_IP = "127.0.0.1"; //NOSONAR

  private static final String    STATS_KEY                = PostgresClient.class.getName();

  private static PostgresProcess postgresProcess          = null;
  private static boolean         embeddedMode             = false;
  private static String          configPath               = null;
  private static ObjectMapper    mapper                   = ObjectMapperTool.getMapper();
  private static MultiKeyMap<Object, PostgresClient> connectionPool = MultiKeyMap.multiKeyMap(new HashedMap<>());
  private static String moduleName                        = null;

  private static final String CLOSE_FUNCTION_POSTGRES = "WINDOW|IMMUTABLE|STABLE|VOLATILE|"
      +"CALLED ON NULL INPUT|RETURNS NULL ON NULL INPUT|STRICT|"
      +"SECURITY INVOKER|SECURITY DEFINER|SET\\s.*|AS\\s.*|COST\\s\\d.*|ROWS\\s.*";

  private static final Pattern POSTGRES_IDENTIFIER = Pattern.compile("^[a-zA-Z_][0-9a-zA-Z_]{0,62}$");

  private static final List<Map.Entry<String,Pattern>> REMOVE_FROM_COUNT_ESTIMATE= new java.util.ArrayList<>();

  private static final Logger log = LogManager.getLogger(PostgresClient.class);

  private static int embeddedPort            = -1;

  private Vertx vertx                       = null;
  private JsonObject postgreSQLClientConfig = null;
  private final Messages messages           = Messages.getInstance();
  private AsyncSQLClient         client;
  private String tenantId;
  private String idField                     = "_id";
  private String countClauseTemplate         = " ${tenantId}.count_estimate_smart('${query}') AS count, ";
  private String returningIdTemplate         = " RETURNING ${id} ";
  private String returningId                 = " RETURNING _id ";

  static {
    REMOVE_FROM_COUNT_ESTIMATE.add(new SimpleEntry<>("LIMIT", Pattern.compile("LIMIT\\s+[\\d]+(?=(([^']*'){2})*[^']*$)", 2)));
    REMOVE_FROM_COUNT_ESTIMATE.add(new SimpleEntry<>("OFFSET", Pattern.compile("OFFSET\\s+[\\d]+(?=(([^']*'){2})*[^']*$)", 2)));
    REMOVE_FROM_COUNT_ESTIMATE.add(new SimpleEntry<>("ORDER BY", Pattern.compile("ORDER BY(([^']*'){2})*\\s+(desc|asc|)", 2)));
  }

  protected PostgresClient(Vertx vertx, String tenantId) throws Exception {
    init(vertx, tenantId);
  }

  public void setIdField(String id){
    idField = id;
    Map<String, String> replaceMapping = new HashMap<>();
    replaceMapping.put("id", idField);
    StrSubstitutor sub = new StrSubstitutor(replaceMapping);
    returningId = sub.replace(returningIdTemplate);
  }

  /**
   * Enable or disable using embedded specific defaults for the
   * PostgreSQL configuration. They are used if there is no
   * postgres json config file.
   * <p>
   * This function must be invoked before calling the constructor.
   * <p>
   * The embedded specific defaults are:
   * <ul>
   * <li><code>username = "username"</code></li>
   * <li><code>password = "password"</code></li>
   * <li><code>host = "127.0.0.1"</code></li>
   * <li><code>port = 6000</code></li>
   * <li><code>database = "postgres"</code></li>
   * </ul>
   *
   * @param embed - whether to use embedded specific defaults
   */
  public static void setIsEmbedded(boolean embed){
    embeddedMode = embed;
  }

  /**
   * Set the port that overwrites to port of the embedded PostgreSQL.
   * This port overwrites any default port and any port set in the
   * PostgreSQL configuration file. It is only used when <code>isEmbedded() == true</code>
   * when invoking the constructor.
   * <p>
   * This function must be invoked before calling the constructor.
   * <p>
   * Use -1 to not overwrite the port.
   *
   * @param port  the port for embedded PostgreSQL, or -1 to not overwrite the port
   */
  public static void setEmbeddedPort(int port){
    embeddedPort = port;
  }

  /**
   * True if embedded specific defaults for the
   * PostgreSQL configuration should be used if there is no
   * postgres json config file.
   * @return true for using embedded specific defaults
   * @see #setIsEmbedded(boolean)
   */
  public static boolean isEmbedded(){
    return embeddedMode;
  }

  /**
   * Set the path to the PostgreSQL connection configuration,
   * must be called before getInstance() to take affect.
   * <p>
   * This function must be invoked before calling the constructor.
   *
   * @param path  new path, or null to use the default path "/postgres-conf.json"
   */
  public static void setConfigFilePath(String path){
    configPath = path;
  }

  /**
   * @return the path to the PostgreSQL connection configuration file;
   *   this is never null
   */
  public static String getConfigFilePath(){
    if(configPath == null){
      configPath = POSTGRES_LOCALHOST_CONFIG;
    }
    return configPath;
  }

  /**
   * Instance for the tenantId from connectionPool or created and
   * added to connectionPool.
   * @param vertx the Vertx to use
   * @param tenantId the tenantId the instance is for
   * @return the PostgresClient instance, or null on error
   */
  private static PostgresClient getInstanceInternal(Vertx vertx, String tenantId) {
    // assumes a single thread vertx model so no sync needed
    PostgresClient postgresClient = connectionPool.get(vertx, tenantId);
    try {
      if (postgresClient == null) {
        postgresClient = new PostgresClient(vertx, tenantId);
        connectionPool.put(vertx, tenantId, postgresClient);
      }
      if (postgresClient.client == null) {
        // in connectionPool, but closeClient() has been invoked
        postgresClient.init(vertx, tenantId);
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
    return postgresClient;
  }

  /**
   * Instance for the Postgres' default schema public.
   * @param vertx the Vertx to use
   * @return the PostgresClient instance, or null on error
   */
  public static PostgresClient getInstance(Vertx vertx) {
    return getInstanceInternal(vertx, DEFAULT_SCHEMA);
  }

  /**
   * Instance for the tenantId.
   * @param vertx the Vertx to use
   * @param tenantId the tenantId the instance is for
   * @return the PostgresClient instance, or null on error
   * @throws IllegalArgumentException when tenantId equals {@link DEFAULT_SCHEMA}
   */
  public static PostgresClient getInstance(Vertx vertx, String tenantId) {
    if (DEFAULT_SCHEMA.equals(tenantId)) {
      throw new IllegalArgumentException("tenantId must not be default schema " + DEFAULT_SCHEMA);
    }
    return getInstanceInternal(vertx, tenantId);
  }

  /* if the password in the config file is encrypted then use the secret key
   * that should have been set via the admin api to decode it and use that to connect
   * note that in embedded mode (such as unit tests) the postgres embedded is started before the
   * verticle is deployed*/
  private String decodePassword(String password) throws Exception {
    String key = AES.getSecretKey();
    if(key != null){
      SecretKey sk = AES.getSecretKeyObject(key);
      String decoded = AES.decryptPassword(password, sk);
      return decoded;
    }
    /* no key , so nothing to decode */
    return password;
  }

  /** this function is intended to receive the tenant id as a password
   * encrypt the tenant id with the secret key and use the encrypted
   * password as the actual password for the tenant user in the DB.
   * In order to then know the password - you need to take the tenant id
   * and encrypt it with the secret key and then you have the tenant's password */
  private String createPassword(String password) throws Exception {
    String key = AES.getSecretKey();
    if(key != null){
      SecretKey sk = AES.getSecretKeyObject(key);
      String newPassword = AES.encryptPasswordAsBase64(password, sk);
      return newPassword;
    }
    /** no key , so nothing to encrypt, the password will be the tenant id */
    return password;
  }

  /**
   * @return this instance's AsyncSQLClient that can connect to Postgres
   */
  AsyncSQLClient getClient() {
    return client;
  }

  /**
   * Close the SQL client of this PostgresClient instance.
   * @param whenDone invoked with the close result; additional close invocations
   *                 are always successful.
   */
  public void closeClient(Handler<AsyncResult<Void>> whenDone) {
    if (client == null) {
      whenDone.handle(Future.succeededFuture());
      return;
    }
    AsyncSQLClient clientToClose = client;
    client = null;
    connectionPool.removeMultiKey(vertx, tenantId);  // remove (vertx, tenantId, this) entry
    clientToClose.close(whenDone);
  }

  /**
   * Close all SQL clients stored in the connection pool.
   */
  public static void closeAllClients() {
    @SuppressWarnings("rawtypes")
    List<Future> list = new ArrayList<>(connectionPool.size());
    // copy of values() because closeClient will delete them from connectionPool
    for (PostgresClient client : connectionPool.values().toArray(new PostgresClient [0])) {
      Future<Object> future = Future.future();
      list.add(future);
      client.closeClient(f -> future.complete());
    }

    CompositeFuture.join(list);
  }

  private void init(Vertx vertx, String tenantId) throws Exception {

    /** check if in pom.xml this prop is declared in order to work with encrypted
     * passwords for postgres embedded - this is a dev mode only feature */
    String secretKey = System.getProperty("postgres_secretkey_4_embeddedmode");

    if(moduleName == null){
      moduleName = PomReader.INSTANCE.getModuleName();
    }

    if(secretKey != null){
      AES.setSecretKey(secretKey);
    }

    this.tenantId = tenantId;
    this.vertx = vertx;

    postgreSQLClientConfig = Envs.allDBConfs();
    if(postgreSQLClientConfig.size() == 0){
      //no env variables passed in, read for module's config file
      log.info("No DB environment variables passed in, attempting to read from config file");
      postgreSQLClientConfig = LoadConfs.loadConfig(getConfigFilePath());
    }
    if(postgreSQLClientConfig == null){
      if (embeddedMode) {
        //embedded mode, if no config passed use defaults
        postgreSQLClientConfig = new JsonObject();
        postgreSQLClientConfig.put(_USERNAME, _USERNAME);
        postgreSQLClientConfig.put(_PASSWORD, _PASSWORD);
        postgreSQLClientConfig.put(HOST, DEFAULT_IP);
        postgreSQLClientConfig.put(PORT, 6000);
        postgreSQLClientConfig.put(DATABASE, "postgres");
      }
      else{
        //not in embedded mode but there is no conf file found
        throw new Exception("No postgres-conf.json file found and not in embedded mode, can not connect to any database");
      }
    }
    else if(tenantId.equals(DEFAULT_SCHEMA)){
      postgreSQLClientConfig.put(_USERNAME, postgreSQLClientConfig.getString(_USERNAME));
      postgreSQLClientConfig.put(_PASSWORD, decodePassword( postgreSQLClientConfig.getString(_PASSWORD) ));
    }
    else{
      log.info("Using schema: " + tenantId);
      postgreSQLClientConfig.put(_USERNAME, convertToPsqlStandard(tenantId));
      postgreSQLClientConfig.put(_PASSWORD, createPassword(tenantId));
    }

    if(embeddedPort != -1 && embeddedMode){
      //over ride the declared default port - coming from the config file and use the
      //passed in port as well. useful when multiple modules start up an embedded postgres
      //in a single server.
      postgreSQLClientConfig.put(PORT, embeddedPort);
    }

    logPostgresConfig();
    client = io.vertx.ext.asyncsql.PostgreSQLClient.createNonShared(vertx, postgreSQLClientConfig);
  }

  /**
   * Log postgreSQLClientConfig.
   */
  private void logPostgresConfig() {
    if (! log.isInfoEnabled()) {
      return;
    }
    JsonObject passwordRedacted = postgreSQLClientConfig.copy();
    passwordRedacted.put(_PASSWORD, "...");
    log.info("postgreSQLClientConfig = " + passwordRedacted.encode());
  }

  /**
   * The PostgreSQL connection info to create the connection, see
   * <a href="http://vertx.io/docs/vertx-mysql-postgresql-client/java/#_configuration">configuration
   * documentation</a>.
   *
   * @return the configuration
   * @see io.vertx.ext.asyncsql.PostgreSQLClient#createNonShared(Vertx vertx, JsonObject config)
   */
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
          log.error(e.getMessage(), e);
          throw e;
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
  public void startTx(Handler<AsyncResult<Object>> done) {
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
          connection.setAutoCommit(false, res1 -> {
            if (res1.failed()) {
              if (connection != null) {
                connection.close();
              }
              done.handle(Future.failedFuture(res1.cause()));
            } else {
              done.handle(Future.succeededFuture(connection));
            }
          });
        } catch (Exception e) {
          log.error(e.getMessage(), e);
          if (connection != null) {
            connection.close();
          }
          done.handle(Future.failedFuture(e));
        }
      }
      else{
        log.error(res.cause().getMessage(), res.cause());
        done.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  //@Timer
  @SuppressWarnings("unchecked")
  public void rollbackTx(Object conn, Handler<AsyncResult<Object>> done) {
    SQLConnection sqlConnection = ((Future<SQLConnection>) conn).result();
    sqlConnection.rollback(res -> {
      sqlConnection.close();
      if (res.failed()) {
        log.error(res.cause().getMessage(), res.cause());
        done.handle(Future.failedFuture(res.cause()));
      } else {
        done.handle(Future.succeededFuture(res));
      }
    });
  }

  //@Timer
  @SuppressWarnings("unchecked")
  public void endTx(Object conn, Handler<AsyncResult<Object>> done) {
    SQLConnection sqlConnection = ((Future<SQLConnection>) conn).result();
    sqlConnection.commit(res -> {
      sqlConnection.close();
      if (res.failed()) {
        log.error(res.cause().getMessage(), res.cause());
        done.handle(Future.failedFuture(res.cause()));
      } else {
        done.handle(Future.succeededFuture(res));
      }
    });
  }

  /**
   *
   * @param table
   *          - tablename to save to
   * @param entity
   *          - this must be a json object
   * @param replyHandler
   * @throws Exception
   */
  public void save(String table, Object entity, Handler<AsyncResult<String>> replyHandler) {
    save(table, null, entity, true, replyHandler);
  }

  public void save(String table, Object entity, boolean returnId, Handler<AsyncResult<String>> replyHandler) {
    save(table, null, entity, returnId, replyHandler);
  }

  public void save(String table, String id, Object entity, Handler<AsyncResult<String>> replyHandler) {
    save(table, id, entity, true, replyHandler);
  }

  public void save(String table, String id, Object entity, boolean returnId, Handler<AsyncResult<String>> replyHandler) {
    save(table, id, entity, returnId, false, replyHandler);
  }

  public void upsert(String table, String id, Object entity, Handler<AsyncResult<String>> replyHandler) {
    save(table, id, entity, true, true, replyHandler);
  }

  /**
   * @param convertEntity - should the entity object be passed in as is to the prepared statement,
   * needed if upserting binary data as base64 where converting it to a json will corrupt the data
   * otherwise this function is not needed as the default is true
   * example:
   *     byte[] data = ......;
   *     JsonArray jsonArray = new JsonArray().add(data);
   *     .upsert(TABLE_NAME, id, jsonArray, false, replyHandler -> {
   */
  public void upsert(String table, String id, Object entity, boolean convertEntity, Handler<AsyncResult<String>> replyHandler) {
    save(table, id, entity, true, true, false, replyHandler);
  }

  public void save(String table, String id, Object entity, boolean returnId, boolean upsert, boolean convertEntity, Handler<AsyncResult<String>> replyHandler) {
    long start = System.nanoTime();

    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();

        StringBuilder clientIdField = new StringBuilder("");
        StringBuilder clientId = new StringBuilder("");
        if(id != null){
          clientId.append("'").append(id).append("',");
          clientIdField.append(idField).append(",");
        }
        String returning = "";
        if(returnId){
          returning = " RETURNING " + idField;
        }

        try {
          String upsertClause = "";
          if(upsert){
            upsertClause = " ON CONFLICT ("+idField+") DO UPDATE SET " +
              DEFAULT_JSONB_FIELD_NAME + " = EXCLUDED."+DEFAULT_JSONB_FIELD_NAME + " ";
          }
          JsonArray queryArg = new JsonArray();
          String type = "?::JSON)";
          if(convertEntity){
            queryArg.add(pojo2json(entity));
          }
          else{
            queryArg = (JsonArray)entity;
            type = "?::text)";
          }
          /* do not change to updateWithParams as this will not return the generated id in the reply */
          connection.queryWithParams(INSERT_CLAUSE + convertToPsqlStandard(tenantId) + "." + table +
            " (" + clientIdField.toString() + DEFAULT_JSONB_FIELD_NAME +
            ") VALUES ("+clientId+type + upsertClause + returning,
            queryArg, query -> {
              connection.close();
              if (query.failed()) {
                replyHandler.handle(Future.failedFuture(query.cause()));
              } else {
                List<JsonArray> resList = query.result().getResults();
                String response = "";
                if(!resList.isEmpty()){
                  response = resList.get(0).getValue(0).toString();
                }
                replyHandler.handle(Future.succeededFuture(response));
              }
              long end = System.nanoTime();
              StatsTracker.addStatElement(STATS_KEY+".save", (end-start));
            });
        } catch (Exception e) {
          if(connection != null){
            connection.close();
          }
          log.error(e.getMessage(), e);
          replyHandler.handle(Future.failedFuture(e));
        }
      } else {
        log.error(res.cause().getMessage(), res.cause());
        replyHandler.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  public void save(String table, String id, Object entity, boolean returnId, boolean upsert, Handler<AsyncResult<String>> replyHandler) {
    save(table, id, entity, returnId, upsert, true, replyHandler);
  }

  @SuppressWarnings("unchecked")
  public void save(Object sqlConnection, String table, Object entity, Handler<AsyncResult<String>> replyHandler) {
    long start = System.nanoTime();

    log.debug("save called on " + table);
    SQLConnection connection = null;
    try {
      // connection not closed by this FUNCTION ONLY BY END TRANSACTION call!
      connection = ((Future<SQLConnection>) sqlConnection).result();
      String pojo = pojo2json(entity);
      connection.queryWithParams(INSERT_CLAUSE + convertToPsqlStandard(tenantId) + "." + table +
        " (" + DEFAULT_JSONB_FIELD_NAME + ") VALUES (?::JSON) RETURNING " + idField,
        new JsonArray().add(pojo), query -> {
          if (query.failed()) {
            replyHandler.handle(Future.failedFuture(query.cause()));
          } else {
            replyHandler.handle(Future.succeededFuture(query.result().getResults().get(0).getValue(0).toString()));
          }
          long end = System.nanoTime();
          StatsTracker.addStatElement(STATS_KEY+".save", (end-start));
        });
    } catch (Exception e) {
      if(connection != null){
        connection.close();
      }
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /***
   * save a list of pojos.
   * pojos are converted to json and saved in a single sql call. the generated IDs of the inserted records are returned
   * in the result set
   * @param table
   * @param entities
   * @param replyHandler
   * @throws Exception
   */
  public void saveBatch(String table, List<Object> entities, Handler<AsyncResult<ResultSet>> replyHandler) {
    long start = System.nanoTime();

    int size = entities.size();

    StringBuilder sb = new StringBuilder();
    try {
      for (int i = 0; i < size; i++) {
        sb.append("('").append( pojo2json(entities.get(i)) ).append("')");
        if(i+1 < size){
          sb.append(",");
        }
      }
    } catch (Exception e) {
      replyHandler.handle(Future.failedFuture(e));
    }

    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
            try {
              connection.query("BEGIN;", begin -> {
                if(begin.succeeded()){
                  connection.query(INSERT_CLAUSE + convertToPsqlStandard(tenantId) + "." + table +
                    " (" + DEFAULT_JSONB_FIELD_NAME + ") VALUES "+sb.toString()+" RETURNING " + idField + ";",
                    query -> {
                      if (query.failed()) {
                        log.error("query saveBatch failed, attempting rollback", query.cause());
                        connection.query("ROLLBACK;", rollbackres -> {
                          if(rollbackres.failed()){
                            log.error("query saveBatch failed, unable to rollback", rollbackres.cause());
                          }
                          else {
                            log.info("rollback success. " + new JsonArray(rollbackres.result().getResults()).encodePrettily());
                          }
                          connection.close();
                          replyHandler.handle(Future.failedFuture(query.cause()));
                        });
                      } else {
                          connection.query("COMMIT;", commit -> {
                            if(commit.succeeded()){
                              long end = System.nanoTime();
                              StatsTracker.addStatElement(STATS_KEY+".save", (end-start));
                              connection.close();
                              replyHandler.handle(Future.succeededFuture(query.result()));
                            }
                            else {
                              log.error("query saveBatch failed to commit, attempting rollback ",
                                commit.cause());
                              connection.query("ROLLBACK;", rollbackres -> {
                                if(rollbackres.failed()){
                                  log.error("query saveBatch failed, unable to rollback", rollbackres.cause());
                                }
                                else{
                                  log.info("rollback success. " + new JsonArray(rollbackres.result().getResults()).encodePrettily());
                                }
                                connection.close();
                                replyHandler.handle(Future.failedFuture(commit.cause()));
                              });
                            }
                          });
                      }
                    });
                }
                else{
                  connection.close();
                  log.error("query saveBatch failed", begin.cause());
                  replyHandler.handle(Future.failedFuture(begin.cause()));
                }
              });

            } catch (Exception e) {
              if(connection != null){
                connection.close();
              }
              log.error(e.getMessage(), e);
              replyHandler.handle(Future.failedFuture(e));
            }
        } catch (Exception e) {
          if(connection != null){
            connection.close();
          }
          log.error(e.getMessage(), e);
          replyHandler.handle(Future.failedFuture(e));
        }
      } else {
        replyHandler.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  /**
   * update a specific record associated with the key passed in the id arg
   * @param table - table to save to (must exist)
   * @param entity - pojo to save
   * @param id - key of the entitiy being updated
   * @param replyHandler
   */
  public void update(String table, Object entity, String id, Handler<AsyncResult<UpdateResult>> replyHandler) {
    update(table, entity, DEFAULT_JSONB_FIELD_NAME, WHERE + idField + "='" + id + "'", false, replyHandler);
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
   *
   */
  public void update(String table, Object entity, Criterion filter, boolean returnUpdatedIds, Handler<AsyncResult<UpdateResult>> replyHandler)
  {
    String where = null;
    if(filter != null){
      where = filter.toString();
    }
    update(table, entity, DEFAULT_JSONB_FIELD_NAME, where, returnUpdatedIds, replyHandler);
  }

  public void update(String table, Object entity, CQLWrapper filter, boolean returnUpdatedIds, Handler<AsyncResult<UpdateResult>> replyHandler)
  {
    String where = "";
    if(filter != null){
      where = filter.toString();
    }
    update(table, entity, DEFAULT_JSONB_FIELD_NAME, where, returnUpdatedIds, replyHandler);
  }

  public void update(Object conn, String table, Object entity, CQLWrapper filter, boolean returnUpdatedIds, Handler<AsyncResult<UpdateResult>> replyHandler) {
    String where = "";
    if(filter != null){
      where = filter.toString();
    }
    update(conn, table, entity, DEFAULT_JSONB_FIELD_NAME, where, returnUpdatedIds, replyHandler);
  }

  public void update(Object conn, String table, Object entity, String jsonbField, String whereClause, boolean returnUpdatedIds, Handler<AsyncResult<UpdateResult>> replyHandler)
  {
    SQLConnection connection = null;
    if(conn != null){
      connection = ((Future<SQLConnection>) conn).result();
      doUpdate(connection, true, table, entity, jsonbField, whereClause, returnUpdatedIds, replyHandler);
    }
    else{
      replyHandler.handle(Future.failedFuture(new Exception("update() called with a null connection...")));
    }
  }

  public void update(String table, Object entity, String jsonbField, String whereClause, boolean returnUpdatedIds, Handler<AsyncResult<UpdateResult>> replyHandler)
  {
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        doUpdate(connection, false, table, entity, jsonbField, whereClause, returnUpdatedIds, replyHandler);
      }
      else{
        replyHandler.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  private void doUpdate(SQLConnection connection, boolean transactionMode, String table, Object entity, String jsonbField, String whereClause, boolean returnUpdatedIds,
      Handler<AsyncResult<UpdateResult>> replyHandler){

    if(connection == null){
      replyHandler.handle(Future.failedFuture(new Exception("update() called with a null connection...")));
      return;
    }

    long start = System.nanoTime();

    StringBuilder sb = new StringBuilder();
    if (whereClause != null) {
      sb.append(whereClause);
    }
    StringBuilder returning = new StringBuilder();
    if (returnUpdatedIds) {
      returning.append(returningId);
    }
    try {
      String q = "UPDATE " + convertToPsqlStandard(tenantId) + "." + table + SET + jsonbField + " = ?::jsonb "  + whereClause
          + " " + returning;
      log.debug("query = " + q);
      String pojo = pojo2json(entity);
      connection.updateWithParams(q, new JsonArray().add(pojo), query -> {
        if(!transactionMode){
          connection.close();
        }
        if (query.failed()) {
          log.error(query.cause().getMessage(),query.cause());
          replyHandler.handle(Future.failedFuture(query.cause()));
        } else {
          replyHandler.handle(Future.succeededFuture(query.result()));
        }
        long end = System.nanoTime();
        StatsTracker.addStatElement(STATS_KEY+".update", (end-start));
        if(log.isDebugEnabled()){
          log.debug("timer: get " +q+ " (ns) " + (end-start));
        }
      });
    } catch (Exception e) {
      if(!transactionMode){
        connection.close();
      }
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }

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
   *
   */
  public void update(String table, UpdateSection section, Criterion when, boolean returnUpdatedIdsCount,
      Handler<AsyncResult<UpdateResult>> replyHandler) {
    long start = System.nanoTime();
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        StringBuilder sb = new StringBuilder();
        if (when != null) {
          sb.append(when.toString());
        }
        StringBuilder returning = new StringBuilder();
        if (returnUpdatedIdsCount) {
          returning.append(returningId);
        }
        try {
          String q = UPDATE + convertToPsqlStandard(tenantId) + "." + table + SET + DEFAULT_JSONB_FIELD_NAME + " = jsonb_set(" + DEFAULT_JSONB_FIELD_NAME + ","
              + section.getFieldsString() + ", '" + section.getValue() + "', false) " + sb.toString() + " " + returning;
          log.debug("query = " + q);
          connection.update(q, query -> {
            connection.close();
            if (query.failed()) {
              log.error(query.cause().getMessage(), query.cause());
              replyHandler.handle(Future.failedFuture(query.cause()));
            } else {
              replyHandler.handle(Future.succeededFuture(query.result()));
            }
            long end = System.nanoTime();
            StatsTracker.addStatElement(STATS_KEY+".update", (end-start));
            if(log.isDebugEnabled()){
              log.debug("timer: get " +q+ " (ns) " + (end-start));
            }
          });
        } catch (Exception e) {
          if(connection != null){
            connection.close();
          }
          log.error(e.getMessage(), e);
          replyHandler.handle(Future.failedFuture(e));
        }
      } else {
        log.error(res.cause().getMessage(), res.cause());
        replyHandler.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  public void delete(String table, CQLWrapper cql, Handler<AsyncResult<UpdateResult>> replyHandler) {
    String where = "";
    if(cql != null){
      where = cql.toString();
    }
    doDelete(table, where, replyHandler);
  }

  /**
   * Delete based on id of record - the id is not in the json object but is a separate column
   * @param table
   * @param id
   * @param replyHandler
   */
  public void delete(String table, String id, Handler<AsyncResult<UpdateResult>> replyHandler) {
    doDelete(table, WHERE + idField + "='" + id + "'", replyHandler);
  }

  /**
   * Delete based on filter
   * @param table
   * @param filter
   * @param replyHandler
   */
  public void delete(String table, Criterion filter, Handler<AsyncResult<UpdateResult>> replyHandler) {
    StringBuilder sb = new StringBuilder();
    if (filter != null) {
      sb.append(filter.toString());
    }
    doDelete(table, sb.toString(), replyHandler);
  }

  public void delete(Object conn, String table, Criterion filter, Handler<AsyncResult<UpdateResult>> replyHandler) {
    SQLConnection sqlConnection = ((Future<SQLConnection>) conn).result();
    StringBuilder sb = new StringBuilder();
    if (filter != null) {
      sb.append(filter.toString());
    }
    doDelete(sqlConnection, true, table, sb.toString(), replyHandler);
  }

  public void delete(String table, Object entity, Handler<AsyncResult<UpdateResult>> replyHandler) {
    String pojo = null;
    try {
      pojo = pojo2json(entity);
    } catch (Exception e) {
      replyHandler.handle(Future.failedFuture(e));
      return;
    }
    doDelete(table, WHERE + DEFAULT_JSONB_FIELD_NAME + "@>'" + pojo + "' ", replyHandler);
  }

  public void delete(Object conn, String table, Object entity, Handler<AsyncResult<UpdateResult>> replyHandler) {
    SQLConnection sqlConnection = ((Future<SQLConnection>) conn).result();
    String pojo = null;
    try {
      pojo = pojo2json(entity);
    } catch (Exception e) {
      replyHandler.handle(Future.failedFuture(e));
      return;
    }
    doDelete(sqlConnection, true, table, WHERE + DEFAULT_JSONB_FIELD_NAME + "@>'" + pojo + "' ", replyHandler);
  }

  private void doDelete(String table, String where, Handler<AsyncResult<UpdateResult>> replyHandler) {
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        doDelete(connection, false, table, where, replyHandler);
      }
      else{
        replyHandler.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  private void doDelete(SQLConnection connection, boolean transactionMode, String table, String where, Handler<AsyncResult<UpdateResult>> replyHandler) {
    long start = System.nanoTime();
    try {
      String q = "DELETE FROM " + convertToPsqlStandard(tenantId) + "." + table + " " + where;
      log.debug("query = " + q);
      connection.update(q, query -> {
        if(!transactionMode){
          connection.close();
        }
        if (query.failed()) {
          log.error(query.cause().getMessage(), query.cause());
          replyHandler.handle(Future.failedFuture(query.cause()));
        } else {
          replyHandler.handle(Future.succeededFuture(query.result()));
        }
        long end = System.nanoTime();
        StatsTracker.addStatElement(STATS_KEY+".delete", (end-start));
        if(log.isDebugEnabled()){
          log.debug("timer: get " +q+ " (ns) " + (end-start));
        }
      });
    } catch (Exception e) {
      if(!transactionMode){
        connection.close();
      }
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  public void get(String table, Class<?> clazz, String fieldName, String where, boolean returnCount, boolean returnIdField,
      boolean setId, Handler<AsyncResult<Results>> replyHandler) {
    get(table, clazz, fieldName, where, returnCount, returnIdField, setId, null, replyHandler);
  }

  public void get(String table, Class<?> clazz, String fieldName, String where, boolean returnCount, boolean returnIdField,
      boolean setId, List<FacetField> facets, Handler<AsyncResult<Results>> replyHandler) {

    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        doGet(connection, false, table, clazz, fieldName, where, returnCount, returnIdField, setId, facets, replyHandler);
      }
      else{
        replyHandler.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  private void doGet(SQLConnection connection, boolean transactionMode, String table, Class<?> clazz, String fieldName, String where, boolean returnCount,
      boolean returnIdField, boolean setId, List<FacetField> facets, Handler<AsyncResult<Results>> replyHandler) {
    long start = System.nanoTime();
    try {
      String addIdField = "";
      if(returnIdField){
        addIdField = "," + idField;
      }

      String select = "SELECT ";

      if(!"null".equals(fieldName) && fieldName.contains("*")){
        //if we are requesting all fields (*) , then dont add the id field to the select
        //this will return two id columns which will create ambiguity in facet queries
        addIdField = "";
      }

      String []q = new String[]{
        select + fieldName + addIdField + " FROM " + convertToPsqlStandard(tenantId) + "." + table + " " + where
      };

      ParsedQuery parsedQuery = null;

      if(returnCount || (facets != null && !facets.isEmpty())){
        parsedQuery = parseQuery(q[0]);
      }

      if (returnCount) {
        //optimize the entire query building process needed!!
        Map<String, String> replaceMapping = new HashMap<>();
        replaceMapping.put("tenantId", convertToPsqlStandard(tenantId));
        replaceMapping.put("query",
          org.apache.commons.lang.StringEscapeUtils.escapeSql(
            parsedQuery.getCountFuncQuery()));
        StrSubstitutor sub = new StrSubstitutor(replaceMapping);
        q[0] = select +
          sub.replace(countClauseTemplate) + q[0].replaceFirst(select , " ");
      }

      if(facets != null && !facets.isEmpty()){
        q[0] = buildFacetQuery(table , parsedQuery, facets, returnCount, q[0]);
      }
      log.debug("query = " + q[0]);
      connection.query(q[0], query -> {
          if(!transactionMode){
            connection.close();
          }
          try {
            if (query.failed()) {
              log.error(query.cause().getMessage(), query.cause());
              replyHandler.handle(Future.failedFuture(query.cause()));
            } else {
              replyHandler.handle(Future.succeededFuture(processResult(query.result(), clazz, returnCount, setId)));
            }
            long end = System.nanoTime();
            StatsTracker.addStatElement(STATS_KEY+".get", (end-start));
            if(log.isDebugEnabled()){
              log.debug("timer: get " +q[0]+ " (ns) " + (end-start));
            }
          } catch (Exception e) {
            log.error(e.getMessage(), e);
            replyHandler.handle(Future.failedFuture(e));
          }
      });
    } catch (Exception e) {
        if(!transactionMode){
          connection.close();
        }
        log.error(e.getMessage(), e);
        replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * function uses freemarker templating, the template will be loaded the first time
   * should take about 70-80 milli - after that the template gets cached and will be sub milli
   * @param tableName
   * @param where
   * @param facets
   * @param query
   * @return
   * @throws Exception
   */
  private String buildFacetQuery(String tableName, ParsedQuery parsedQuery, List<FacetField> facets, boolean countRequested, String query) throws Exception {
    long start = System.nanoTime();
    FacetManager fm = new FacetManager(convertToPsqlStandard(tenantId) + "." + tableName);
    if(parsedQuery.getWhereClause() != null){
      fm.setWhere(" where " + parsedQuery.getWhereClause());
    }
    fm.setSupportFacets(facets);
    fm.setIdField(idField);
    fm.setLimitClause(parsedQuery.getLimitClause());
    fm.setOffsetClause(parsedQuery.getOffsetClause());
    fm.setMainQuery(parsedQuery.getQueryWithoutLimOff());
    fm.setSchema(convertToPsqlStandard(tenantId));
    fm.setCountQuery(org.apache.commons.lang.StringEscapeUtils.escapeSql(
      parsedQuery.getCountFuncQuery()));
    long end = System.nanoTime();
    log.debug( "timer: buildFacetQuery (ns) " + (end - start));

    return fm.generateFacetQuery();
  }


  /**
   * pass in an entity that is fully / partially populated and the query will return all records matching the
   * populated fields in the entity - note that this queries the jsonb object, so should not be used to query external
   * fields
   *
   * @param table
   * @param entity
   * @param replyHandler
   * @throws Exception
   */
  //@Timer
  public void get(String table, Object entity, boolean returnCount, Handler<AsyncResult<Results>> replyHandler) {
    get(table,  entity, returnCount, true, replyHandler);
  }

  public void get(String table, Object entity, boolean returnCount, boolean returnIdField, Handler<AsyncResult<Results>> replyHandler) {
    boolean setId = true;
    if(returnIdField == false){
      //if no id fields then cannot setId from external column into json object
      setId = false;
    }
    String pojo = null;
    try {
      pojo = pojo2json(entity);
    } catch (Exception e) {
      replyHandler.handle(Future.failedFuture(e));
      return;
    }
    get(table, entity.getClass(), DEFAULT_JSONB_FIELD_NAME, WHERE + DEFAULT_JSONB_FIELD_NAME
      + "@>'" + pojo + "' ", returnCount, returnIdField, setId, replyHandler);
  }

  public void get(String table, Object entity, String[] fields, boolean returnCount, boolean returnIdField, Handler<AsyncResult<Results>> replyHandler) {
    get(table, entity, fields, returnCount, returnIdField, -1, -1, replyHandler);
  }

  public void get(String table, Object entity, String[] fields, boolean returnCount,
      boolean returnIdField, int offset, int limit, Handler<AsyncResult<Results>> replyHandler) { //NOSONAR
    boolean setId = true;
    if(returnIdField == false){
      //if no id fields then cannot setId from extrnal column into json object
      setId = false;
    }
    StringBuilder sb = new StringBuilder();
    if(offset != -1){
      sb.append(" ").append(new Offset(offset).toString()).append(" ");
    }
    if(limit != -1){
      sb.append(" ").append(new Limit(limit).toString()).append(" ");
    }
    String pojo = null;
    try {
      pojo = pojo2json(entity);
    } catch (Exception e) {
      replyHandler.handle(Future.failedFuture(e));
      return;
    }
    String fieldsStr = Arrays.toString(fields);
    get(table, entity.getClass(), fieldsStr.substring(1, fieldsStr.length()-1), WHERE + DEFAULT_JSONB_FIELD_NAME
      + "@>'" + pojo + "' "+sb.toString(), returnCount, returnIdField, setId, replyHandler);
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
  public void get(String table, Class<?> clazz, Criterion filter, boolean returnCount, Handler<AsyncResult<Results>> replyHandler)
  {
    get(table, clazz, filter, returnCount, true, replyHandler);
  }

  public void get(String table, Class<?> clazz, String[] fields, CQLWrapper filter, boolean returnCount, boolean setId,
      Handler<AsyncResult<Results>> replyHandler) {
    get(table, clazz, fields, filter, returnCount, setId, null, replyHandler);
  }

  public void get(String table, Class<?> clazz, String[] fields, CQLWrapper filter, boolean returnCount, boolean setId,
      List<FacetField> facets, Handler<AsyncResult<Results>> replyHandler) {
    String where = "";
    if(filter != null){
      where = filter.toString();
    }
    String fieldsStr = Arrays.toString(fields);
    get(table, clazz, fieldsStr.substring(1, fieldsStr.length()-1), where, returnCount, true, setId, facets, replyHandler);
  }

  public void get(String table, Class<?> clazz, String[] fields, String filter, boolean returnCount, boolean setId, Handler<AsyncResult<Results>> replyHandler)
  {
    String where = "";
    if(filter != null){
      where = filter;
    }
    String fieldsStr = Arrays.toString(fields);
    get(table, clazz, fieldsStr.substring(1, fieldsStr.length()-1), where, returnCount, true, setId, replyHandler);
  }

  public void get(String table, Class<?> clazz, String filter, boolean returnCount, boolean setId, Handler<AsyncResult<Results>> replyHandler)
  {
    String where = "";
    if(filter != null){
      where = filter;
    }
    get(table, clazz, new String[]{DEFAULT_JSONB_FIELD_NAME}, where, returnCount, setId, replyHandler);
  }

  public void get(String table, Class<?> clazz, String[] fields, CQLWrapper filter, boolean returnCount, Handler<AsyncResult<Results>> replyHandler)
  {
    get(table, clazz, fields, filter, returnCount, true, replyHandler);
  }

  public void get(String table, Class<?> clazz, CQLWrapper filter, boolean returnCount, Handler<AsyncResult<Results>> replyHandler)
  {
    get(table, clazz, new String[]{DEFAULT_JSONB_FIELD_NAME}, filter, returnCount, true, replyHandler);
  }

  public void get(String table, Class<?> clazz, CQLWrapper filter, boolean returnCount, boolean setId, Handler<AsyncResult<Results>> replyHandler)
  {
    get(table, clazz, new String[]{DEFAULT_JSONB_FIELD_NAME}, filter, returnCount, setId, replyHandler);
  }

  public void get(String table, Class<?> clazz, CQLWrapper filter, boolean returnCount, boolean setId, List<FacetField> facets,
      Handler<AsyncResult<Results>> replyHandler) {
    get(table, clazz, new String[]{DEFAULT_JSONB_FIELD_NAME}, filter, returnCount, setId, facets, replyHandler);
  }


  public void get(String table, Class<?> clazz, Criterion filter, boolean returnCount, boolean setId,
      Handler<AsyncResult<Results>> replyHandler) {
    get(table, clazz, filter, returnCount, setId, null, replyHandler);
  }

  public void get(Object conn, String table, Class<?> clazz, Criterion filter, boolean returnCount, boolean setId,
      Handler<AsyncResult<Results>> replyHandler) {
    get(conn, table, clazz, filter, returnCount, setId, null, replyHandler);
  }

  /**
   * select query
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - see Criterion class
   * @param returnCount - whether to return the amount of records matching the query
   * @param setId - whether to automatically set the "id" field of the returned object
   * @param replyHandler
   * @throws Exception
   */
  public void get(String table, Class<?> clazz, Criterion filter, boolean returnCount, boolean setId,
      List<FacetField> facets, Handler<AsyncResult<Results>> replyHandler) {

    get(null, table, clazz, filter, returnCount, setId, facets, replyHandler);
  }

  public void get(Object conn, String table, Class<?> clazz, Criterion filter, boolean returnCount, boolean setId,
      List<FacetField> facets, Handler<AsyncResult<Results>> replyHandler) {

    StringBuilder sb = new StringBuilder();
    StringBuilder fromClauseFromCriteria = new StringBuilder();
    if (filter != null) {
      sb.append(filter.toString());
      fromClauseFromCriteria.append(filter.from2String());
      if (fromClauseFromCriteria.length() > 0) {
        fromClauseFromCriteria.insert(0, ",");
      }
    }
    if(conn == null){
      get(table, clazz, DEFAULT_JSONB_FIELD_NAME, fromClauseFromCriteria.toString() + sb.toString(),
        returnCount, true, setId, facets, replyHandler);
    }
    else{
      SQLConnection sqlConnection = ((Future<SQLConnection>) conn).result();
      doGet(sqlConnection, true, table, clazz, DEFAULT_JSONB_FIELD_NAME,
        fromClauseFromCriteria.toString() + sb.toString(), returnCount, true, setId, facets, replyHandler);
    }
  }

  /**
   * run simple join queries between two tables
   *
   * for example, to generate the following query:
   * SELECT  c1.* , c2.* FROM univeristy.config_data c1
   *  INNER JOIN univeristy.config_data c2 ON ((c1.jsonb->>'code') = (c2.jsonb->'scope'->>'library_id'))
   *    WHERE (c2.jsonb->>'default')::boolean IS TRUE  AND (c2.jsonb->>'default')::boolean IS TRUE
   *
   * Create a criteria representing a join column for each of the tables
   * Create two JoinBy objects containing the:
   *   1. The table to join from and to,
   *   2. An alias for the tables,
   *   3. The Fields to return
   *   4. The column to join on (using the criteria object)
   *
   * @param  JoinBy jb1= new JoinBy("config_data","c1",
   *   new Criteria().addField("'code'"), new String[]{"count(c1._id)"});
   *
   * @param  JoinBy jb2= new JoinBy("config_data","c2",
   *   new Criteria().addField("'scope'").addField("'library_id'"),  new String[]{"avg(length(c2.description))"});
   *
   * Passing "*" to the fields to return may be used as well (or a list of aliased column names)
   *
   * JoinBy jb1= new JoinBy("config_data","c1",
   *  new Criteria().addField("'code'"), new String[]{"*"});
   *
   * @param operation what operation to use when comparing the two columns. For example:
   * setting this to equals would yeild something like:
   *
   * ((c1.jsonb->>'code') = (c2.jsonb->'scope'->>'library_id'))
   *
   * @param joinType - for example: INNER JOIN, LEFT JOIN,
   * some prepared COnsts can be found: JoinBy.RIGHT_JOIN
   *
   * @param cr criterion to use to further filter results per table. can be used to also sort results
   * new Criterion().setOrder(new Order("c2._id", ORDER.DESC))
   * But can be used to create a more complex where clause if needed
   *
   * For example:
   * GroupedCriterias gc = new GroupedCriterias();
   *  gc.addCriteria(new Criteria().setAlias("c2").addField("'default'")
   *    .setOperation(Criteria.OP_IS_TRUE));
   *  gc.addCriteria(new Criteria().setAlias("c2").addField("'enabled'")
   *    .setOperation(Criteria.OP_IS_TRUE) , "OR");
   *  gc.setGroupOp("AND");
   *
   *  NOTE that to use sorting with a combination of functions - group by is needed - not currently implemented
   *
   *  Criterion cr =
   *      new Criterion().addGroupOfCriterias(gc).addGroupOfCriterias(gc1).setOrder(new Order("c1._id", ORDER.DESC));
   *
   * */
  public void join(JoinBy from, JoinBy to, String operation, String joinType, String cr, Class<?> returnedClass,
      boolean setId, Handler<AsyncResult<?>> replyHandler){
    long start = System.nanoTime();

    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
          String select = "SELECT ";

          StringBuffer joinon = new StringBuffer();
          StringBuffer tables = new StringBuffer();
          StringBuffer selectFields = new StringBuffer();

          String filter = "";
          if(cr != null){
            filter = cr;
          }

          String selectFromTable = from.getSelectFields();
          String selectToTable = to.getSelectFields();
          boolean addComma = false;
          if(selectFromTable != null && selectFromTable.length() > 0){
            selectFields.append(from.getSelectFields());
            addComma = true;
          }
          if(selectToTable != null && selectToTable.length() > 0){
            if(addComma){
              selectFields.append(",");
            }
            selectFields.append(to.getSelectFields());
          }

          tables.append(convertToPsqlStandard(tenantId) + "." + from.getTableName() + " " + from.getAlias() + " ");

          joinon.append(joinType + " " + convertToPsqlStandard(tenantId) + "." + to.getTableName() + " " + to.getAlias() + " ");

          String q[] = new String[]{ select + selectFields.toString() + " FROM " + tables.toString() + joinon.toString() +
              new Criterion().addCriterion(from.getJoinColumn(), operation, to.getJoinColumn(), " AND ") + filter};

          //TODO optimize query building
          Map<String, String> replaceMapping = new HashMap<>();
          replaceMapping.put("tenantId", convertToPsqlStandard(tenantId));
          replaceMapping.put("query",
            org.apache.commons.lang.StringEscapeUtils.escapeSql(
              parseQuery(q[0]).getCountFuncQuery()));
          StrSubstitutor sub = new StrSubstitutor(replaceMapping);
          q[0] = select +
            sub.replace(countClauseTemplate) + q[0].replaceFirst(select , " ");

          log.debug("query = " + q[0]);
          connection.query(q[0],
            query -> {
            connection.close();
            if (query.failed()) {
              log.error(query.cause().getMessage(), query.cause());
              replyHandler.handle(Future.failedFuture(query.cause()));
            } else {
              if(returnedClass != null){
                replyHandler.handle(Future.succeededFuture(
                  processResult(query.result(), returnedClass, true, setId)));
              }
              else{
                replyHandler.handle(Future.succeededFuture(query.result()));
              }
            }
            long end = System.nanoTime();
            StatsTracker.addStatElement(STATS_KEY+".join", (end-start));
            if(log.isDebugEnabled()){
              log.debug("timer: get " +q[0]+ " (ns) " + (end-start));
            }
          });
        } catch (Exception e) {
          if(connection != null){
            connection.close();
          }
          log.error(e.getMessage(), e);
          replyHandler.handle(Future.failedFuture(e));
        }
      } else {
        log.error(res.cause().getMessage(), res.cause());
        replyHandler.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  public void join(JoinBy from, JoinBy to, String operation, String joinType, String cr, Class<?> returnedClass,
      Handler<AsyncResult<?>> replyHandler){
    join(from, to, operation, joinType, cr, returnedClass, true, replyHandler);
  }

  public void join(JoinBy from, JoinBy to, String operation, String joinType, Criterion cr
      ,Handler<AsyncResult<?>> replyHandler){
    String filter = "";
    if(cr != null){
      filter = cr.toString();
    }
    join(from, to, operation, joinType, filter, null, true, replyHandler);
  }

  public void join(JoinBy from, JoinBy to, String operation, String joinType, CQLWrapper cr
      ,Handler<AsyncResult<?>> replyHandler){
    String filter = "";
    if(cr != null){
      filter = cr.toString();
    }
    join(from, to, operation, joinType, filter, null, true, replyHandler);
  }

  public void join(JoinBy from, JoinBy to, String operation, String joinType, Class<?> returnedClazz, CQLWrapper cr
      ,Handler<AsyncResult<?>> replyHandler){
    String filter = "";
    if(cr != null){
      filter = cr.toString();
    }
    join(from, to, operation, joinType, filter, returnedClazz, true, replyHandler);
  }

  public void join(JoinBy from, JoinBy to, String operation, String joinType, Class<?> returnedClazz, CQLWrapper cr
      , boolean setId, Handler<AsyncResult<?>> replyHandler){
    String filter = "";
    if(cr != null){
      filter = cr.toString();
    }
    join(from, to, operation, joinType, filter, returnedClazz, setId, replyHandler);
  }

  public void join(JoinBy from, JoinBy to, String operation, String joinType, Class<?> returnedClazz, String where
      ,Handler<AsyncResult<?>> replyHandler){
    String filter = "";
    if(where != null){
      filter = where;
    }
    join(from, to, operation, joinType, filter, returnedClazz, true, replyHandler);
  }

  private Results processResult(io.vertx.ext.sql.ResultSet rs, Class<?> clazz, boolean count) {
    return processResult(rs, clazz, count, true);
  }

  private Results processResult(io.vertx.ext.sql.ResultSet rs, Class<?> clazz, boolean count, boolean setId) {
    long start = System.nanoTime();
    Object[] ret = new Object[2];
    List<Object> list = new ArrayList<>();
    List<JsonObject> tempList = rs.getRows();
    List<String> columnNames = rs.getColumnNames();
    int columnNamesCount = columnNames.size();
    Map<String, org.folio.rest.jaxrs.model.Facet> rInfo = new HashMap<>();
    int rowCount = rs.getNumRows(); //this is incorrect in facet queries which add a row per facet value
    boolean countSet = false;
    if (rowCount > 0 && count) {
      //if facet query, this wont set the count as it doesnt have a count column at this location,
      Object firstColFirstVal = rs.getResults().get(0).getValue(0);
      if(null != firstColFirstVal && "Integer".equals(firstColFirstVal.getClass().getSimpleName())){
        //regular query with count requested since count is the first column for each record
        rowCount = rs.getResults().get(0).getInteger(0);
      }
    }
    /* an exception to having the jsonb column get mapped to the corresponding clazz is a case where the
     * clazz has an jsonb field, for example an audit class which contains a field called
     * jsonb - meaning it encapsulates the real object for example for auditing purposes
     * (contains the jsonb object as well as some other fields). In such a
     * case, do not map the clazz to the content of the jsonb - but rather set the jsonb named field of the clazz
     * with the jsonb column value */
    boolean isAuditFlavored = false;
    try{
      clazz.getField(DEFAULT_JSONB_FIELD_NAME);
      isAuditFlavored = true;
    }catch(NoSuchFieldException nse){
      if(log.isDebugEnabled()){
        log.debug("non audit table, no "+ DEFAULT_JSONB_FIELD_NAME + " found in json");
      }
    }

    int facetEntriesInResultSet = 0;

    for (int i = 0; i < tempList.size(); i++) {
      try {
        Object jo = tempList.get(i).getValue(DEFAULT_JSONB_FIELD_NAME);
        Object id = tempList.get(i).getValue(idField);
        Object o = null;
        if(!isAuditFlavored && jo != null){
          try {
            //is this a facet entry - if so process it, otherwise will throw an exception
            //and continue trying to map to the pojos
            o =  mapper.readValue(jo.toString(), org.folio.rest.jaxrs.model.Facet.class);
            org.folio.rest.jaxrs.model.Facet facet = rInfo.get(((org.folio.rest.jaxrs.model.Facet)o).getType());
            if(facet == null){
              rInfo.put(((org.folio.rest.jaxrs.model.Facet)o).getType(), (org.folio.rest.jaxrs.model.Facet)o);
            }
            else{
              facet.getFacetValues().add(((org.folio.rest.jaxrs.model.Facet)o).getFacetValues().get(0));
            }
            facetEntriesInResultSet = facetEntriesInResultSet+1;
            continue;
          } catch (Exception e) {
            try {
              o = mapper.readValue(jo.toString(), clazz);
            } catch (UnrecognizedPropertyException e1) {
              // this is a facet query , and this is the count entry {"count": 11}
              rowCount = new JsonObject(tempList.get(i).getString("jsonb")).getInteger("count");
              continue;
            }
          }
        }
        else{
          o = clazz.newInstance();
        }
        /* attempt to populate jsonb object with values from external columns - for example:
         * if there is an update_date column in the record - try to populate a field updateDate in the
         * jsonb object - this allows to use the DB for things like triggers to populate the update_date
         * automatically, but still push them into the jsonb object - the json schema must declare this field
         * as well - also support the audit mode descrbed above.
         * NOTE that the query must request any field it wants to get populated into the jsonb obj*/
        for (int j = 0; j < columnNamesCount; j++) {
/*          if(columnNames.get(j).equals("count") && !countSet){
          //check if this is reachable
            rowCount = tempList.get(i).getLong(columnNames.get(j)).intValue();
          }*/
          if((isAuditFlavored || !columnNames.get(j).equals(DEFAULT_JSONB_FIELD_NAME))
              && !columnNames.get(j).equals(idField)){
            try {
              Method m[] = o.getClass().getMethods();
              for (int k = 0; k < m.length; k++) {
                if(m[k].getName().equals(columnNametoCamelCaseWithset(columnNames.get(j)))){
                  o.getClass().getMethod(columnNametoCamelCaseWithset(columnNames.get(j)),
                    m[k].getParameterTypes()).invoke(o, new Object[] { tempList.get(i).getValue(columnNames.get(j)) });
                }
              }
            } catch (Exception e) {
              log.warn("Unable to populate field " + columnNametoCamelCaseWithset(columnNames.get(j))
                + " for object of type " + clazz.getName());
            }
          }
        }
        if(setId){
          o.getClass().getMethod(columnNametoCamelCaseWithset(idField),
            new Class[] { String.class }).invoke(o, new String[] { id.toString() });
        }
        list.add(o);
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        list.add(null);
      }
    }

    ResultInfo rn = new ResultInfo();
    rInfo.forEach( (k , v ) -> {
      rn.getFacets().add(v);
    });
    rn.setTotalRecords(rowCount);

    Results r = new Results();
    r.setResults(list);
    r.setResultInfo(rn);

    long end = System.nanoTime();
    StatsTracker.addStatElement(STATS_KEY+".processResult", (end-start));
    if(log.isDebugEnabled()){
      log.debug("timer: process results (ns) " + (end-start));
    }
    return r;
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
            connection.close();
            if (query.failed()) {
              replyHandler.handle(Future.failedFuture(query.cause()));
            } else {
              replyHandler.handle(Future.succeededFuture(query.result()));
            }
          });
        } catch (Exception e) {
          if(connection != null){
            connection.close();
          }
          log.error(e.getMessage(), e);
          replyHandler.handle(Future.failedFuture(e));
        }
      } else {
        replyHandler.handle(Future.failedFuture(res.cause()));
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
            connection.close();
            if (query.failed()) {
              replyHandler.handle(Future.failedFuture(query.cause()));
            } else {
              replyHandler.handle(Future.succeededFuture(query.result().toString()));
            }
            log.debug("mutate timer: " + sql + " took " + (System.nanoTime()-s)/1000000);
          });
        } catch (Exception e) {
          if(connection != null){
            connection.close();
          }
          log.error(e.getMessage(), e);
          replyHandler.handle(Future.failedFuture(e));
        }
      } else {
        replyHandler.handle(Future.failedFuture(res.cause()));
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
    SQLConnection sqlConnection = ((Future<SQLConnection>) conn).result();
    try {
      sqlConnection.update(sql, query -> {
        if (query.failed()) {
          replyHandler.handle(Future.failedFuture(query.cause()));
        } else {
          replyHandler.handle(Future.succeededFuture(query.result().toString()));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * For queries where you only want to populate the where clause
   * <br/>
   * See {@link #persistentlyCacheResult(String, String, Handler) }
   * @param cacheName
   * @param tableName
   * @param filter
   * @param replyHandler
   */
  public void persistentlyCacheResult(String cacheName, String tableName, CQLWrapper filter, Handler<AsyncResult<Integer>> replyHandler){
    String where = "";
    if(filter != null){
      where = filter.toString();
    }
    String q =
        "SELECT * FROM " + convertToPsqlStandard(tenantId) + "." + tableName + " " + where;
    persistentlyCacheResult(cacheName, q, replyHandler);
  }

  /**
   * For queries where you only want to populate the where clause
   * <br/>
   * See {@link #persistentlyCacheResult(String, String, Handler) }
   * @param cacheName
   * @param tableName
   * @param filter
   * @param replyHandler
   */
  public void persistentlyCacheResult(String cacheName, String tableName, Criterion filter, Handler<AsyncResult<Integer>> replyHandler){
    String where = "";
    if(filter != null){
      where = filter.toString();
    }
    String q =
        "SELECT * FROM " + convertToPsqlStandard(tenantId) + "." + tableName + " " + where;
    persistentlyCacheResult(cacheName, q, replyHandler);
  }

  /**
   * Create a table, a type of materialized view, with the results of a specific query.
   * This can be very helpful when the query is complex and the data is relatively static.
   * This will create a table populated with the results from the query (sql2cache).
   * Further queries can then be run on this table (cacheName) instead of re-executing the complex
   * sql query over and over again.
   * <br/>
   * 1. The table will not track subsequent changes to the source tables
   * <br/>
   * 2. The table should be DROPPED when not needed anymore
   * <br/>
   * 3. To Refresh the table, DROP and Re-call this function
   * <br/>
   * Use carefully, index support on created table to be added
   * @param cacheName - name of the table holding the results of the query
   * @param sql2cache - the sql query to use to populate the table
   * @param replyHandler
   */
  public void persistentlyCacheResult(String cacheName, String sql2cache, Handler<AsyncResult<Integer>> replyHandler){
    long start = System.nanoTime();
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
          String q = "CREATE UNLOGGED TABLE IF NOT EXISTS "
              + convertToPsqlStandard(tenantId) + "." + cacheName +" AS " + sql2cache;
          System.out.println(q);
          connection.update(q,
            query -> {
            connection.close();
            if (query.failed()) {
              replyHandler.handle(Future.failedFuture(query.cause()));
            } else {
              replyHandler.handle(Future.succeededFuture(query.result().getUpdated()));
            }
            long end = System.nanoTime();
            StatsTracker.addStatElement(STATS_KEY+".persistentlyCacheResult", (end-start));
            log.debug("CREATE TABLE AS timer: " + q + " took " + (end-start)/1000000);
          });
        } catch (Exception e) {
          if(connection != null){
            connection.close();
          }
          log.error(e.getMessage(), e);
          replyHandler.handle(Future.failedFuture(e));
        }
      } else {
        replyHandler.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  public void removePersistentCacheResult(String cacheName, Handler<AsyncResult<Integer>> replyHandler){
    long start = System.nanoTime();
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
          connection.update("DROP TABLE "
              + convertToPsqlStandard(tenantId) + "." + cacheName,
            query -> {
            connection.close();
            if (query.failed()) {
              replyHandler.handle(Future.failedFuture(query.cause()));
            } else {
              replyHandler.handle(Future.succeededFuture(query.result().getUpdated()));
            }
            long end = System.nanoTime();
            StatsTracker.addStatElement(STATS_KEY+".removePersistentCacheResult", (end-start));
            log.debug("DROP TABLE timer: " + cacheName + " took " + (end-start)/1000000);
          });
        } catch (Exception e) {
          if(connection != null){
            connection.close();
          }
          log.error(e.getMessage(), e);
          replyHandler.handle(Future.failedFuture(e));
        }
      } else {
        replyHandler.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  /**
   * @param identifier  the identifier to check
   * @return if the identifier is a valid Postgres identifier and does not contain
   *          letters with diacritical marks or non-Latin letters
   */
  public boolean isValidPostgresIdentifier(String identifier) {
    return POSTGRES_IDENTIFIER.matcher(identifier).matches();
  }

  /**
   * Drop the database if it exists.
   * @param database  database name
   * @throws SQLException  on database error
   * @throws IllegalArgumentException  if database name is too long, contains
   *          illegal characters or letters with diacritical marks or non-Latin letters
   */
  public void dropCreateDatabase(String database) throws SQLException {
    if (! isValidPostgresIdentifier(database)) {
      throw new IllegalArgumentException("Illegal character in database name: " + database);
    }

    try (Connection connection = getStandaloneConnection("postgres", true);
        Statement statement = connection.createStatement()) {
      statement.executeUpdate("DROP DATABASE IF EXISTS " + database); //NOSONAR
      statement.executeUpdate("CREATE DATABASE " + database); //NOSONAR
    } catch (SQLException e) {
      throw e;
    }
  }

  /**
   * Will connect to a specific database and execute the commands in the .sql file
   * against that database.<p />
   * NOTE: NOT tested on all types of statements - but on a lot
   *
   * @param sqlFile - string of sqls with executable statements
   * @param stopOnError - stop on first error
   * @param replyHandler - the handler's result is the list of statements that failed; the list may be empty
   */
  public void runSQLFile(String sqlFile, boolean stopOnError,
      Handler<AsyncResult<List<String>>> replyHandler){
    if(sqlFile == null){
      log.error("sqlFile value is null");
      replyHandler.handle(Future.failedFuture("sqlFile value is null"));
      return;
    }
    try {
      StringBuilder singleStatement = new StringBuilder();
      String[] allLines = sqlFile.split("(\r\n|\r|\n)");
      List<String> execStatements = new ArrayList<>();
      boolean inFunction = false;
      boolean inCopy = false;
      for (int i = 0; i < allLines.length; i++) {
        if(allLines[i].toUpperCase().matches("^\\s*(CREATE USER|CREATE ROLE).*") && AES.getSecretKey() != null) {
          final Pattern pattern = Pattern.compile("PASSWORD\\s*'(.+?)'\\s*", Pattern.CASE_INSENSITIVE);
          final Matcher matcher = pattern.matcher(allLines[i]);
          if(matcher.find()){
            /** password argument indicated in the create user / role statement */
            String newPassword = createPassword(matcher.group(1));
            allLines[i] = matcher.replaceFirst(" PASSWORD '" + newPassword +"' ");
          }
        }
        if(allLines[i].trim().startsWith("\ufeff--") || allLines[i].trim().length() == 0 || allLines[i].trim().startsWith("--")){
          //this is an sql comment, skip
          continue;
        }
        else if(allLines[i].toUpperCase().matches("^\\s*(COPY ).*?FROM.*?STDIN.*") && !inFunction){
          singleStatement.append(allLines[i]);
          inCopy = true;
        }
        else if (inCopy && (allLines[i].trim().equals("\\."))){
          inCopy = false;
          execStatements.add( singleStatement.toString() );
          singleStatement = new StringBuilder();
        }
        else if(allLines[i].toUpperCase().matches("^\\s*(CREATE OR REPLACE FUNCTION|CREATE FUNCTION).*")){
          singleStatement.append(allLines[i]+"\n");
          inFunction = true;
        }
        else if (inFunction && allLines[i].trim().toUpperCase().matches(".*\\s*LANGUAGE .*")){
          singleStatement.append(" " + allLines[i]);
          if(!allLines[i].trim().endsWith(";")){
            int j=0;
            if(i+1<allLines.length){
              for (j = i+1; j < allLines.length; j++) {
                if(allLines[j].trim().toUpperCase().trim().matches(CLOSE_FUNCTION_POSTGRES)){
                  singleStatement.append(" " + allLines[j]);
                }
                else{
                  break;
                }
              }
            }
            i = j;
          }
          inFunction = false;
          execStatements.add( singleStatement.toString() );
          singleStatement = new StringBuilder();
        }
        else if(allLines[i].trim().endsWith(";") && !inFunction && !inCopy){
          execStatements.add( singleStatement.append(" " + allLines[i]).toString() );
          singleStatement = new StringBuilder();
        }
        else {
          if(inCopy)  {
            singleStatement.append("\n");
          }
          else{
            singleStatement.append(" ");
          }
          singleStatement.append(allLines[i]);
        }
      }
      String lastStatement = singleStatement.toString();
      if (! lastStatement.trim().isEmpty()) {
        execStatements.add(lastStatement);
      }
      execute(execStatements.toArray(new String[]{}), stopOnError, replyHandler);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  private Connection getStandaloneConnection(String newDB, boolean superUser) throws SQLException {
    String host = postgreSQLClientConfig.getString(HOST);
    int port = postgreSQLClientConfig.getInteger(PORT);
    String user = postgreSQLClientConfig.getString(_USERNAME);
    String pass = postgreSQLClientConfig.getString(_PASSWORD);
    String db = postgreSQLClientConfig.getString(DATABASE);

    if(newDB != null){
      db = newDB;
      if(!superUser){
        pass = newDB;
        user = newDB;
      }
    }
    return DriverManager.getConnection(
      "jdbc:postgresql://"+host+":"+port+"/"+db, user , pass);
  }

  /**
   * Copy files via the COPY FROM postgres syntax
   * Support 3 modes
   * 1. In line (STDIN) Notice the mandatory \. at the end of all entries to import
   * COPY config_data (jsonb) FROM STDIN ENCODING 'UTF8';
   * {"module":"SETTINGS","config_name":"locale","update_date":"1.1.2017","code":"system.currency_symbol.dk","description":"currency code","default": false,"enabled": true,"value": "kr"}
   * \.
   * 2. Copy from a data file packaged in the jar
   * COPY config_data (jsonb) FROM 'data/locales.data' ENCODING 'UTF8';
   * 3. Copy from a file on disk (absolute path)
   * COPY config_data (jsonb) FROM 'C:\\Git\\configuration\\mod-configuration-server\\src\\main\\resources\\data\\locales.data' ENCODING 'UTF8';

   * @param copyInStatement
   * @param connection
   * @throws Exception
   */
  private void copyIn(String copyInStatement, Connection connection) throws Exception {
    long totalInsertedRecords = 0;
    CopyManager copyManager =
        new CopyManager((BaseConnection) connection);
    if(copyInStatement.contains("STDIN")){
      //run as is
      int sep = copyInStatement.indexOf("\n");
      String copyIn = copyInStatement.substring(0, sep);
      String data = copyInStatement.substring(sep+1);
      totalInsertedRecords = copyManager.copyIn(copyIn, new StringReader(data));
    }
    else{
      //copy from a file,
      String[] valuesInQuotes = StringUtils.substringsBetween(copyInStatement , "'", "'");
      if(valuesInQuotes.length == 0){
        log.warn("SQL statement: COPY FROM, has no STDIN and no file path wrapped in ''");
        throw new Exception("SQL statement: COPY FROM, has no STDIN and no file path wrapped in ''");
      }
      //do not read from the file system for now as this needs to support data files packaged in
      //the jar, read files into memory and load - consider improvements to this
      String filePath = valuesInQuotes[0];
      String data;
      if(new File(filePath).isAbsolute()){
        data = FileUtils.readFileToString(new File(filePath), "UTF8");
      }
      else{
        try {
          //assume running from within a jar,
          data = ResourceUtils.resource2String(filePath);
        } catch (Exception e) {
          //from IDE
          data = ResourceUtils.resource2String("/"+filePath);
        }
      }
      copyInStatement = copyInStatement.replace("'"+filePath+"'", "STDIN");
      totalInsertedRecords = copyManager.copyIn(copyInStatement, new StringReader(data));
    }
    log.info("Inserted " + totalInsertedRecords + " via COPY IN. Tenant: " + tenantId);
  }

  private void execute(String[] sql, boolean stopOnError,
      Handler<AsyncResult<List<String>>> replyHandler){

    long s = System.nanoTime();
    log.info("Executing multiple statements with id " + Arrays.hashCode(sql));
    List<String> results = new ArrayList<>();
    vertx.executeBlocking(dothis -> {
      Connection connection = null;
      Statement statement = null;
      boolean error = false;
      try {

        /* this should be  super user account that is in the config file */
        connection = getStandaloneConnection(null, false);
        connection.setAutoCommit(false);
        statement = connection.createStatement();

        for (int j = 0; j < sql.length; j++) {
          try {
            log.info("trying to execute: " + sql[j].substring(0, Math.min(sql[j].length()-1, 1000)));
            if(sql[j].trim().toUpperCase().startsWith("COPY ")){
              copyIn(sql[j], connection);
            }
            else{
              statement.executeUpdate(sql[j]); //NOSONAR
            }
            log.info("Successfully executed: " + sql[j].substring(0, Math.min(sql[j].length()-1, 400)));
          } catch (Exception e) {
            results.add(sql[j]);
            error = true;
            log.error(e.getMessage(),e);
            if(stopOnError){
              break;
            }
          }
        }
        try {
          if(error){
            connection.rollback();
            log.error("Rollback for: " + Arrays.hashCode(sql));
          }
          else{
            connection.commit();
            log.info("Successfully committed: " + Arrays.hashCode(sql));
          }
        } catch (Exception e) {
          error = true;
          log.error("Commit failed " + Arrays.hashCode(sql) + " " + e.getMessage(), e);
        }
      }
      catch(Exception e){
        log.error(e.getMessage(), e);
        error = true;
      }
      finally {
        try {
          if(statement != null) statement.close();
        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
        try {
          if(connection != null) connection.close();
        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
        if(error){
          dothis.fail("error");
        }
        else{
          dothis.complete();
        }
      }
    }, done -> {
      log.debug("execute timer for: " + Arrays.hashCode(sql) + " took " + (System.nanoTime()-s)/1000000);
      replyHandler.handle(Future.succeededFuture(results));
    });
  }

  /**
   * Start an embedded PostgreSQL using the configuration of {@link #getConnectionConfig()}.
   * It also sets embedded mode to true, see {@link #setIsEmbedded(boolean)}, but
   * doesn't change the configuration.
   *
   * @throws IOException  when starting embedded PostgreSQL fails
   */
  public void startEmbeddedPostgres() throws IOException {
    // starting Postgres
    embeddedMode = true;
    if (postgresProcess == null || !postgresProcess.isProcessRunning()) {
      // turns off the default functionality of unzipping on every run.
      IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
        .defaults(Command.Postgres)
        .artifactStore(
          new NonCachedPostgresArtifactStoreBuilder().defaults(Command.Postgres).
          download(new PostgresDownloadConfigBuilder().defaultsForCommand(Command.Postgres)
          // .progressListener(new LoggingProgressListener(logger, Level.ALL))
            .build())).build();
      PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getInstance(runtimeConfig);

      int port = postgreSQLClientConfig.getInteger(PORT);
      String username = postgreSQLClientConfig.getString(_USERNAME);
      String password = postgreSQLClientConfig.getString(_PASSWORD);
      String database = postgreSQLClientConfig.getString(DATABASE);

      String locale = "en_US.UTF-8";
      String OS = System.getProperty("os.name").toLowerCase();
      if(OS.indexOf("win") >= 0){
        locale = "american_usa";
      }

      final PostgresConfig config = new PostgresConfig(Version.Main.V10,
        new AbstractPostgresConfig.Net(DEFAULT_IP, port),
        new AbstractPostgresConfig.Storage(database),
        new AbstractPostgresConfig.Timeout(20000),
        new AbstractPostgresConfig.Credentials(username, password));

      config.getAdditionalInitDbParams().addAll(Arrays.asList(
        "-E", "UTF-8",
        "--locale", locale
      ));

      postgresProcess = runtime.prepare(config).start();

      log.info("embedded postgress started....");
    } else {
      log.info("embedded postgress is already running...");
    }
  }

  /**
   * .sql files
   * @param path
   */
  public void importFileEmbedded(String path) {
    // starting Postgres
    if (embeddedMode) {
      if (postgresProcess != null && postgresProcess.isProcessRunning()) {
        log.info("embedded postgress import starting....");

        postgresProcess.importFromFile(new File(path));

        log.info("embedded postgress import complete....");
      } else {
        log.info("embedded postgress is not running...");
      }
    }

  }

  /**
   * This is a blocking call - run in an execBlocking statement
   * import data in a tab delimited file into columns of an existing table
   * Using only default values of the COPY FROM STDIN Postgres command
   * @param path - path to the file
   * @param tableName - name of the table to import the content into
   */
  public void importFile(String path, String tableName) {

   long recordsImported[] = new long[]{-1};
   vertx.<String>executeBlocking(dothis -> {

    try {
      String host = postgreSQLClientConfig.getString(HOST);
      int port = postgreSQLClientConfig.getInteger(PORT);
      String user = postgreSQLClientConfig.getString(_USERNAME);
      String pass = postgreSQLClientConfig.getString(_PASSWORD);
      String db = postgreSQLClientConfig.getString(DATABASE);

      log.info("Connecting to " + db);

      Connection con = DriverManager.getConnection(
        "jdbc:postgresql://"+host+":"+port+"/"+db, user , pass);

      log.info("Copying text data rows from stdin");

      CopyManager copyManager = new CopyManager((BaseConnection) con);

      FileReader fileReader = new FileReader(path);
      recordsImported[0] = copyManager.copyIn("COPY "+tableName+" FROM STDIN", fileReader );

    } catch (Exception e) {
      log.error(messages.getMessage("en", MessageConsts.ImportFailed), e);
      dothis.fail(e);
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
      closeAllClients();
      LogUtil.formatLogMessage(PostgresClient.class.getName(), "stopEmbeddedPostgres", "called stop on embedded postgress ...");
      postgresProcess.stop();
      embeddedMode = false;
    }
  }

  public static String convertToPsqlStandard(String tenantId){
    return tenantId.toLowerCase() + "_" + moduleName;
  }

  public static String getModuleName(){
    return moduleName;
  }

  /**
   * assumes column cames are all lower case with multi word column names
   * separated by an '_'
   * @param str
   * @return
   */
  private String columnNametoCamelCaseWithset(String str){
    StringBuilder sb = new StringBuilder(str);
    sb.replace(0, 1, String.valueOf(Character.toUpperCase(sb.charAt(0))));
    for (int i = 0; i < sb.length(); i++) {
        if (sb.charAt(i) == '_') {
            sb.deleteCharAt(i);
            sb.replace(i, i+1, String.valueOf(Character.toUpperCase(sb.charAt(i))));
        }
    }
    return "set"+sb.toString();
  }

  /**
   * returns ParsedQuery with:
   * 1. Original query stripped of the order by, limit and offset clauses (if they existed in the query)
   * 2. Original query stripped of the limit and offset clauses (if they existed in the query)
   * 3. where clause part of query (included in the stripped query)
   * 4. original order by clause that was removed (or null)
   * 5. original limit clause that was removed (or null)
   * 6. original offset clause that was removed (or null)
   * @param query
   * @return
   */
  static ParsedQuery parseQuery(String query) {
    log.debug("parseQuery " + query);
    List<OrderByElement> orderBy = null;
    net.sf.jsqlparser.statement.select.Limit limit = null;
    Expression where = null;
    net.sf.jsqlparser.statement.select.Offset offset = null;
    long start = System.nanoTime();
    String queryWithoutLimitOffset = "";
    try {
      try {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(query);
        Select selectStatement = (Select) statement;
        orderBy = ((PlainSelect) selectStatement.getSelectBody()).getOrderByElements();
        limit = ((PlainSelect) selectStatement.getSelectBody()).getLimit();
        offset = ((PlainSelect) selectStatement.getSelectBody()).getOffset();
        where = ((PlainSelect) selectStatement.getSelectBody()).getWhere();
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }

      int startOfLimit = getLastStartPos(query, "limit");
      if(limit != null){
        String suffix = Pattern.compile(limit.toString().trim(), Pattern.CASE_INSENSITIVE).matcher(query.substring(startOfLimit)).replaceFirst("");
        query = query.substring(0, startOfLimit) + suffix;
      }
      else if(startOfLimit != -1){
        //offset returns null if it was placed before the limit although postgres does allow this
        //we are here if offset appears in the query and not within quotes
        query = query.substring(0, startOfLimit) +
        Pattern.compile("limit\\s+[\\d]+", Pattern.CASE_INSENSITIVE).matcher(query.substring(startOfLimit)).replaceFirst("");
      }

      int startOfOffset = getLastStartPos(query, "offset");
      if(offset != null){
        String suffix = Pattern.compile(offset.toString().trim(), Pattern.CASE_INSENSITIVE).matcher(query.substring(startOfOffset)).replaceFirst("");
        query = query.substring(0, startOfOffset) + suffix;
      }
      else if(startOfOffset != -1){
        //offset returns null if it was placed before the limit although postgres does allow this
        //we are here if offset appears in the query and not within quotes
        query = query.substring(0, startOfOffset) +
        Pattern.compile("offset\\s+[\\d]+", Pattern.CASE_INSENSITIVE).matcher(query.substring(startOfOffset)).replaceFirst("");
      }

      queryWithoutLimitOffset = query;

      //in the rare case where the order by clause somehow appears in the where clause
      int startOfOrderBy = getLastStartPos(query, "order by");
      if(orderBy != null){
        StringBuilder sb = new StringBuilder("order by[ ]+");
        int size = orderBy.size();
        for (int i = 0; i < size; i++) {
          sb.append(orderBy.get(i).toString().replaceAll(" ", "[ ]+"));
          if(i<size-1){
            sb.append(",?[ ]+");
          }
        }
        String regex = escape(sb.toString().trim());
        query = query.substring(0, startOfOrderBy) +
            Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(query.substring(startOfOrderBy)).replaceFirst("");
      }
      else if(startOfOrderBy != -1){
        //offset returns null if it was placed before the limit although postgres does allow this
        //we are here if offset appears in the query and not within quotes
        query = query.substring(0, startOfOrderBy) +
        Pattern.compile("order by.*", Pattern.CASE_INSENSITIVE).matcher(query.substring(startOfOrderBy)).replaceFirst("");
      }
   }
   catch(Exception e){
     log.error(e.getMessage(), e);
   }

   ParsedQuery pq = new ParsedQuery();
   pq.setCountFuncQuery(query);
   pq.setQueryWithoutLimOff(queryWithoutLimitOffset);
   if(where != null){
     pq.setWhereClause( where.toString() );
   }
   if(orderBy != null){
     pq.setOrderByClause( orderBy.toString() );
   }
   if(limit != null){
     pq.setLimitClause( limit.toString() );
   }
   if(offset != null){
     pq.setOffsetClause( offset.toString() );
   }
   long end = System.nanoTime();
   log.debug("Parse query for count_estimate function (ns) " + (end-start));
   return pq;
  }

  private static String escape(String str){
    StringBuilder sb = new StringBuilder();
    for (char c : str.toCharArray())
    {
        switch(c)
        {
            case '(':
            case ')':
            case '\\':
                sb.append('\\');
                // intended fall-through
            default:
                sb.append(c);
        }
    }
    return sb.toString();
  }

  /**
   * Return the last position of <code>token</code> in <code>query</code> skipping
   * standard SQL strings like 'some string' and C-style SQL strings like E'some string'.
   * @param query  where to search
   * @param token  what to search for
   * @return position (starting at 0), or -1 if not found
   */
  static int getLastStartPos(String query, String token) {
    String quotedString       = "(?<!E)'(?:[^']|'')*'";
    String quotedStringCStyle = "(?<=E)'(?:[^'\\\\]|\\.)*'";
    Matcher matcher = Pattern.compile(
        "(?:[^'\\\\]|\\.|" + quotedString + "|" + quotedStringCStyle + ")*"
        + "\\b(" + Pattern.quote(token) + ")\\b.*", Pattern.CASE_INSENSITIVE).matcher(query);
    if (! matcher.matches()) {
      return -1;
    }
    return matcher.start(1);
  }
}
