package org.folio.rest.persist;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.crypto.SecretKey;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
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
import org.folio.rest.tools.utils.NaiveSQLParse;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.tools.utils.ResourceUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;

import freemarker.template.TemplateException;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLRowStream;
import io.vertx.ext.sql.UpdateResult;
import java.util.Optional;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.folio.rest.tools.utils.NetworkUtils;

import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
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

  private static final String    SELECT = "SELECT ";
  private static final String    FROM   = " FROM ";
  private static final String    UPDATE = "UPDATE ";
  private static final String    SET    = " SET ";
  private static final String    WHERE  = " WHERE ";
  private static final String    AND    = " AND ";
  private static final String    INSERT_CLAUSE = "INSERT INTO ";

  private static final String    COUNT = "COUNT(%s)";
  private static final String    COLUMN_CONTROL_REGEX = "(?<=(?i)SELECT )(.*)(?= (?i)FROM )";

  private static final Pattern   OFFSET_MATCH_PATTERN = Pattern.compile("(?<=(?i)OFFSET\\s)(?:\\s*)(\\d+)(?=\\b)");
  private static final Pattern   DISTINCT_ON_MATCH_PATTERN = Pattern.compile("(?i)((DISTINCT) (?i)ON \\((.*)\\) ).*(?i)FROM");

  private static final String    _PASSWORD = "password"; //NOSONAR
  private static final String    _USERNAME = "username";
  private static final String    HOST      = "host";
  private static final String    PORT      = "port";
  private static final String    DATABASE  = "database";
  private static final String    DEFAULT_IP = "127.0.0.1"; //NOSONAR

  private static final String    STATS_KEY = PostgresClient.class.getName();

  private static final String    GET_STAT_METHOD = "get";
  private static final String    COUNT_STAT_METHOD = "count";
  private static final String    SAVE_STAT_METHOD = "save";
  private static final String    UPDATE_STAT_METHOD = "update";
  private static final String    DELETE_STAT_METHOD = "delete";
  private static final String    JOIN_STAT_METHOD = "join";
  private static final String    PROCESS_RESULTS_STAT_METHOD = "processResults";

  private static final String    SPACE = " ";
  private static final String    DOT = ".";
  private static final String    COMMA = ",";
  private static final String    SEMI_COLON = ";";

  private static final String    COUNT_FIELD = "count";

  private static EmbeddedPostgres embeddedPostgres;
  private static boolean         embeddedMode             = false;
  private static String          configPath               = null;
  private static ObjectMapper    mapper                   = ObjectMapperTool.getMapper();

  private static MultiKeyMap<Object, PostgresClient> connectionPool = MultiKeyMap.multiKeyMap(new HashedMap<>());

  private static final String    MODULE_NAME              = PomReader.INSTANCE.getModuleName();

  private static final String CLOSE_FUNCTION_POSTGRES = "WINDOW|IMMUTABLE|STABLE|VOLATILE|"
      +"CALLED ON NULL INPUT|RETURNS NULL ON NULL INPUT|STRICT|"
      +"SECURITY INVOKER|SECURITY DEFINER|SET\\s.*|AS\\s.*|COST\\s\\d.*|ROWS\\s.*";

  private static final Pattern POSTGRES_IDENTIFIER = Pattern.compile("^[a-zA-Z_][0-9a-zA-Z_]{0,62}$");

  private static final List<Map.Entry<String,Pattern>> REMOVE_FROM_COUNT_ESTIMATE= new java.util.ArrayList<>();

  private static final Logger log = LoggerFactory.getLogger(PostgresClient.class);

  private static int embeddedPort            = -1;

  private final Vertx vertx;
  private JsonObject postgreSQLClientConfig = null;
  private final Messages messages           = Messages.getInstance();
  private AsyncSQLClient client;
  private final String tenantId;
  private final String schemaName;
  private String idField                     = "_id";

  private String returningIdTemplate         = " RETURNING ${id} ";
  private String returningId                 = " RETURNING _id ";

  static {
    REMOVE_FROM_COUNT_ESTIMATE.add(new SimpleEntry<>("LIMIT", Pattern.compile("LIMIT\\s+[\\d]+(?=(([^']*'){2})*[^']*$)", 2)));
    REMOVE_FROM_COUNT_ESTIMATE.add(new SimpleEntry<>("OFFSET", Pattern.compile("OFFSET\\s+[\\d]+(?=(([^']*'){2})*[^']*$)", 2)));
    REMOVE_FROM_COUNT_ESTIMATE.add(new SimpleEntry<>("ORDER BY", Pattern.compile("ORDER BY(([^']*'){2})*\\s+(desc|asc|)", 2)));
  }

  protected PostgresClient(Vertx vertx, String tenantId) throws Exception {
    this.tenantId = tenantId;
    this.vertx = vertx;
    this.schemaName = convertToPsqlStandard(tenantId);
    init();
  }

  /**
   * test constructor for unit testing
   *
   * @param tenantId
   */
  private PostgresClient() {
    this.tenantId = "test";
    this.vertx = null;
    this.schemaName = convertToPsqlStandard(tenantId);
    log.warn("Instantiating test Postgres client! Only use with tests!");
  }

  static PostgresClient testClient() {
    return new PostgresClient();
  }

  /**
   * Log the duration since startNanoTime as a debug message.
   * @param description  text for the log entry
   * @param sql  additional text for the log entry
   * @param startNanoTime  start time as returned by System.nanoTime()
   */
  private void logTimer(String description, String sql, long startNanoTime) {
    if (! log.isDebugEnabled()) {
      return;
    }
    logTimer(description, sql, startNanoTime, System.nanoTime());
  }

  /**
   * Log the duration between startNanoTime and endNanoTime as a debug message.
   * @param description  text for the log entry
   * @param sql  additional text for the log entry
   * @param startNanoTime  start time in nanoseconds
   * @param endNanoTime  end time in nanoseconds
   */
  private void logTimer(String description, String sql, long startNanoTime, long endNanoTime) {
    log.debug(description + " timer: " + sql + " took " + ((endNanoTime - startNanoTime) / 1000000) + " ms");
  }

  /**
   * Log the duration since startNanoTime at the StatsTracker and as a debug message.
   * @param descriptionKey  key for StatsTracker and text for the log entry
   * @param sql  additional text for the log entry
   * @param startNanoTime  start time as returned by System.nanoTime()
   */
  private void statsTracker(String descriptionKey, String sql, long startNanoTime) {
    long endNanoTime = System.nanoTime();
    StatsTracker.addStatElement(STATS_KEY + DOT + descriptionKey, (endNanoTime - startNanoTime));
    if (log.isDebugEnabled()) {
      logTimer(descriptionKey, sql, startNanoTime, endNanoTime);
    }
  }

  /**
   * Set the name of the primary key field. Default is "_id".
   *
   * <p>Danger: getInstance(Vertx vertx) and getInstance(Vertx vertx, String tenantId) may
   * cache PostgresClient instances. This includes the name set for the id field.
   *
   * @param id  the new name
   */
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
   * DB_PORT environment variable or the
   * PostgreSQL configuration file. It is only used when <code>isEmbedded() == true</code>
   * when invoking the constructor.
   * <p>
   * This function must be invoked before calling the constructor.
   * <p>
   * Use -1 to not overwrite the port.
   *
   * <p>-1 is the default.
   *
   * @param port  the port for embedded PostgreSQL, or -1 to not overwrite the port
   */
  public static void setEmbeddedPort(int port){
    embeddedPort = port;
  }

  /**
   * @return the port number to use for embedded PostgreSQL, or -1 for not overwriting the
   *         port number of the configuration.
   * @see #setEmbeddedPort(int)
   */
  public static int getEmbeddedPort() {
    return embeddedPort;
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
        postgresClient.init();
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
  private static String decodePassword(String password) throws Exception {
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
  private static String createPassword(String password) throws Exception {
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
   * Set this instance's AsyncSQLClient that can connect to Postgres.
   * @param client  the new client
   */
  void setClient(AsyncSQLClient client) {
    this.client = client;
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

  private void init() throws Exception {

    /** check if in pom.xml this prop is declared in order to work with encrypted
     * passwords for postgres embedded - this is a dev mode only feature */
    String secretKey = System.getProperty("postgres_secretkey_4_embeddedmode");

    if(secretKey != null){
      AES.setSecretKey(secretKey);
    }

    postgreSQLClientConfig = getPostgreSQLClientConfig(tenantId, schemaName, Envs.allDBConfs());

    logPostgresConfig();
    client = io.vertx.ext.asyncsql.PostgreSQLClient.createNonShared(vertx, postgreSQLClientConfig);
  }

  /**
   * Get PostgreSQL configuration, invokes setIsEmbedded(true) if needed.
   * @return configuration for PostgreSQL
   * @throws Exception  on password decryption or encryption failure
   */
  @SuppressWarnings("squid:S2068")  /* Suppress "Credentials should not be hard-coded" -
    The docker container does not expose the embedded postges port.
    Moving the hard-coded credentials into some default config file
    doesn't remove them from the build. */
  static JsonObject getPostgreSQLClientConfig(String tenantId, String schemaName, JsonObject environmentVariables)
      throws Exception {
    // static function for easy unit testing
    JsonObject config = environmentVariables;
    if (config.size() > 0) {
      log.info("DB config read from environment variables");
    } else {
      //no env variables passed in, read for module's config file
      config = LoadConfs.loadConfig(getConfigFilePath());
      // LoadConfs.loadConfig writes its own log message
    }
    if (config == null) {
      if (NetworkUtils.isLocalPortFree(EMBEDDED_POSTGRES_PORT)) {
        log.info("No DB configuration found, starting embedded postgres with default config");
        setIsEmbedded(true);
      } else {
        log.info("No DB configuration found, using default config, port is already in use");
      }
      config = new JsonObject();
      config.put(_USERNAME, _USERNAME);
      config.put(_PASSWORD, _PASSWORD);
      config.put(HOST, DEFAULT_IP);
      config.put(PORT, EMBEDDED_POSTGRES_PORT);
      config.put(DATABASE, "postgres");
    }
    if (tenantId.equals(DEFAULT_SCHEMA)) {
      config.put(_PASSWORD, decodePassword( config.getString(_PASSWORD) ));
    } else {
      log.info("Using schema: " + tenantId);
      config.put(_USERNAME, schemaName);
      config.put(_PASSWORD, createPassword(tenantId));
    }
    if(embeddedPort != -1 && embeddedMode){
      //over ride the declared default port - coming from the config file and use the
      //passed in port as well. useful when multiple modules start up an embedded postgres
      //in a single server.
      config.put(PORT, embeddedPort);
    }
    return config;
  }

  /**
   * Log postgreSQLClientConfig.
   */
  @SuppressWarnings("squid:S2068")  // Suppress "Credentials should not be hard-coded"
                                    // "'password' detected in this expression".
                                    // False positive: Password is configurable, here we remove it from the log.
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
   * Start a SQL transaction.
   *
   * <p>Use the AsyncResult<SQLConnection> result to invoke any of the
   * functions that take that result as first parameter for the commands
   * within the transaction.
   *
   * <p>To close the open connection invoke the END or ROLLBACK
   * function. Note that after a failing operation (for example some UPDATE)
   * both the connection and the transaction remain open to let the caller
   * decide what to do.
   *
   * @see #endTx(Object, Handler)
   * @see #rollbackTx(Future, Handler)
   * @param done - the result is the current connection
   */
  //@Timer
  public void startTx(Handler<AsyncResult<SQLConnection>> done) {
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

  /**
   * Rollback a SQL transaction started on the connection. This closes the connection.
   *
   * @see #startTx(Handler)
   * @param conn  the connection with an open transaction
   * @param done  success or failure
   */
  //@Timer
  public void rollbackTx(AsyncResult<SQLConnection> conn, Handler<AsyncResult<Void>> done) {
    SQLConnection sqlConnection = conn.result();
    sqlConnection.rollback(res -> {
      sqlConnection.close();
      if (res.failed()) {
        log.error(res.cause().getMessage(), res.cause());
        done.handle(Future.failedFuture(res.cause()));
      } else {
        done.handle(Future.succeededFuture());
      }
    });
  }

  /**
   * Ends a SQL transaction (commit) started on the connection. This closes the connection.
   *
   * @see #startTx(Handler)
   * @param conn  the connection with an open transaction
   * @param done  success or failure
   */
  //@Timer
  public void endTx(AsyncResult<SQLConnection> conn, Handler<AsyncResult<Void>> done) {
    SQLConnection sqlConnection = conn.result();
    sqlConnection.commit(res -> {
      sqlConnection.close();
      if (res.failed()) {
        log.error(res.cause().getMessage(), res.cause());
        done.handle(Future.failedFuture(res.cause()));
      } else {
        done.handle(Future.succeededFuture());
      }
    });
  }

  /**
   * The returned handler first closes the SQLConnection and then passes on the AsyncResult to handler.
   *
   * <p>The returned Handler ignores (but logs) any failure when opening the connection (conn) or
   * closing the connection and always passes on the AsyncResult<T>. This is in contrast to
   * io.vertx.ext.sql.HandlerUtil.closeAndHandleResult where the connection
   * closing failure suppresses any result or failure of the AsyncResult<T> input.
   *
   * @param conn  the SQLConnection to close
   * @param handler  where to pass on the input AsyncResult
   * @return the Handler
   */
  static <T> Handler<AsyncResult<T>> closeAndHandleResult(
      AsyncResult<SQLConnection> conn, Handler<AsyncResult<T>> handler) {

    return ar -> {
      if (conn.failed()) {
        log.error("Opening SQLConnection failed: " + conn.cause().getMessage(), conn.cause());
        handler.handle(ar);
        return;
      }
      conn.result().close(close -> {
        if (close.failed()) {
          log.error("Closing SQLConnection failed: " + close.cause().getMessage(), close.cause());
        }
        handler.handle(ar);
      });
    };
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
    vertx.runOnContext(v -> {
      client.getConnection(res -> {
        if (res.succeeded()) {
          SQLConnection connection = res.result();

          StringBuilder clientIdField = new StringBuilder("");
          StringBuilder clientId = new StringBuilder("");
          if(id != null){
            clientId.append("'").append(id).append("',");
            clientIdField.append(idField).append(COMMA);
          }
          String returning = "";
          if(returnId){
            returning = " RETURNING " + idField;
          }

          try {
            String upsertClause = "";
            if(upsert){
              upsertClause = " ON CONFLICT ("+idField+") DO UPDATE SET " +
                DEFAULT_JSONB_FIELD_NAME + " = EXCLUDED."+DEFAULT_JSONB_FIELD_NAME + SPACE;
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
            connection.queryWithParams(INSERT_CLAUSE + schemaName + DOT + table +
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
                statsTracker(SAVE_STAT_METHOD, table, start);
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
    });
  }

  public void save(String table, String id, Object entity, boolean returnId, boolean upsert, Handler<AsyncResult<String>> replyHandler) {
    save(table, id, entity, returnId, upsert, true, replyHandler);
  }

  /**
   * Save entity in table. Use the transaction of sqlConnection. Return the created id via the replyHandler;
   * this is only the same as the id in the entity if some database trigger syncs them.
   * @param sqlConnection  connection with transaction
   * @param table  where to insert the entity record
   * @param entity  the record to insert
   * @param replyHandler  where to report success status and the created id
   */
  public void save(AsyncResult<SQLConnection> sqlConnection, String table, Object entity, Handler<AsyncResult<String>> replyHandler) {
    long start = System.nanoTime();

    log.debug("save called on " + table);
    SQLConnection connection = null;
    try {
      // connection not closed by this FUNCTION ONLY BY END TRANSACTION call!
      connection = sqlConnection.result();
      String pojo = pojo2json(entity);
      connection.queryWithParams(INSERT_CLAUSE + schemaName + DOT + table +
        " (" + DEFAULT_JSONB_FIELD_NAME + ") VALUES (?::JSON) RETURNING " + idField,
        new JsonArray().add(pojo), query -> {
          if (query.failed()) {
            replyHandler.handle(Future.failedFuture(query.cause()));
          } else {
            replyHandler.handle(Future.succeededFuture(query.result().getResults().get(0).getValue(0).toString()));
          }
          statsTracker(SAVE_STAT_METHOD, table, start);
        });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Save entity in table. Use the transaction of sqlConnection. Return the id of the id field (primary key)
   * via the replyHandler. If id (primary key) and the id of entity (jsonb field) are different you may need
   * a trigger in the database to sync them.
   * @param sqlConnection  connection with transaction
   * @param table  where to insert the entity record
   * @param id  the value for the id field (primary key); may be different from the id in entity (= in the jsonb field)
   * @param entity  the record to insert
   * @param replyHandler  where to report success status and the final id of the id field
   */
  public void save(AsyncResult<SQLConnection> sqlConnection, String table, String id, Object entity,
      Handler<AsyncResult<String>> replyHandler) {

    long start = System.nanoTime();

    log.debug("save (with connection and id) called on " + table);
    SQLConnection connection = null;
    try {
      // connection not closed by this FUNCTION ONLY BY END TRANSACTION call!
      connection = sqlConnection.result();
      String pojo = pojo2json(entity);
      connection.queryWithParams(INSERT_CLAUSE + schemaName + DOT + table +
        " (" + idField + ", " + DEFAULT_JSONB_FIELD_NAME + ") VALUES (?, ?::JSON) RETURNING " + idField,
        new JsonArray().add(id).add(pojo), query -> {
          if (query.failed()) {
            replyHandler.handle(Future.failedFuture(query.cause()));
          } else {
            replyHandler.handle(Future.succeededFuture(query.result().getResults().get(0).getValue(0).toString()));
          }
          statsTracker(SAVE_STAT_METHOD, table, start);
        });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Insert the entities into table using a single INSERT statement.
   * @param table  destination table to insert into
   * @param entities  each array element is a String with the content for the JSONB field of table
   * @param replyHandler  result, containing the id field for each inserted element of entities
   */
  public void saveBatch(String table, JsonArray entities, Handler<AsyncResult<ResultSet>> replyHandler) {
    saveBatch(table, entities, DEFAULT_JSONB_FIELD_NAME, replyHandler);
  }

  public void saveBatch(String table, JsonArray entities, String column, Handler<AsyncResult<ResultSet>> replyHandler) {
    long start = System.nanoTime();
    if (entities == null || entities.isEmpty()) {
      // return empty result
      ResultSet resultSet = new ResultSet(
          Collections.singletonList(idField), Collections.emptyList(), null);
      replyHandler.handle(Future.succeededFuture(resultSet));
      return;
    }
    client.getConnection(res -> {
      if (res.failed()) {
        replyHandler.handle(Future.failedFuture(res.cause()));
        return;
      }

      if (log.isInfoEnabled()) {
        log.info("starting: saveBatch size=" + entities.size());
      }
      SQLConnection connection = res.result();
      StringBuilder sql = new StringBuilder()
          .append(INSERT_CLAUSE)
          .append(schemaName).append(DOT).append(table)
          .append(" (").append(column).append(") VALUES (?)");
      for (int i=1; i<entities.size(); i++) {
        sql.append(",(?)");
      }
      sql.append(" RETURNING ").append(idField);

      connection.queryWithParams(sql.toString(), entities, queryRes -> {
        connection.close();
        if (queryRes.failed()) {
          log.error("saveBatch size=" + entities.size()
            + SPACE +
              queryRes.cause().getMessage(),
              queryRes.cause());
          statsTracker("saveBatchFailed", table, start);
          replyHandler.handle(Future.failedFuture(queryRes.cause()));
          return;
        }
        if (log.isInfoEnabled()) {
          log.info("success: saveBatch size=" + entities.size());
        }
        statsTracker("saveBatch", table, start);
        replyHandler.handle(Future.succeededFuture(queryRes.result()));
      });
    });
  }

  /***
   * Save a list of POJOs.
   * POJOs are converted to json and saved in a single sql call. The generated IDs of the
   * inserted records are returned in the ResultSet.
   * @param table  destination table to insert into
   * @param entities  each list element is a POJO
   * @param replyHandler  result, containing the id field for each inserted POJO
   */
  public void saveBatch(String table, List<Object> entities, Handler<AsyncResult<ResultSet>> replyHandler) {
    JsonArray jsonArray = new JsonArray();
    try {
      for (Object entity : entities) {
        String json = pojo2json(entity);
        jsonArray.add(json);
      }
    } catch (Exception e) {
      replyHandler.handle(Future.failedFuture(e));
      return;
    }
    saveBatch(table, jsonArray, replyHandler);
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

  public void update(AsyncResult<SQLConnection> conn, String table, Object entity, CQLWrapper filter, boolean returnUpdatedIds, Handler<AsyncResult<UpdateResult>> replyHandler) {
    String where = "";
    try {
      if (filter != null) {
        where = filter.toString();
      }
      update(conn, table, entity, DEFAULT_JSONB_FIELD_NAME, where, returnUpdatedIds, replyHandler);
    } catch (Exception e) {
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  public void update(AsyncResult<SQLConnection> conn, String table, Object entity, String jsonbField, String whereClause, boolean returnUpdatedIds, Handler<AsyncResult<UpdateResult>> replyHandler)
  {
    SQLConnection connection = null;
    if(conn != null){
      connection = conn.result();
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
    vertx.runOnContext(v -> {
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
        String q = UPDATE + schemaName + DOT + table + SET + jsonbField + " = ?::jsonb "  + whereClause
            + SPACE + returning;
        log.debug("doUpdate query = " + q);
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
          statsTracker(UPDATE_STAT_METHOD, table, start);
        });
      } catch (Exception e) {
        if(!transactionMode){
          connection.close();
        }
        log.error(e.getMessage(), e);
        replyHandler.handle(Future.failedFuture(e));
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
   *
   */
  public void update(String table, UpdateSection section, Criterion when, boolean returnUpdatedIdsCount,
      Handler<AsyncResult<UpdateResult>> replyHandler) {
    long start = System.nanoTime();
    vertx.runOnContext(v -> {
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
            String q = UPDATE + schemaName + DOT + table + SET + DEFAULT_JSONB_FIELD_NAME + " = jsonb_set(" + DEFAULT_JSONB_FIELD_NAME + ","
                + section.getFieldsString() + ", '" + section.getValue() + "', false) " + sb.toString() + SPACE + returning;
            log.debug("update query = " + q);
            connection.update(q, query -> {
              connection.close();
              if (query.failed()) {
                log.error(query.cause().getMessage(), query.cause());
                replyHandler.handle(Future.failedFuture(query.cause()));
              } else {
                replyHandler.handle(Future.succeededFuture(query.result()));
              }
              statsTracker(UPDATE_STAT_METHOD, table, start);
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
    });
  }

  public void delete(String table, CQLWrapper cql, Handler<AsyncResult<UpdateResult>> replyHandler) {
    String where = "";
    try {
      if (cql != null) {
        where = cql.toString();
      }
      doDelete(table, where, replyHandler);
    } catch (Exception e) {
      replyHandler.handle(Future.failedFuture(e));
    }
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

  /**
   * Delete as part of a transaction
   * @param conn
   * @param table
   * @param filter
   * @param replyHandler
   */
  public void delete(AsyncResult<SQLConnection> conn, String table, Criterion filter, Handler<AsyncResult<UpdateResult>> replyHandler) {
    SQLConnection sqlConnection = conn.result();
    StringBuilder sb = new StringBuilder();
    if (filter != null) {
      sb.append(filter.toString());
    }
    doDelete(sqlConnection, true, table, sb.toString(), replyHandler);
  }

  /**
   * delete based on jsons matching the field/value pairs in the pojo (which is first converted to json and then similar jsons are searched)
   *  --> do not use on large tables without checking as the @> will not use a btree
   * @param table
   * @param entity
   * @param replyHandler
   */
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

  public void delete(AsyncResult<SQLConnection> conn, String table, Object entity, Handler<AsyncResult<UpdateResult>> replyHandler) {
    SQLConnection sqlConnection = conn.result();
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
    vertx.runOnContext(v -> {
      try {
        String q = "DELETE FROM " + schemaName + DOT + table + " " + where;
        log.debug("doDelete query = " + q);
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
          statsTracker(DELETE_STAT_METHOD, table, start);
        });
      } catch (Exception e) {
        if(!transactionMode){
          connection.close();
        }
        log.error(e.getMessage(), e);
        replyHandler.handle(Future.failedFuture(e));
      }
    });
  }

  public <T> void get(String table, Class<T> clazz, String fieldName, String where,
      boolean returnCount, boolean returnIdField, boolean setId,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, fieldName, where, returnCount, returnIdField, setId, null, replyHandler);
  }

  public <T> void get(String table, Class<T> clazz, String fieldName, String where,
      boolean returnCount, boolean returnIdField, boolean setId, List<FacetField> facets,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, fieldName, where, returnCount, returnIdField, setId, facets, null, replyHandler);
  }

  public <T> void get(String table, Class<T> clazz, String fieldName, String where,
      boolean returnCount, boolean returnIdField, boolean setId, List<FacetField> facets, String distinctOn,
      Handler<AsyncResult<Results<T>>> replyHandler) {

    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        doGet(connection, false, table, clazz, fieldName, where, returnCount, returnIdField, setId, facets, distinctOn, replyHandler);
      }
      else{
        replyHandler.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  static class QueryHelper {
    final boolean transactionMode;
    String table;
    List<FacetField> facets;
    String selectQuery;
    String countQuery;
    int offset;
    public QueryHelper(boolean transactionMode, String table, List<FacetField> facets) {
      this.transactionMode = transactionMode;
      this.table = table;
      this.facets = facets;
      this.offset = 0;
    }
  }

  static class TotaledResults {
    final ResultSet set;
    final Integer total;
    public TotaledResults(ResultSet set, Integer total) {
      this.set = set;
      this.total = total;
    }
  }

  /**
   *
   * @param connection
   * @param transactionMode
   * @param table
   * @param clazz
   * @param fieldName
   * @param where
   * @param returnCount
   * @param returnIdField
   * @param setId
   * @param facets
   * @param replyHandler
   */
  private <T> void doGet(
    SQLConnection connection, boolean transactionMode, String table, Class<T> clazz,
    String fieldName, String where, boolean returnCount, boolean returnIdField, boolean setId,
    List<FacetField> facets, String distinctOn, Handler<AsyncResult<Results<T>>> replyHandler
  ) {

    vertx.runOnContext(v -> {
      try {
        QueryHelper queryHelper = buildSelectQueryHelper(transactionMode, table, fieldName, where, returnIdField, facets, distinctOn);

        if (returnCount) {
          processQueryWithCount(connection, queryHelper, GET_STAT_METHOD,
            totaledResults -> processResults(totaledResults.set, totaledResults.total, clazz, setId), replyHandler);
        } else {
          processQuery(connection, queryHelper, null, GET_STAT_METHOD,
            totaledResults -> processResults(totaledResults.set, totaledResults.total, clazz, setId), replyHandler);
        }

      } catch (Exception e) {
        if (!transactionMode) {
          connection.close();
        }
        log.error(e.getMessage(), e);
        replyHandler.handle(Future.failedFuture(e));
      }
    });
  }

  public <T> void streamGet(
    String table, Class<T> clazz, String fieldName, String where, boolean returnIdField,
    boolean setId, String distinctOn, Handler<T> streamHandler, Handler<AsyncResult<?>> replyHandler
  ) {
    streamGet(table, clazz, fieldName, where, returnIdField, setId, null, distinctOn, streamHandler, replyHandler);
  }

  public <T> void streamGet(
      String table, Class<T> clazz, String fieldName, String where,
      boolean returnIdField, boolean setId, List<FacetField> facets,
      Handler<T> streamHandler, Handler<AsyncResult<?>> replyHandler
    ) {
    streamGet(table, clazz, fieldName, where, returnIdField, setId, facets, null, streamHandler, replyHandler);
  }

  public <T> void streamGet(
    String table, Class<T> clazz, String fieldName, String where,
    boolean returnIdField, boolean setId, List<FacetField> facets, String distinctOn,
    Handler<T> streamHandler, Handler<AsyncResult<?>> replyHandler
  ) {
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        doStreamGet(connection, false, table, clazz, fieldName, where, returnIdField, setId, facets, distinctOn, streamHandler, replyHandler);
      }
      else{
        replyHandler.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  /**
   *
   * @param connection
   * @param transactionMode
   * @param table
   * @param clazz
   * @param fieldName
   * @param where
   * @param returnIdField
   * @param setId
   * @param facets
   * @param streamHandler
   * @param replyHandler
   */
  private <T> void doStreamGet(
    SQLConnection connection, boolean transactionMode, String table, Class<T> clazz,
    String fieldName, String where, boolean returnIdField, boolean setId, List<FacetField> facets, String distinctOn,
    Handler<T> streamHandler, Handler<AsyncResult<?>> replyHandler
  ) {

    vertx.runOnContext(v1 -> {
      try {
        QueryHelper queryHelper = buildSelectQueryHelper(transactionMode, table, fieldName, where, returnIdField, facets, distinctOn);

        connection.queryStream(queryHelper.selectQuery, stream -> {
          if (stream.succeeded()) {

            SQLRowStream sqlRowStream = stream.result();

            ResultsHelper<T> resultsHelper = new ResultsHelper<>(sqlRowStream, clazz, setId);

            boolean isAuditFlavored = isAuditFlavored(resultsHelper.clazz);

            Map<String, Method> externalColumnSettters = getExternalColumnSetters(
              resultsHelper.columnNames,
              resultsHelper.clazz,
              isAuditFlavored
            );

            String idPropName = databaseFieldToPojoSetter(idField);

            sqlRowStream.resultSetClosedHandler(v ->  sqlRowStream.moreResults()).handler(r -> {
              JsonObject row = convertRowStreamArrayToObject(sqlRowStream, r);
              try {
                streamHandler.handle((T) deserializeRow(resultsHelper, externalColumnSettters, isAuditFlavored, idPropName, row));
              } catch (Exception e) {
                sqlRowStream.close();
                if (!transactionMode) {
                  connection.close();
                }
                log.error(e.getMessage(), e);
                replyHandler.handle(Future.failedFuture(e));
              }
            }).endHandler(v2 -> {
              if (!transactionMode) {
                connection.close();
              }
              replyHandler.handle(Future.succeededFuture());
            });

          } else {
            if (!transactionMode) {
              connection.close();
            }
            log.error(stream.cause().getMessage(), stream.cause());
            replyHandler.handle(Future.failedFuture(stream.cause()));
          }
        });

      } catch (Exception e) {
        if (!transactionMode) {
          connection.close();
        }
        log.error(e.getMessage(), e);
        replyHandler.handle(Future.failedFuture(e));
      }
    });
  }

  private JsonObject convertRowStreamArrayToObject(SQLRowStream sqlRowStream, JsonArray rowAsArray) {
    Map<String, Object> rowMap = new HashMap<>();
    for(String colName : sqlRowStream.columns()) {
      rowMap.put(colName, rowAsArray.getValue(sqlRowStream.column(colName)));
    }
    return new JsonObject(rowMap);
  }

  QueryHelper buildSelectQueryHelper(
    boolean transactionMode, String table, String fieldName,
    String where, boolean returnIdField, List<FacetField> facets, String distinctOn
  ) {
    String addIdField = "";
    if (returnIdField) {
      addIdField = COMMA + idField;
    }

    if (!"null".equals(fieldName) && fieldName.contains("*")) {
      // if we are requesting all fields (*) , then dont add the id field to the select
      // this will return two id columns which will create ambiguity in facet queries
      addIdField = "";
    }

    QueryHelper queryHelper = new QueryHelper(transactionMode, table, facets);

    String distinctOnClause = "";
    if (distinctOn != null && !distinctOn.isEmpty()) {
      String[] fields = distinctOn.split(",");
      String distinctOnFields = Stream.of(fields)
          .map(str -> String.format("lower(f_unaccent(%s))", str))
          .collect(Collectors.joining(","));
      distinctOnClause = String.format("DISTINCT ON (%s) ", distinctOnFields);
    }

    queryHelper.selectQuery = new StringBuilder()
      .append(SELECT)
      .append(distinctOnClause)
      .append(fieldName)
      .append(addIdField)
      .append(FROM)
      .append(schemaName)
      .append(DOT)
      .append(table)
      .append(SPACE)
      .append(where)
      .toString();

    return queryHelper;
  }

  /**
   *
   * @param connection
   * @param queryHelper
   * @param statMethod
   * @param resultSetMapper
   * @param replyHandler
   */
  <T> void processQueryWithCount(
    SQLConnection connection, QueryHelper queryHelper, String statMethod,
    Function<TotaledResults, T> resultSetMapper, Handler<AsyncResult<T>> replyHandler
  ) throws IOException, TemplateException {
    long start = System.nanoTime();

    prepareCountQuery(queryHelper);

    log.debug("Attempting count query: " + queryHelper.countQuery);
    connection.querySingle(queryHelper.countQuery, countQuery -> {
      try {
        if (countQuery.failed()) {
          if (!queryHelper.transactionMode) {
            connection.close();
          }
          log.error(countQuery.cause().getMessage(), countQuery.cause());
          replyHandler.handle(Future.failedFuture(countQuery.cause()));
          return;
        }

        int total = countQuery.result().getInteger(0);

        long countQueryTime = (System.nanoTime() - start);
        StatsTracker.addStatElement(STATS_KEY + COUNT_STAT_METHOD, countQueryTime);
        log.debug("timer: get " + queryHelper.countQuery + " (ns) " + countQueryTime);

        if (total <= queryHelper.offset) {
          if (!queryHelper.transactionMode) {
            connection.close();
          }
          log.debug("Skipping query due to no results expected!");
          ResultSet emptyResultSet = new ResultSet(Collections.singletonList(idField), Collections.emptyList(), null);
          replyHandler.handle(Future.succeededFuture(resultSetMapper.apply(new TotaledResults(emptyResultSet, total))));
          return;
        }

        processQuery(connection, queryHelper, total, statMethod, resultSetMapper, replyHandler);
      } catch (Exception e) {
        if (!queryHelper.transactionMode) {
          connection.close();
        }
        log.error(e.getMessage(), e);
        replyHandler.handle(Future.failedFuture(e));
      }
    });
}

  /**
   *
   * @param queryHelper
   */
  void prepareCountQuery(QueryHelper queryHelper) throws IOException, TemplateException {
    String offsetClause = null;

    ParsedQuery parsedQuery = parseQuery(queryHelper.selectQuery);

    queryHelper.countQuery = parsedQuery.getCountQuery();

    if (queryHelper.facets != null && !queryHelper.facets.isEmpty() && queryHelper.table != null) {
      FacetManager facetManager = buildFacetManager(queryHelper.table, parsedQuery, queryHelper.facets);
      // this method call invokes freemarker templating
      queryHelper.selectQuery = facetManager.generateFacetQuery();
      queryHelper.countQuery = facetManager.getCountQuery();

      offsetClause = facetManager.getOffsetClause();
    } else {
      offsetClause = parsedQuery.getOffsetClause();
    }

    if (offsetClause != null) {
      Matcher matcher = OFFSET_MATCH_PATTERN.matcher(offsetClause);
      if (matcher.find()) {
          queryHelper.offset = Integer.parseInt(matcher.group(1));
      }
    }
  }

  /**
   *
   * @param connection
   * @param queryHelper
   * @param total
   * @param statMethod
   * @param resultSetMapper
   * @param replyHandler
   */
  <T> void processQuery(
    SQLConnection connection, QueryHelper queryHelper, Integer total, String statMethod,
    Function<TotaledResults, T> resultSetMapper, Handler<AsyncResult<T>> replyHandler
  ) {
    long start = System.nanoTime();
    log.debug("Attempting query: " + queryHelper.selectQuery);
    connection.query(queryHelper.selectQuery, query -> {
      if (!queryHelper.transactionMode) {
        connection.close();
      }
      try {
        if (query.failed()) {
          log.error(query.cause().getMessage(), query.cause());
          replyHandler.handle(Future.failedFuture(query.cause()));
        } else {
          replyHandler.handle(Future.succeededFuture(resultSetMapper.apply(new TotaledResults(query.result(), total))));
        }
        long queryTime = (System.nanoTime() - start);
        StatsTracker.addStatElement(STATS_KEY + statMethod, queryTime);
        log.debug("timer: get " + queryHelper.selectQuery + " (ns) " + queryTime);
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        replyHandler.handle(Future.failedFuture(e));
      }
    });
  }

  /**
   * function uses freemarker templating, the template will be loaded the first time
   * should take about 70-80 milli - after that the template gets cached and will be sub milli
   * @param tableName
   * @param where
   * @param facets
   * @return
   */
  private FacetManager buildFacetManager(String tableName, ParsedQuery parsedQuery, List<FacetField> facets) {
    FacetManager fm = new FacetManager(schemaName + DOT + tableName);
    if (parsedQuery.getWhereClause() != null) {
      fm.setWhere(" where " + parsedQuery.getWhereClause());
    }
    fm.setSupportFacets(facets);
    fm.setIdField(idField);
    fm.setLimitClause(parsedQuery.getLimitClause());
    fm.setOffsetClause(parsedQuery.getOffsetClause());
    fm.setMainQuery(parsedQuery.getQueryWithoutLimOff());
    fm.setSchema(schemaName);
    fm.setCountQuery(org.apache.commons.lang.StringEscapeUtils.escapeSql(parsedQuery.getCountQuery()));
    return fm;
  }

  /**
   * pass in an entity that is fully / partially populated and the query will return all records matching the
   * populated fields in the entity - note that this queries the jsonb object, so should not be used to query external
   * fields
   *
   * @param <T>  type of the query entity and the result entity
   * @param table  database table to query
   * @param entity  contains the fields to use for the query
   * @param replyHandler  the result contains the entities found
   */
  //@Timer
  public <T> void get(String table, T entity, boolean returnCount,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table,  entity, returnCount, true, replyHandler);
  }

  public <T> void get(String table, T entity, boolean returnCount, boolean returnIdField,
      Handler<AsyncResult<Results<T>>> replyHandler) {
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
    Class<T> clazz = (Class<T>) entity.getClass();
    get(table, clazz, DEFAULT_JSONB_FIELD_NAME, WHERE + DEFAULT_JSONB_FIELD_NAME
      + "@>'" + pojo + "' ", returnCount, returnIdField, setId, replyHandler);
  }

  public <T> void get(String table, T entity, String[] fields, boolean returnCount, boolean returnIdField,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, entity, fields, returnCount, returnIdField, -1, -1, replyHandler);
  }

  public <T> void get(String table, T entity, String[] fields, boolean returnCount,
      boolean returnIdField, int offset, int limit,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    boolean setId = true;
    if(returnIdField == false){
      //if no id fields then cannot setId from extrnal column into json object
      setId = false;
    }
    StringBuilder sb = new StringBuilder();
    if(offset != -1){
      sb.append(SPACE).append(new Offset(offset).toString()).append(SPACE);
    }
    if(limit != -1){
      sb.append(SPACE).append(new Limit(limit).toString()).append(SPACE);
    }
    String pojo = null;
    try {
      pojo = pojo2json(entity);
    } catch (Exception e) {
      replyHandler.handle(Future.failedFuture(e));
      return;
    }
    String fieldsStr = Arrays.toString(fields);
    Class<T> clazz = (Class<T>) entity.getClass();
    get(table, clazz, fieldsStr.substring(1, fieldsStr.length()-1), WHERE + DEFAULT_JSONB_FIELD_NAME
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
  public <T> void get(String table, Class<T> clazz, Criterion filter, boolean returnCount,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, filter, returnCount, true, replyHandler);
  }

  public <T> void get(String table, Class<T> clazz, String[] fields, CQLWrapper filter,
      boolean returnCount, boolean setId,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, fields, filter, returnCount, setId, null, replyHandler);
  }

  public <T> void get(String table, Class<T> clazz, String[] fields, CQLWrapper filter,
      boolean returnCount, boolean setId, List<FacetField> facets,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    String where = "";
    try {
      if (filter != null) {
        where = filter.toString();
      }
      String fieldsStr = Arrays.toString(fields);
      get(table, clazz, fieldsStr.substring(1, fieldsStr.length()-1), where, returnCount, true, setId, facets, replyHandler);
    } catch (Exception e) {
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  public <T> void get(String table, Class<T> clazz, String[] fields, String filter,
      boolean returnCount, boolean setId,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    String where = "";
    if(filter != null){
      where = filter;
    }
    String fieldsStr = Arrays.toString(fields);
    get(table, clazz, fieldsStr.substring(1, fieldsStr.length()-1), where, returnCount, true, setId, replyHandler);
  }

  public <T> void get(String table, Class<T> clazz, String filter,
      boolean returnCount, boolean setId,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    String where = "";
    if(filter != null){
      where = filter;
    }
    get(table, clazz, new String[]{DEFAULT_JSONB_FIELD_NAME}, where, returnCount, setId, replyHandler);
  }

  public <T> void get(String table, Class<T> clazz, String[] fields, CQLWrapper filter,
      boolean returnCount, Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, fields, filter, returnCount, true, replyHandler);
  }

  public <T> void get(String table, Class<T> clazz, CQLWrapper filter, boolean returnCount,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, new String[]{DEFAULT_JSONB_FIELD_NAME}, filter, returnCount, true, replyHandler);
  }

  public <T> void get(String table, Class<T> clazz, CQLWrapper filter, boolean returnCount, boolean setId,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, new String[]{DEFAULT_JSONB_FIELD_NAME}, filter, returnCount, setId, replyHandler);
  }

  public <T> void get(String table, Class<T> clazz, CQLWrapper filter,
      boolean returnCount, boolean setId, List<FacetField> facets,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, new String[]{DEFAULT_JSONB_FIELD_NAME}, filter, returnCount, setId, facets, replyHandler);
  }

  public <T> void get(String table, Class<T> clazz, Criterion filter, boolean returnCount, boolean setId,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, filter, returnCount, setId, null, replyHandler);
  }

  public <T> void get(AsyncResult<SQLConnection> conn, String table, Class<T> clazz, Criterion filter, boolean returnCount, boolean setId,
      Handler<AsyncResult<Results<T>>> replyHandler) {
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
  public <T> void get(String table, Class<T> clazz, Criterion filter, boolean returnCount, boolean setId,
      List<FacetField> facets, Handler<AsyncResult<Results<T>>> replyHandler) {

    get(null, table, clazz, filter, returnCount, setId, facets, replyHandler);
  }

  public <T> void get(AsyncResult<SQLConnection> conn, String table, Class<T> clazz, Criterion filter, boolean returnCount, boolean setId,
      List<FacetField> facets, Handler<AsyncResult<Results<T>>> replyHandler) {

    StringBuilder sb = new StringBuilder();
    StringBuilder fromClauseFromCriteria = new StringBuilder();
    if (filter != null) {
      sb.append(filter.toString());
      fromClauseFromCriteria.append(filter.from2String());
      if (fromClauseFromCriteria.length() > 0) {
        fromClauseFromCriteria.insert(0, COMMA);
      }
    }
    if(conn == null){
      get(table, clazz, DEFAULT_JSONB_FIELD_NAME, fromClauseFromCriteria.toString() + sb.toString(),
        returnCount, true, setId, facets, replyHandler);
    }
    else{
      SQLConnection sqlConnection = conn.result();
      doGet(sqlConnection, true, table, clazz, DEFAULT_JSONB_FIELD_NAME,
        fromClauseFromCriteria.toString() + sb.toString(), returnCount, true, setId, facets, null, replyHandler);
    }
  }

  /**
   * A FunctionalInterface that may throw an Exception.
   *
   * @param <T>  input type
   * @param <R>  output type
   * @param <E>  the type of Exception
   */
  @FunctionalInterface
  public interface FunctionWithException<T, R, E extends Exception> {
    /**
     * @param t  some input
     * @return some output
     * @throws Exception of type E
     */
    R apply(T t) throws E;
  }

  /**
   * Get the jsonb by id.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param function  how to convert the (String encoded) JSON
   * @param replyHandler  the result after applying function
   */
  private <R> void getById(String table, String id,
      FunctionWithException<String, R, Exception> function,
      Handler<AsyncResult<R>> replyHandler) {
    client.getConnection(res -> {
      if (res.failed()) {
        replyHandler.handle(Future.failedFuture(res.cause()));
        return;
      }
      SQLConnection connection = res.result();
      String sql = SELECT + DEFAULT_JSONB_FIELD_NAME
          + FROM + schemaName + DOT + table
          + WHERE + idField + "= ?";
      connection.querySingleWithParams(sql, new JsonArray().add(id), query -> {
        connection.close();
        if (query.failed()) {
          replyHandler.handle(Future.failedFuture(query.cause()));
          return;
        }
        JsonArray result = query.result();
        if (result == null || result.size() == 0) {
          replyHandler.handle(Future.succeededFuture(null));
          return;
        }
        try {
          R r = function.apply(result.getString(0));
          replyHandler.handle(Future.succeededFuture(r));
        } catch (Exception e) {
          replyHandler.handle(Future.failedFuture(e));
        }
      });
    });
  }

  /**
   * Get the jsonb by id and return it as a String.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param replyHandler  the result; the JSON is encoded as a String
   */
  public void getByIdAsString(String table, String id, Handler<AsyncResult<String>> replyHandler) {
    getById(table, id, string -> string, replyHandler);
  }

  /**
   * Get the jsonb by id and return it as a JsonObject.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param replyHandler  the result; the JSON is encoded as a JsonObject
   */
  public void getById(String table, String id, Handler<AsyncResult<JsonObject>> replyHandler) {
    getById(table, id, JsonObject::new, replyHandler);
  }

  /**
   * Get the jsonb by id and return it as a pojo of type T.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param clazz  the type of the pojo
   * @param replyHandler  the result; the JSON is converted into a T pojo.
   */
  public <T> void getById(String table, String id, Class<T> clazz,
      Handler<AsyncResult<T>> replyHandler) {
    getById(table, id, json -> mapper.readValue(json, clazz), replyHandler);
  }

  /**
   * Get jsonb by id for a list of ids.
   * <p>
   * The result is a map of all found records where the key is the id
   * and the value is the jsonb.
   *
   * @param table  the table to search in
   * @param ids  the values of the id field
   * @param function  how to convert the (String encoded) JSON
   * @param replyHandler  the result after applying function
   */
  private <R> void getById(String table, JsonArray ids,
      FunctionWithException<String, R, Exception> function,
      Handler<AsyncResult<Map<String,R>>> replyHandler) {
    if (ids == null || ids.isEmpty()) {
      replyHandler.handle(Future.succeededFuture(Collections.emptyMap()));
      return;
    }
    client.getConnection(res -> {
      if (res.failed()) {
        replyHandler.handle(Future.failedFuture(res.cause()));
        return;
      }

      SQLConnection connection = res.result();
      StringBuilder sql = new StringBuilder()
          .append(SELECT).append(idField).append(", ").append(DEFAULT_JSONB_FIELD_NAME)
          .append(FROM).append(schemaName).append(DOT).append(table)
          .append(WHERE).append(idField).append(" IN (?");
      for (int i=1; i<ids.size(); i++) {
        sql.append(",?");
      }
      sql.append(")");
      connection.queryWithParams(sql.toString(), ids, query -> {
        connection.close();
        if (query.failed()) {
          replyHandler.handle(Future.failedFuture(query.cause()));
          return;
        }
        try {
          ResultSet resultSet = query.result();
          Map<String,R> result = new HashMap<>();
          for (JsonArray jsonArray : resultSet.getResults()) {
            result.put(jsonArray.getString(0), function.apply(jsonArray.getString(1)));
          }
          replyHandler.handle(Future.succeededFuture(result));
        } catch (Exception e) {
          replyHandler.handle(Future.failedFuture(e));
        }
      });
    });
  }

  /**
   * Get the jsonb by id for a list of ids and return each jsonb as a String.
   * @param table  the table to search in
   * @param ids  the values of the id field
   * @param replyHandler  the result; the JSON is encoded as a String
   */
  public void getByIdAsString(String table, JsonArray ids,
      Handler<AsyncResult<Map<String,String>>> replyHandler) {
    getById(table, ids, string -> string, replyHandler);
  }

  /**
   * Get the jsonb by id for a list of ids and return each jsonb as a JsonObject.
   * @param table  the table to search in
   * @param ids  the values of the id field
   * @param replyHandler  the result; the JSON is encoded as a JsonObject
   */
  public void getById(String table, JsonArray ids,
      Handler<AsyncResult<Map<String,JsonObject>>> replyHandler) {
    getById(table, ids, JsonObject::new, replyHandler);
  }

  /**
   * Get the jsonb by id for a list of ids and return each jsonb as pojo of type T.
   * @param table  the table to search in
   * @param ids  the values of the id field
   * @param clazz  the type of the pojo
   * @param replyHandler  the result; the JSON is encoded as a T pojo
   */
  public <T> void getById(String table, JsonArray ids, Class<T> clazz,
      Handler<AsyncResult<Map<String,T>>> replyHandler) {
    getById(table, ids, json -> mapper.readValue(json, clazz), replyHandler);
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
   */
  public <T> void join(JoinBy from, JoinBy to, String operation, String joinType, String cr,
      Class<T> returnedClass, boolean setId,
      Handler<AsyncResult<Results<T>>> replyHandler) {

    Function<TotaledResults, Results<T>> resultSetMapper =
        totaledResults -> processResults(totaledResults.set, totaledResults.total, returnedClass, setId);
    join(from, to, operation, joinType, cr, resultSetMapper, replyHandler);
  }

  public void join(JoinBy from, JoinBy to, String operation, String joinType, String cr,
      Handler<AsyncResult<ResultSet>> replyHandler) {

    Function<TotaledResults, ResultSet> resultSetMapper = totaledResults -> totaledResults.set;
    join(from, to, operation, joinType, cr, resultSetMapper, replyHandler);
  }

  public <T> void join(JoinBy from, JoinBy to, String operation, String joinType, String cr,
      Function<TotaledResults, T> resultSetMapper,
      Handler<AsyncResult<T>> replyHandler) {

    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        try {
          StringBuffer joinon = new StringBuffer();
          StringBuffer tables = new StringBuffer();
          StringBuffer selectFields = new StringBuffer();

          String filter = "";
          if (cr != null) {
            filter = cr;
          }

          String selectFromTable = from.getSelectFields();
          String selectToTable = to.getSelectFields();
          boolean addComma = false;
          if (selectFromTable != null && selectFromTable.length() > 0) {
            selectFields.append(from.getSelectFields());
            addComma = true;
          }
          if (selectToTable != null && selectToTable.length() > 0) {
            if (addComma) {
              selectFields.append(COMMA);
            }
            selectFields.append(to.getSelectFields());
          }

          tables.append(schemaName + DOT + from.getTableName() + SPACE + from.getAlias() + SPACE);

          joinon.append(joinType + SPACE + schemaName + DOT + to.getTableName() + SPACE + to.getAlias() + SPACE);

          Criterion jcr = new Criterion().addCriterion(from.getJoinColumn(), operation, to.getJoinColumn(), AND);

          QueryHelper queryHelper = new QueryHelper(false, null, null);
          queryHelper.selectQuery = SELECT + selectFields.toString() + FROM + tables.toString() + joinon.toString() + jcr + filter;
          queryHelper.countQuery = parseQuery(queryHelper.selectQuery).getCountQuery();

          processQueryWithCount(connection, queryHelper, JOIN_STAT_METHOD, resultSetMapper, replyHandler);
        } catch (Exception e) {
          if (connection != null) {
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

  public <T> void join(JoinBy from, JoinBy to, String operation, String joinType, String cr, Class<T> returnedClass,
      Handler<AsyncResult<Results<T>>> replyHandler){
    join(from, to, operation, joinType, cr, returnedClass, true, replyHandler);
  }

  public void join(JoinBy from, JoinBy to, String operation, String joinType, Criterion cr,
      Handler<AsyncResult<ResultSet>> replyHandler){
    String filter = "";
    if(cr != null){
      filter = cr.toString();
    }
    join(from, to, operation, joinType, filter, replyHandler);
  }

  public void join(JoinBy from, JoinBy to, String operation, String joinType, CQLWrapper cr,
      Handler<AsyncResult<ResultSet>> replyHandler){
    String filter = "";
    if(cr != null){
      filter = cr.toString();
    }
    join(from, to, operation, joinType, filter, replyHandler);
  }

  public <T> void join(JoinBy from, JoinBy to, String operation, String joinType,
      Class<T> returnedClazz, CQLWrapper cr,
      Handler<AsyncResult<Results<T>>> replyHandler){
    String filter = "";
    if(cr != null){
      filter = cr.toString();
    }
    join(from, to, operation, joinType, filter, returnedClazz, true, replyHandler);
  }

  public <T> void join(JoinBy from, JoinBy to, String operation, String joinType,
      Class<T> returnedClazz, CQLWrapper cr, boolean setId,
      Handler<AsyncResult<Results<T>>> replyHandler){
    String filter = "";
    if(cr != null){
      filter = cr.toString();
    }
    join(from, to, operation, joinType, filter, returnedClazz, setId, replyHandler);
  }

  public <T> void join(JoinBy from, JoinBy to, String operation, String joinType,
      Class<T> returnedClazz, String where,
      Handler<AsyncResult<Results<T>>> replyHandler){
    String filter = "";
    if(where != null){
      filter = where;
    }
    join(from, to, operation, joinType, filter, returnedClazz, true, replyHandler);
  }

  static class ResultsHelper<T> {
    final List<T> list;
    final Map<String, org.folio.rest.jaxrs.model.Facet> facets;
    final ResultSet resultSet;
    final List<String> columnNames;
    final Class<T> clazz;
    final boolean setId;
    int total;
    public ResultsHelper(ResultSet resultSet, int total, Class<T> clazz, boolean setId) {
      this.list = new ArrayList<>();
      this.facets = new HashMap<>();
      this.resultSet = resultSet;
      this.columnNames = resultSet.getColumnNames();
      this.clazz= clazz;
      this.setId = setId;
      this.total = total;
    }
    public ResultsHelper(SQLRowStream sqlRowStream, Class<T> clazz, boolean setId) {
      this.list = new ArrayList<>();
      this.facets = new HashMap<>();
      this.resultSet = null;
      this.columnNames = sqlRowStream.columns();
      this.clazz= clazz;
      this.setId = setId;
    }
  }

  /**
   * converts a result set into pojos - handles 3 types of queries:
   * 1. a regular query will return N rows, where each row contains Y columns. one of those columns is the jsonb
   * column which is mapped into a pojo. each row will also contain the count column (if count was requested for
   * the query), other fields , like updated date may also be returned if they were requested in the select.
   *    1a. note that there is an attempt to map external (non jsonb) columns to fields in the pojo. for example,
   *    a column called update_date will attempt to map its value to a field called updateDate in the pojo. however,
   *    for this to happen, the query must select the update_date -> select id,jsonb,update_date from ....
   * 2. a facet query returns 2 columns, a uuid and a jsonb column. the results of the query are returned as
   * id and json rows. facets are returned as jsonb values:
   * {"facetValues": [{"count": 542,"value": "11 ed."}], "type": "name"}
   * (along with a static '00000000-0000-0000-0000-000000000000' uuid)
   * the count for a facet query is returned in the following manner:
   * {"count": 501312} , with a static uuid as the facets
   * 3. audit queries - queries that query an audit table, meaning the clazz parameter passed in has a jsonb member.
   *
   * @param rs
   * @param total
   * @param clazz
   * @param setId
   * @return
   */
  <T> Results<T> processResults(ResultSet rs, Integer total, Class<T> clazz, boolean setId) {
    long start = System.nanoTime();

    if (total == null) {
      // NOTE: this may not be an accurate total, may be better for it to be 0 or null
      total = rs.getNumRows();
    }

    ResultsHelper<T> resultsHelper = new ResultsHelper<>(rs, total, clazz, setId);

    deserializeResults(resultsHelper);

    ResultInfo resultInfo = new ResultInfo();
    resultsHelper.facets.forEach((k , v) -> resultInfo.getFacets().add(v));
    resultInfo.setTotalRecords(resultsHelper.total);

    Results<T> results = new Results<>();
    results.setResults(resultsHelper.list);
    results.setResultInfo(resultInfo);

    statsTracker(PROCESS_RESULTS_STAT_METHOD, clazz.getSimpleName(), start);
    return results;
  }

  /**
   *
   * @param resultsHelper
   */
  <T> void deserializeResults(ResultsHelper<T> resultsHelper) {

    boolean isAuditFlavored = isAuditFlavored(resultsHelper.clazz);

    Map<String, Method> externalColumnSettters = getExternalColumnSetters(
      resultsHelper.columnNames,
      resultsHelper.clazz,
      isAuditFlavored
    );

    String idPropName = databaseFieldToPojoSetter(idField);

    for(JsonObject row : resultsHelper.resultSet.getRows()) {
      try {
        resultsHelper.list.add((T) deserializeRow(resultsHelper, externalColumnSettters, isAuditFlavored, idPropName, row));
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        resultsHelper.list.add(null);
      }
    }
  }

  /**
   *
   * @param resultsHelper
   * @param externalColumnSettters
   * @param isAuditFlavored
   * @param idPropName
   * @param row
   */
  <T> Object deserializeRow(
    ResultsHelper<T> resultsHelper, Map<String, Method> externalColumnSettters,
    boolean isAuditFlavored, String idPropName, JsonObject row
  ) throws IOException, InstantiationException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

    Object jo = row.getValue(DEFAULT_JSONB_FIELD_NAME);
    Object id = row.getValue(idField);

    Object o = null;

    if (!isAuditFlavored && jo != null) {
      boolean finished = false;
      try {
        // is this a facet entry - if so process it, otherwise will throw an exception
        // and continue trying to map to the pojos
        o =  mapper.readValue(jo.toString(), org.folio.rest.jaxrs.model.Facet.class);
        org.folio.rest.jaxrs.model.Facet of = (org.folio.rest.jaxrs.model.Facet) o;
        org.folio.rest.jaxrs.model.Facet facet = resultsHelper.facets.get(of.getType());
        if (facet == null) {
          resultsHelper.facets.put(of.getType(), of);
        } else {
          facet.getFacetValues().add(of.getFacetValues().get(0));
        }
        finished = true;
      } catch (Exception e) {
        try {
          o = mapper.readValue(jo.toString(), resultsHelper.clazz);
        } catch (UnrecognizedPropertyException upe) {
          // this is a facet query , and this is the count entry {"count": 11}
          resultsHelper.total = new JsonObject(row.getString(DEFAULT_JSONB_FIELD_NAME)).getInteger(COUNT_FIELD);
          finished = true;
        }
      }
      if (finished) {
        return o;
      }
    } else {
      o = resultsHelper.clazz.newInstance();
    }

    populateExternalColumns(externalColumnSettters, o, row);

    if (resultsHelper.setId) {
      o.getClass().getMethod(idPropName, String.class).invoke(o, id.toString());
    }

    return o;
  }

  /**
   * an exception to having the jsonb column and the fields within the json
   * get mapped to the corresponding clazz is a case where the
   * clazz has a jsonb field (member), for example an audit class which contains a field called
   * jsonb - meaning it encapsulates the real object for example for auditing purposes
   * (contains the jsonb object as well as some other fields). In such a
   * case, do not map the clazz to the content of the jsonb - but rather set the jsonb named field of the clazz
   * with the jsonb column value
   *
   * @param clazz
   * @return
   */
  <T> boolean isAuditFlavored(Class<T> clazz) {
    boolean isAuditFlavored = false;
    try {
      clazz.getDeclaredField(DEFAULT_JSONB_FIELD_NAME);
      isAuditFlavored = true;
    } catch (NoSuchFieldException nse) {
      if (log.isDebugEnabled()) {
        log.debug("non audit table, no " + DEFAULT_JSONB_FIELD_NAME + " found in json");
      }
    }
    return isAuditFlavored;
  }

  /**
   * get the class methods in order to populate jsonb object from external columns
   * abiding to audit mode
   *
   * @param columnNames
   * @param clazz
   * @param isAuditFlavored
   * @return
   */
  <T> Map<String, Method> getExternalColumnSetters(List<String> columnNames, Class<T> clazz, boolean isAuditFlavored) {
    Map<String, Method> externalColumnSettters = new HashMap<>();
    for (String columnName : columnNames) {
      if ((isAuditFlavored || !columnName.equals(DEFAULT_JSONB_FIELD_NAME)) && !columnName.equals(idField)) {
        String methodName = databaseFieldToPojoSetter(columnName);
        for (Method method : clazz.getMethods()) {
          if (method.getName().equals(methodName)) {
            externalColumnSettters.put(columnName, method);
          }
        }
      }
    }
    return externalColumnSettters;
  }

  /**
   * populate jsonb object with values from external columns - for example:
   * if there is an update_date column in the record - try to populate a field updateDate in the
   * jsonb object - this allows to use the DB for things like triggers to populate the update_date
   * automatically, but still push them into the jsonb object - the json schema must declare this field
   * as well - also support the audit mode descrbed above.
   * NOTE: that the query must request any field it wants to get populated into the jsonb obj
   *
   * @param externalColumnSettters
   * @param o
   * @param row
   */
  void populateExternalColumns(Map<String, Method> externalColumnSettters, Object o, JsonObject row) {
    for (Map.Entry<String, Method> entry : externalColumnSettters.entrySet()) {
      String columnName = entry.getKey();
      Method method = entry.getValue();
      try {
        Object value = row.getValue(columnName);
        if (value instanceof JsonArray) {
           method.invoke(o, Json.decodeValue(((JsonArray) value).encode(), method.getParameterTypes()[0]));
        } else {
          method.invoke(o, value);
        }
      } catch (Exception e) {
        log.warn("Unable to populate field " + columnName + " for object of type " + o.getClass().getName());
      }
    }
  }

  /**
   * assumes column names are all lower case with multi word column names
   * separated by an '_'
   * @param str
   * @return
   */
  String databaseFieldToPojoSetter(String str) {
    StringBuilder sb = new StringBuilder(str);
    sb.replace(0, 1, String.valueOf(Character.toUpperCase(sb.charAt(0))));
    for (int i = 0; i < sb.length(); i++) {
        if (sb.charAt(i) == '_') {
            sb.deleteCharAt(i);
            sb.replace(i, i + 1, String.valueOf(Character.toUpperCase(sb.charAt(i))));
        }
    }
    return "set" + sb.toString();
  }

  /**
   * Run a select query.
   *
   * <p>To update see {@link #execute(String, Handler)}.
   *
   * @param sql - the sql query to run
   * @param replyHandler  the query result or the failure
   */
  public void select(String sql, Handler<AsyncResult<ResultSet>> replyHandler) {
    client.getConnection(conn -> select(conn, sql, closeAndHandleResult(conn, replyHandler)));
  }

  /**
   * Run a select query.
   *
   * <p>This never closes the connection conn.
   *
   * <p>To update see {@link #execute(AsyncResult, String, Handler)}.
   *
   * @param conn  The connection on which to execute the query on.
   * @param sql  The sql query to run.
   * @param replyHandler  The query result or the failure.
   */
  public void select(AsyncResult<SQLConnection> conn, String sql, Handler<AsyncResult<ResultSet>> replyHandler) {
    try {
      if (conn.failed()) {
        replyHandler.handle(Future.failedFuture(conn.cause()));
        return;
      }
      conn.result().query(sql, replyHandler);
    } catch (Exception e) {
      log.error("select sql: " + e.getMessage() + " - " + sql, e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Run a parameterized/prepared select query.
   *
   * <p>To update see {@link #execute(String, JsonArray, Handler)}.
   *
   * @param conn  The connection on which to execute the query on.
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   */
  public void select(String sql, JsonArray params, Handler<AsyncResult<ResultSet>> replyHandler) {
    client.getConnection(conn -> select(conn, sql, params, closeAndHandleResult(conn, replyHandler)));
  }

  /**
   * Run a parameterized/prepared select query.
   *
   * <p>This never closes the connection conn.
   *
   * <p>To update see {@link #execute(AsyncResult, String, JsonArray, Handler)}.
   *
   * @param conn  The connection on which to execute the query on.
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   */
  public void select(AsyncResult<SQLConnection> conn, String sql, JsonArray params,
      Handler<AsyncResult<ResultSet>> replyHandler) {
    try {
      if (conn.failed()) {
        replyHandler.handle(Future.failedFuture(conn.cause()));
        return;
      }
      conn.result().queryWithParams(sql, params, replyHandler);
    } catch (Exception e) {
      log.error("select sql: " + e.getMessage() + " - " + sql, e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Run a select query and return the first record, or null if there is no result.
   *
   * <p>To update see {@link #execute(String, Handler)}.
   *
   * @param sql  The sql query to run.
   * @param replyHandler  The query result or the failure.
   */
  public void selectSingle(String sql, Handler<AsyncResult<JsonArray>> replyHandler) {
    client.getConnection(conn -> selectSingle(conn, sql, closeAndHandleResult(conn, replyHandler)));
  }

  /**
   * Run a select query and return the first record, or null if there is no result.
   *
   * <p>This never closes the connection conn.
   *
   * <p>To update see {@link #execute(AsyncResult, String, Handler)}.
   *
   * @param conn  The connection on which to execute the query on.
   * @param sql  The sql query to run.
   * @param replyHandler  The query result or the failure.
   */
  public void selectSingle(AsyncResult<SQLConnection> conn, String sql, Handler<AsyncResult<JsonArray>> replyHandler) {
    try {
      if (conn.failed()) {
        replyHandler.handle(Future.failedFuture(conn.cause()));
        return;
      }
      conn.result().querySingle(sql, replyHandler);
    } catch (Exception e) {
      log.error("select single sql: " + e.getMessage() + " - " + sql, e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Run a parameterized/prepared select query and return the first record, or null if there is no result.
   *
   * <p>To update see {@link #execute(String, Handler)}.
   *
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   */
  public void selectSingle(String sql, JsonArray params, Handler<AsyncResult<JsonArray>> replyHandler) {
    client.getConnection(conn -> selectSingle(conn, sql, params, closeAndHandleResult(conn, replyHandler)));
  }

  /**
   * Run a parameterized/prepared select query and return the first record, or null if there is no result.
   *
   * <p>This never closes the connection conn.
   *
   * <p>To update see {@link #execute(AsyncResult, String, Handler)}.
   *
   * @param conn  The connection on which to execute the query on.
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   */
  public void selectSingle(AsyncResult<SQLConnection> conn, String sql, JsonArray params,
      Handler<AsyncResult<JsonArray>> replyHandler) {
    try {
      if (conn.failed()) {
        replyHandler.handle(Future.failedFuture(conn.cause()));
        return;
      }
      conn.result().querySingleWithParams(sql, params, replyHandler);
    } catch (Exception e) {
      log.error("select single sql: " + e.getMessage() + " - " + sql, e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Run a parameterized/prepared select query returning with an SQLRowStream.
   *
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   */
  public void selectStream(String sql, Handler<AsyncResult<SQLRowStream>> replyHandler) {
    client.getConnection(conn -> selectStream(conn, sql, closeAndHandleResult(conn, replyHandler)));
  }

  /**
   * Run a parameterized/prepared select query returning with an SQLRowStream.
   *
   * <p>This never closes the connection conn.
   *
   * @param conn  The connection on which to execute the query on.
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   */
  public void selectStream(AsyncResult<SQLConnection> conn, String sql,
      Handler<AsyncResult<SQLRowStream>> replyHandler) {
    try {
      if (conn.failed()) {
        replyHandler.handle(Future.failedFuture(conn.cause()));
        return;
      }
      conn.result().queryStream(sql, replyHandler);
    } catch (Exception e) {
      log.error("select stream sql: " + e.getMessage() + " - " + sql, e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Run a parameterized/prepared select query returning with an SQLRowStream.
   *
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   */
  public void selectStream(String sql, JsonArray params, Handler<AsyncResult<SQLRowStream>> replyHandler) {
    client.getConnection(conn -> selectStream(conn, sql, params, closeAndHandleResult(conn, replyHandler)));
  }

  /**
   * Run a parameterized/prepared select query returning with an SQLRowStream.
   *
   * <p>This never closes the connection conn.
   *
   * @param conn  The connection on which to execute the query on.
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   */
  public void selectStream(AsyncResult<SQLConnection> conn, String sql, JsonArray params,
      Handler<AsyncResult<SQLRowStream>> replyHandler) {
    try {
      if (conn.failed()) {
        replyHandler.handle(Future.failedFuture(conn.cause()));
        return;
      }
      conn.result().queryStreamWithParams(sql, params, replyHandler);
    } catch (Exception e) {
      log.error("select stream sql: " + e.getMessage() + " - " + sql, e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Execute an INSERT, UPDATE or DELETE statement.
   * @param sql - the sql to run
   * @param replyHandler - the result handler with UpdateResult converted toString().
   * @deprecated use {@link #execute(String, Handler)} instead.
   */
  @Deprecated
  public void mutate(String sql, Handler<AsyncResult<String>> replyHandler) {
    execute(sql, res -> {
      if (res.failed()) {
        replyHandler.handle(Future.failedFuture(res.cause()));
        return;
      }
      replyHandler.handle(Future.succeededFuture(res.result().toString()));
    });
  }

  /**
   * Execute an INSERT, UPDATE or DELETE statement.
   * @param sql - the sql to run
   * @param replyHandler - the result handler with UpdateResult
   */
  public void execute(String sql, Handler<AsyncResult<UpdateResult>> replyHandler)  {
    long s = System.nanoTime();
    client.getConnection(res -> {
      if (res.failed()) {
        replyHandler.handle(Future.failedFuture(res.cause()));
        return;
      }
      SQLConnection connection = res.result();
      try {
        connection.update(sql, query -> {
          connection.close();
          if (query.failed()) {
            replyHandler.handle(Future.failedFuture(query.cause()));
          } else {
            replyHandler.handle(Future.succeededFuture(query.result()));
          }
          logTimer("execute", sql, s);
        });
      } catch (Exception e) {
        if (connection != null) {
          connection.close();
        }
        log.error(e.getMessage(), e);
        replyHandler.handle(Future.failedFuture(e));
      }
    });
  }

  /**
   * Execute a parameterized/prepared INSERT, UPDATE or DELETE statement.
   * @param sql  The SQL statement to run.
   * @param params The parameters for the placeholders in sql.
   * @param replyHandler
   */
  public void execute(String sql, JsonArray params, Handler<AsyncResult<UpdateResult>> replyHandler)  {
    long s = System.nanoTime();
    client.getConnection(res -> {
      if (res.failed()) {
        replyHandler.handle(Future.failedFuture(res.cause()));
        return;
      }
      SQLConnection connection = res.result();
      try {
        connection.updateWithParams(sql, params, query -> {
          connection.close();
          if (query.failed()) {
            replyHandler.handle(Future.failedFuture(query.cause()));
          } else {
            replyHandler.handle(Future.succeededFuture(query.result()));
          }
          logTimer("executeWithParams", sql, s);
        });
      } catch (Exception e) {
        if (connection != null) {
          connection.close();
        }
        log.error(e.getMessage(), e);
        replyHandler.handle(Future.failedFuture(e));
      }
    });
  }

  /**
   * send a query to update within a transaction
   *
   * <p>Example:
   * <pre>
   *  postgresClient.startTx(beginTx -> {
   *        try {
   *          postgresClient.mutate(beginTx, sql, reply -> {...
   * </pre>
   * @param conn - connection - see {@link #startTx(Handler)}
   * @param sql - the sql to run
   * @param replyHandler
   * @deprecated use execute(AsyncResult<SQLConnection>, String, Handler<AsyncResult<UpdateResult>>) instead
   */
  @Deprecated
  public void mutate(AsyncResult<SQLConnection> conn, String sql, Handler<AsyncResult<String>> replyHandler){
    try {
      SQLConnection sqlConnection = conn.result();
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
   * Send an INSERT, UPDATE or DELETE statement within a transaction.
   *
   * <p>Example:
   * <pre>
   *  postgresClient.startTx(beginTx -> {
   *        try {
   *          postgresClient.execute(beginTx, sql, reply -> {...
   * </pre>
   * @param conn - connection - see {@link #startTx(Handler)}
   * @param sql - the sql to run
   * @param replyHandler - reply handler with UpdateResult
   */
  public void execute(AsyncResult<SQLConnection> conn, String sql, Handler<AsyncResult<UpdateResult>> replyHandler){
    try {
      SQLConnection sqlConnection = conn.result();
      sqlConnection.update(sql, query -> {
        if (query.failed()) {
          replyHandler.handle(Future.failedFuture(query.cause()));
        } else {
          replyHandler.handle(Future.succeededFuture(query.result()));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Send an INSERT, UPDATE or DELETE parameterized/prepared statement within a transaction.
   *
   * <p>Example:
   * <pre>
   *  postgresClient.startTx(beginTx -> {
   *        try {
   *          postgresClient.execute(beginTx, sql, params, reply -> {...
   * </pre>
   * @param conn - connection - see {@link #startTx(Handler)}
   * @param sql - the sql to run
   * @param replyHandler - reply handler with UpdateResult
   */
  public void execute(AsyncResult<SQLConnection> conn, String sql, JsonArray params,
      Handler<AsyncResult<UpdateResult>> replyHandler){
    try {
      SQLConnection sqlConnection = conn.result();
      sqlConnection.updateWithParams(sql, params, query -> {
        if (query.failed()) {
          replyHandler.handle(Future.failedFuture(query.cause()));
        } else {
          replyHandler.handle(Future.succeededFuture(query.result()));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Create a parameterized/prepared INSERT, UPDATE or DELETE statement and
   * run it with a list of sets of parameters.
   *
   * <p>Example:
   * <pre>
   *  postgresClient.startTx(beginTx -> {
   *        try {
   *          postgresClient.execute(beginTx, sql, params, reply -> {...
   * </pre>
   * @param conn - connection - see {@link #startTx(Handler)}
   * @param sql - the sql to run
   * @param params - there is one list entry for each sql invocation containing the parameters for the placeholders.
   * @param replyHandler - reply handler with one UpdateResult for each list entry of params.
   */
  public void execute(AsyncResult<SQLConnection> conn, String sql, List<JsonArray> params,
      Handler<AsyncResult<List<UpdateResult>>> replyHandler) {
    try {
      SQLConnection sqlConnection = conn.result();
      List<UpdateResult> results = new ArrayList<>(params.size());
      Iterator<JsonArray> iterator = params.iterator();
      Runnable task = new Runnable() {
        @Override
        public void run() {
          if (! iterator.hasNext()) {
            replyHandler.handle(Future.succeededFuture(results));
            return;
          }
          sqlConnection.updateWithParams(sql, iterator.next(), query -> {
            if (query.failed()) {
              replyHandler.handle(Future.failedFuture(query.cause()));
              return;
            }
            results.add(query.result());
            this.run();
          });
        }
      };
      task.run();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Create a parameterized/prepared INSERT, UPDATE or DELETE statement and
   * run it with a list of sets of parameters. Wrap all in a transaction.
   *
   * @param sql - the sql to run
   * @param params - there is one list entry for each sql invocation containing the parameters for the placeholders.
   * @param replyHandler - reply handler with one UpdateResult for each list entry of params.
   */
  public void execute(String sql, List<JsonArray> params, Handler<AsyncResult<List<UpdateResult>>> replyHandler) {
    startTx(transaction -> {
      if (transaction.failed()) {
        replyHandler.handle(Future.failedFuture(transaction.cause()));
        return;
      }
      execute(transaction, sql, params, result -> {
        if (result.failed()) {
          rollbackTx(transaction, rollback -> replyHandler.handle(result));
          return;
        }
        endTx(transaction, end -> {
          if (end.failed()) {
            replyHandler.handle(Future.failedFuture(end.cause()));
            return;
          }
          replyHandler.handle(result);
        });
      });
    });
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
    String q = "SELECT * FROM " + schemaName + DOT + tableName + SPACE + where;
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
    String q = "SELECT * FROM " + schemaName + DOT + tableName + SPACE + where;
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
              + schemaName + DOT + cacheName +" AS " + sql2cache;
          log.info(q);
          connection.update(q,
            query -> {
            connection.close();
            if (query.failed()) {
              replyHandler.handle(Future.failedFuture(query.cause()));
            } else {
              replyHandler.handle(Future.succeededFuture(query.result().getUpdated()));
            }
            statsTracker("persistentlyCacheResult", "CREATE TABLE AS", start);
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
          connection.update("DROP TABLE " + schemaName + DOT + cacheName, query -> {
            connection.close();
            if (query.failed()) {
              replyHandler.handle(Future.failedFuture(query.cause()));
            } else {
              replyHandler.handle(Future.succeededFuture(query.result().getUpdated()));
            }
            statsTracker("removePersistentCacheResult", "DROP TABLE " + cacheName, start);
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
          singleStatement.append(SPACE + allLines[i]);
          if(!allLines[i].trim().endsWith(SEMI_COLON)){
            int j=0;
            if(i+1<allLines.length){
              for (j = i+1; j < allLines.length; j++) {
                if(allLines[j].trim().toUpperCase().trim().matches(CLOSE_FUNCTION_POSTGRES)){
                  singleStatement.append(SPACE + allLines[j]);
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
        else if(allLines[i].trim().endsWith(SEMI_COLON) && !inFunction && !inCopy){
          execStatements.add( singleStatement.append(SPACE + allLines[i]).toString() );
          singleStatement = new StringBuilder();
        }
        else {
          if(inCopy)  {
            singleStatement.append("\n");
          }
          else{
            singleStatement.append(SPACE);
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
      replyHandler.handle(Future.failedFuture(e));
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
    CopyManager copyManager = new CopyManager((BaseConnection) connection);
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
          log.error("Commit failed " + Arrays.hashCode(sql) + SPACE + e.getMessage(), e);
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
      logTimer("execute", "" + Arrays.hashCode(sql), s);
      replyHandler.handle(Future.succeededFuture(results));
    });
  }

  private static void rememberEmbeddedPostgres() {
     embeddedPostgres = new EmbeddedPostgres(Version.Main.V10);
  }

  /**
   * Start an embedded PostgreSQL using the configuration of {@link #getConnectionConfig()}.
   * doesn't change the configuration.
   *
   * @throws IOException  when starting embedded PostgreSQL fails
   */
  public void startEmbeddedPostgres() throws IOException {
    // starting Postgres
    setIsEmbedded(true);
    if (embeddedPostgres == null) {
      int port = postgreSQLClientConfig.getInteger(PORT);
      String username = postgreSQLClientConfig.getString(_USERNAME);
      String password = postgreSQLClientConfig.getString(_PASSWORD);
      String database = postgreSQLClientConfig.getString(DATABASE);

      String locale = "en_US.UTF-8";
      String OS = System.getProperty("os.name").toLowerCase();
      if (OS.indexOf("win") >= 0) {
        locale = "american_usa";
      }
      rememberEmbeddedPostgres();
      embeddedPostgres.start("localhost", port, database, username, password,
        Arrays.asList("-E", "UTF-8", "--locale", locale));
      log.info("embedded postgres started on port " + port);
    } else {
      log.info("embedded postgres is already running...");
    }
  }

  /**
   * .sql files
   * @param path
   */
  public void importFileEmbedded(String path) {
    // starting Postgres
    if (embeddedMode) {
      if (embeddedPostgres != null) {
        Optional<PostgresProcess> optionalPostgresProcess = embeddedPostgres.getProcess();
        if (optionalPostgresProcess.isPresent()) {
          log.info("embedded postgress import starting....");
          PostgresProcess postgresProcess = optionalPostgresProcess.get();
          postgresProcess.importFromFile(new File(path));
          log.info("embedded postgress import complete....");
        } else {
          log.warn("embedded postgress is not running...");
        }
      } else {
        log.info("embedded postgress not enabled");
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
    if (embeddedPostgres != null) {
      closeAllClients();
      LogUtil.formatLogMessage(PostgresClient.class.getName(), "stopEmbeddedPostgres", "called stop on embedded postgress ...");
      embeddedPostgres.stop();
      embeddedPostgres = null;
      embeddedMode = false;
    }
  }

  public static String convertToPsqlStandard(String tenantId){
    return tenantId.toLowerCase() + "_" + MODULE_NAME;
  }

  public static String getModuleName(){
    return MODULE_NAME;
  }

  /**
   * @return the tenantId of this PostgresClient
   */
  String getTenantId() {
    return tenantId;
  }

  /**
   * @return the PostgreSQL schema name for the tenantId and the module name of this PostgresClient.
   *   A PostgreSQL schema name is of the form tenant_module and is used to address tables:
   *   "SELECT * FROM tenant_module.table"
   */
  String getSchemaName() {
    return schemaName;
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
    String notTrue = " IS NOT TRUE";
    String notTrueReplacement = " AND \\(\\(\\(FALSE\\)\\)\\)";
    String isTrue = " IS TRUE";
    String isTrueReplacement = " AND \\(\\(\\(TRUE\\)\\)\\)";

    String countOn = "*";
    List<OrderByElement> orderBy = null;
    net.sf.jsqlparser.statement.select.Limit limit = null;
    Expression where = null;
    net.sf.jsqlparser.statement.select.Offset offset = null;
    long start = System.nanoTime();
    String queryWithoutLimitOffset = "";
    try {
      try {
        //TEMPORARY HACK SINCE PARSER CANT HANDLE "IS NOT TRUE" , so replace it with IS NOT NULL
        //parse, and then below return the "IS NOT TRUE" - this is buggy as if this appears for some
        //strange reason outside a where clause this will fail
        query = query.replaceAll(notTrue, notTrueReplacement).replaceAll(isTrue, isTrueReplacement);
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(query);
        Select selectStatement = (Select) statement;

        Matcher distinctOnMatcher = DISTINCT_ON_MATCH_PATTERN.matcher(query);
        if(distinctOnMatcher.find() && distinctOnMatcher.groupCount() >= 3) {
          countOn = String.format("%s (%s)", distinctOnMatcher.group(2), distinctOnMatcher.group(3));
        }
        orderBy = ((PlainSelect) selectStatement.getSelectBody()).getOrderByElements();
        limit = ((PlainSelect) selectStatement.getSelectBody()).getLimit();
        offset = ((PlainSelect) selectStatement.getSelectBody()).getOffset();
        where = ((PlainSelect) selectStatement.getSelectBody()).getWhere();
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }

      //TEMPORARY HACK - see above - back to original query after parsing completes
      query = query.replaceAll(notTrueReplacement, notTrue).replaceAll(isTrueReplacement, isTrue);
      int startOfLimit = NaiveSQLParse.getLastStartPos(query, "limit");
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

      int startOfOffset = NaiveSQLParse.getLastStartPos(query, "offset");
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
      int startOfOrderBy = NaiveSQLParse.getLastStartPos(query, "order by");
      if(orderBy != null){
        StringBuilder sb = new StringBuilder("order by[ ]+");
        int size = orderBy.size();
        for (int i = 0; i < size; i++) {
          sb.append(orderBy.get(i).toString().replaceAll(SPACE, "[ ]+"));
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

   pq.setCountQuery(query.replaceFirst(COLUMN_CONTROL_REGEX, String.format(COUNT, countOn)).trim());

   pq.setQueryWithoutLimOff(queryWithoutLimitOffset);
   if(where != null){
     //TEMPORARY HACK see above
     pq.setWhereClause( where.toString().replaceAll(notTrueReplacement, notTrue)
       .replaceAll(isTrueReplacement, isTrue) );
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

}
