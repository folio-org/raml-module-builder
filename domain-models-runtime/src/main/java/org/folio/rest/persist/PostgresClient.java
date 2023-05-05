package org.folio.rest.persist;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import freemarker.template.TemplateException;
import io.netty.handler.ssl.OpenSsl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JdkSSLEngineOptions;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.SslMode;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;

import java.io.IOException;
import java.lang.StackWalker.Option;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.SecretKey;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.jaxrs.model.ResultInfo;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.UpdateSection;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.facets.FacetField;
import org.folio.rest.persist.facets.FacetManager;
import org.folio.rest.persist.interfaces.Results;
import org.folio.rest.security.AES;
import org.folio.rest.tools.utils.Envs;
import org.folio.rest.tools.utils.MetadataUtil;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.util.PostgresTester;
import org.folio.util.PostgresTesterStartException;

/**
 * @author shale
 *
 * currently does not support binary data unless base64 encoded
 */
public class PostgresClient {

  public static final String     DEFAULT_SCHEMA           = "public";
  public static final String     DEFAULT_JSONB_FIELD_NAME = "jsonb";

  static Logger log = LogManager.getLogger(PostgresClient.class);

  /** default analyze threshold value in milliseconds */
  static final long              EXPLAIN_QUERY_THRESHOLD_DEFAULT = 1000;

  static final String            COUNT_FIELD = "count";

  static final int               STREAM_GET_DEFAULT_CHUNK_SIZE = 100;
  static final ObjectMapper      MAPPER                   = ObjectMapperTool.getMapper();

  /**
   * True if all tenants of a Vertx share one PgPool, false for having a separate PgPool for
   * each combination of tenant and Vertx (= each PostgresClient has its own PgPool).
   *
   * <p>sharedPgPool is set to true if the {@code DB_MAXSHAREDPOOLSIZE} environment variable is set.
   *
   * @see #PG_POOLS
   */
  static boolean sharedPgPool = false;

  private static final String    MODULE_NAME              = getModuleName("org.folio.rest.tools.utils.ModuleName");
  private static final String    ID_FIELD                 = "id";

  private static final String    CONNECTION_RELEASE_DELAY = "connectionReleaseDelay";
  private static final String    MAX_POOL_SIZE = "maxPoolSize";
  private static final String    MAX_SHARED_POOL_SIZE = "maxSharedPoolSize";
  /** default release delay in milliseconds; after this time an idle database connection is closed */
  private static final int       DEFAULT_CONNECTION_RELEASE_DELAY = 60000;
  private static final String    POSTGRES_LOCALHOST_CONFIG = "/postgres-conf.json";

  private static PostgresTester postgresTester;

  private static final String    SELECT = "SELECT ";
  private static final String    FROM   = " FROM ";
  private static final String    WHERE  = " WHERE ";

  @SuppressWarnings("java:S2068")  // suppress "Hard-coded credentials are security-sensitive"
  // we use it as a key in the config. We use it as a default password only when testing
  // using embedded postgres, see getPostgreSQLClientConfig
  private static final String    PASSWORD = "password";
  private static final String    USERNAME = "username";
  private static final String    HOST      = "host";
  private static final String    HOST_READER = "host_reader";
  private static final String    PORT      = "port";
  private static final String    PORT_READER = "port_reader";
  private static final String    DATABASE  = "database";
  private static final String    RECONNECT_ATTEMPTS = "reconnectAttempts";
  private static final String    RECONNECT_INTERVAL = "reconnectInterval";
  private static final String    SERVER_PEM = "server_pem";
  private static final String    POSTGRES_TESTER = "postgres_tester";

  private static final String    GET_STAT_METHOD = "get";
  private static final String    EXECUTE_STAT_METHOD = "execute";

  private static final String    PROCESS_RESULTS_STAT_METHOD = "processResults";

  private static final String    SPACE = " ";
  private static final String    DOT = ".";
  private static final String    COMMA = ",";
  private static final String    SEMI_COLON = ";";

  private static String          configPath               = null;

  /**
   * Used only if {@link #sharedPgPool} is true.
   */
  private static final Map<Vertx,PgPool> PG_POOLS = new HashMap<>();

  /**
   * Used only if {@link #sharedPgPool} is true.
   */
  private static final Map<Vertx,PgPool> PG_POOLS_READER = new HashMap<>();

  /** map (Vertx, String tenantId) to PostgresClient */
  private static final MultiKeyMap<Object, PostgresClient> CONNECTION_POOL =
      MultiKeyMap.multiKeyMap(new HashedMap<>());

  @SuppressWarnings("squid:S5852") // "Using slow regular expression is security sensitive"
  // This works on static SQL provided by the developer, not on runtime provided SQL, therefore it is safe.
  private static final Pattern POSTGRES_DOLLAR_QUOTING =
      // \\B = a non-word boundary, the first $ must not be part of an identifier (foo$bar$baz)
      Pattern.compile("[^\\n\\r]*?\\B(\\$\\w*\\$).*?\\1[^\\n\\r]*", Pattern.DOTALL);
  private static final Pattern POSTGRES_COPY_FROM_STDIN =
      // \\b = a word boundary
      Pattern.compile("^\\s*COPY\\b.*\\bFROM\\s+STDIN\\b.*", Pattern.CASE_INSENSITIVE);

  /** analyze threshold value in milliseconds */
  private static long explainQueryThreshold = EXPLAIN_QUERY_THRESHOLD_DEFAULT;

  private final Vertx vertx;
  private JsonObject postgreSQLClientConfig = null;
  /**
   * PgPool client that is initialized with mainly the database writer instance's connection string.
   */
  private PgPool client;
  /**
   * PgPool client that is initialized with mainly the database reader instance's connection string.
   * When there is no reader instance, then this client should be initialized with the writer's connection string
   */
  private PgPool readClient;
  private final String tenantId;
  private final String schemaName;

  protected PostgresClient(Vertx vertx, String tenantId) throws Exception {
    this.tenantId = tenantId;
    this.vertx = vertx;
    this.schemaName = convertToPsqlStandard(tenantId);
    init();
  }


  /**
   * test constructor for unit testing
   */
  private PostgresClient() {
    this.tenantId = "test";
    this.vertx = null;
    this.schemaName = convertToPsqlStandard(tenantId);
    log.warn("Instantiating test Postgres client! Only use with tests!");
  }

  static PostgresClient testClient() {
    explainQueryThreshold = 0;
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
    if (log.isDebugEnabled()) {
      long endNanoTime = System.nanoTime();
      logTimer(descriptionKey, sql, startNanoTime, endNanoTime);
    }
  }

  /**
   * Set PostgresTester instance to use for testing.
   *
   * <p>If database configuration is already provided (DB_* env variables or JSON config file),
   * this call is ignored and testing is performed against the database instance given by configuration.
   *
   * <p>Setting the same or a different PostgresTester instance invokes the close method of the
   * old PostgresTester instance.
   *
   * <p>See {@link org.folio.postgres.testing.PostgresTesterContainer#PostgresTesterContainer()}
   *
   * @param tester instance to use for testing.
   */
  public static void setPostgresTester(PostgresTester tester) {
    stopPostgresTester();
    postgresTester = tester;
  }

  /**
   * True if embedded specific defaults for the
   * PostgreSQL configuration should be used if there is no
   * postgres json config file.
   * @return true for using embedded specific defaults
   */
  public static boolean isEmbedded(){
    return postgresTester != null;
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
    if (configPath == null){
      configPath = POSTGRES_LOCALHOST_CONFIG;
    }
    return configPath;
  }

  static void setExplainQueryThreshold(long ms) {
    explainQueryThreshold = ms;
  }

  static Long getExplainQueryThreshold() {
    return explainQueryThreshold;
  }

  /**
   * Instance for the tenantId from CONNECTION_POOL, or created and
   * added to CONNECTION_POOL.
   * @param vertx the Vertx to use
   * @param tenantId the tenantId the instance is for
   * @return the PostgresClient instance, or null on error
   */
  private static PostgresClient getInstanceInternal(Vertx vertx, String tenantId) {
    // assumes a single thread vertx model so no sync needed
    PostgresClient postgresClient = CONNECTION_POOL.get(vertx, tenantId);
    try {
      if (postgresClient == null) {
        postgresClient = new PostgresClient(vertx, tenantId);
        CONNECTION_POOL.put(vertx, tenantId, postgresClient);
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
  static String decodePassword(String password) throws Exception {
    String key = AES.getSecretKey();
    if (key != null){
      SecretKey sk = AES.getSecretKeyObject(key);
      return AES.decryptPassword(password, sk);
    }
    /* no key , so nothing to decode */
    return password;
  }

  /** this function is intended to receive the tenant id as a password
   * encrypt the tenant id with the secret key and use the encrypted
   * password as the actual password for the tenant user in the DB.
   * In order to then know the password - you need to take the tenant id
   * and encrypt it with the secret key and then you have the tenant's password */
  static String createPassword(String password) throws Exception {
    String key = AES.getSecretKey();
    if (key != null){
      SecretKey sk = AES.getSecretKeyObject(key);
      return AES.encryptPasswordAsBase64(password, sk);
    }
    /* no key , so nothing to encrypt, the password will be the tenant id */
    return password;
  }

  /**
   * This instance's Reader PgPool for database connections which is instantiated with the database reader
   * connections string, whereas getClient() returns the PgPool instance which is instantiated with the database writer
   * connection string that would do non-reading operations.
   *
   * Take care to execute "SET ROLE ...; SET SCHEMA ..." when {@link #sharedPgPool} is true; consider using
   * one of the with... methods that automatically execute "SET ROLE ...; SET SCHEMA ...".
   *
   * @see #withReadConnection(Function)
   * @see #withReadConn(Function)
   * @see #withTrans(Function)
   * @see #withTransaction(Function)
   * @see #getClient()
   */
  PgPool getReaderClient() {
    return readClient;
  }

  /**
   * Set this instance's Reader PgPool that can connect to Postgres.
   * @param readClient  the new client
   */
  void setReaderClient(PgPool readClient) {
    this.readClient = readClient;
  }

  /**
   * This instance's PgPool for database connections. It is instantiated with the "write" db instance's
   * connection string and is responsible for executing all the non-read queries (upsert & delete)
   *
   * Take care to execute "SET ROLE ...; SET SCHEMA ..." when {@link #sharedPgPool} is true; consider using
   * one of the with... methods that automatically execute "SET ROLE ...; SET SCHEMA ...".
   *
   * @see #withConnection(Function)
   * @see #withConn(Function)
   * @see #withTrans(Function)
   * @see #withTransaction(Function)
   * @see #getReaderClient()
   */
  PgPool getClient() {
    return client;
  }

  /**
   * Set this instance's PgPool that can connect to Postgres.
   * @param client  the new client
   */
  void setClient(PgPool client) {
    this.client = client;
  }

  /**
   * Close the SQL client of this PostgresClient instance.
   * This is idempotent: additional close invocations are always successful.
   */
  public Future<Void> closeClient() {
    if (client == null) {
      return Future.succeededFuture();
    }
    PgPool clientToClose = client;
    PgPool readClientToClose = client == readClient ? null : readClient;
    client = null;
    readClient = null;

    // this method may be called when Vert.x is not using event loop, so no compose. Just wait for the one!
    closeClient(clientToClose);
    return closeClient(readClientToClose);
  }

  private Future<Void> closeClient(PgPool clientToClose) {
    if (clientToClose == null) {
      return Future.succeededFuture();
    }

    CONNECTION_POOL.removeMultiKey(vertx, tenantId);  // remove (vertx, tenantId, this) entry
    if (sharedPgPool) {
      return Future.succeededFuture();
    }
    return clientToClose.close();
  }

  /**
   * Close the SQL client of this PostgresClient instance.
   * This is idempotent: additional close invocations are always successful.
   * @param whenDone invoked with the close result
   */
  public void closeClient(Handler<AsyncResult<Void>> whenDone) {
    closeClient().onComplete(whenDone);
  }

  /**
   * The number of PgPool instances in use.
   */
  static int getConnectionPoolSize() {
    return sharedPgPool ? PG_POOLS.size() + PG_POOLS_READER.size(): CONNECTION_POOL.size();
  }

  /**
   * Closes all SQL clients of the tenant.
   */
  public static void closeAllClients(String tenantId) {
    if (sharedPgPool) {
      return;
    }

    // A for or forEach loop does not allow concurrent delete
    List<PostgresClient> clients = new ArrayList<>();
    CONNECTION_POOL.forEach((multiKey, postgresClient) -> {
      if (tenantId.equals(multiKey.getKey(1))) {
        clients.add(postgresClient);
      }
    });
    clients.forEach(PostgresClient::closeClient);
  }

  /**
   * Close all SQL clients stored in the connection pool.
   */
  public static void closeAllClients() {
    // copy of values() because closeClient will delete them from CONNECTION_POOL
    for (PostgresClient client : CONNECTION_POOL.values().toArray(new PostgresClient [0])) {
      client.closeClient();
    }

    PG_POOLS.values().forEach(PgPool::close);
    PG_POOLS.clear();
    PG_POOLS_READER.values().forEach(PgPool::close);
    PG_POOLS_READER.clear();
  }

  static PgConnectOptions createPgConnectOptions(JsonObject sqlConfig, boolean isReader) {
    PgConnectOptions pgConnectOptions = new PgConnectOptions();
    String hostToResolve = HOST;
    String portToResolve = PORT;

    if (isReader) {
       hostToResolve = HOST_READER;
       portToResolve = PORT_READER;
    }

    String host = sqlConfig.getString(hostToResolve);
    if (host != null) {
      pgConnectOptions.setHost(host);
    }

    Integer port;
    port = sqlConfig.getInteger(portToResolve);

    if (port != null) {
      pgConnectOptions.setPort(port);
    }

    if (isReader && (host == null || port == null)) {
      return null;
    }

    String username = sqlConfig.getString(USERNAME);
    if (username != null) {
      pgConnectOptions.setUser(username);
    }
    String password = sqlConfig.getString(PASSWORD);
    if (password != null) {
      pgConnectOptions.setPassword(password);
    }
    String database = sqlConfig.getString(DATABASE);
    if (database != null) {
      pgConnectOptions.setDatabase(database);
    }
    Integer reconnectAttempts = sqlConfig.getInteger(RECONNECT_ATTEMPTS);
    if (reconnectAttempts != null) {
      pgConnectOptions.setReconnectAttempts(reconnectAttempts);
    }
    Long reconnectInterval = sqlConfig.getLong(RECONNECT_INTERVAL);
    if (reconnectInterval != null) {
      pgConnectOptions.setReconnectInterval(reconnectInterval);
    }
    String serverPem = sqlConfig.getString(SERVER_PEM);
    if (serverPem != null) {
      pgConnectOptions.setSslMode(SslMode.VERIFY_FULL);
      pgConnectOptions.setHostnameVerificationAlgorithm("HTTPS");
      pgConnectOptions.setPemTrustOptions(
          new PemTrustOptions().addCertValue(Buffer.buffer(serverPem)));
      pgConnectOptions.setEnabledSecureTransportProtocols(Collections.singleton("TLSv1.3"));
      if (OpenSSLEngineOptions.isAvailable()) {
        pgConnectOptions.setOpenSslEngineOptions(new OpenSSLEngineOptions());
      } else {
        pgConnectOptions.setJdkSslEngineOptions(new JdkSSLEngineOptions());
        log.error("Cannot run OpenSSL, using slow JDKSSL. Is netty-tcnative-boringssl-static for windows-x86_64, "
            + "osx-x86_64 or linux-x86_64 installed? https://netty.io/wiki/forked-tomcat-native.html "
            + "Is libc6-compat installed (if required)? https://github.com/pires/netty-tcnative-alpine");
      }
      log.debug("Enforcing SSL encryption for PostgreSQL connections, "
          + "requiring TLSv1.3 with server name certificate, "
          + "using " + (OpenSSLEngineOptions.isAvailable() ? "OpenSSL " + OpenSsl.versionString() : "JDKSSL"));
    }
    return pgConnectOptions;
  }

  private void init() throws Exception {

    /* check if in pom.xml this prop is declared in order to work with encrypted
     * passwords for postgres embedded - this is a dev mode only feature */
    String secretKey = System.getProperty("postgres_secretkey_4_embeddedmode");

    if (secretKey != null) {
      AES.setSecretKey(secretKey);
    }

    postgreSQLClientConfig = getPostgreSQLClientConfig(tenantId, schemaName, Envs.allDBConfs(), isEmbedded());

    if (Boolean.TRUE.equals(postgreSQLClientConfig.getBoolean(POSTGRES_TESTER))) {
      startPostgresTester();
    }
    logPostgresConfig();

    if (sharedPgPool) {
      client = PG_POOLS.computeIfAbsent(vertx, x -> createPgPool(vertx, postgreSQLClientConfig, false));
      readClient = PG_POOLS_READER.computeIfAbsent(vertx, x -> createPgPool(vertx, postgreSQLClientConfig, true));
    } else {
      client = createPgPool(vertx, postgreSQLClientConfig, false);
      readClient = createPgPool(vertx, postgreSQLClientConfig, true);
    }

    // TODO Remove this.
    // readClient = null;

    readClient = readClient != null ? readClient : client;
  }

  static PgPool createPgPool(Vertx vertx, JsonObject configuration, Boolean isReader) {
    PgConnectOptions connectOptions = createPgConnectOptions(configuration, isReader);

    if (connectOptions == null) {
      return null;
    }

    PoolOptions poolOptions = new PoolOptions();
    poolOptions.setMaxSize(
        configuration.getInteger(MAX_SHARED_POOL_SIZE, configuration.getInteger(MAX_POOL_SIZE, 4)));
    Integer connectionReleaseDelay = configuration.getInteger(CONNECTION_RELEASE_DELAY, DEFAULT_CONNECTION_RELEASE_DELAY);
    poolOptions.setIdleTimeout(connectionReleaseDelay);
    poolOptions.setIdleTimeoutUnit(TimeUnit.MILLISECONDS);

    return PgPool.pool(vertx, connectOptions, poolOptions);
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
  static JsonObject getPostgreSQLClientConfig(String tenantId, String schemaName, JsonObject environmentVariables,
                                              boolean postgresTesterEnabled)
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
      log.info("No DB configuration found, setting username, password and database for testing");
      config = new JsonObject();
      config.put(POSTGRES_TESTER, postgresTesterEnabled);
      config.put(USERNAME, USERNAME);
      config.put(PASSWORD, PASSWORD);
      config.put(DATABASE, "postgres");
    } else {
      config.put(POSTGRES_TESTER, false);
    }
    Object v = config.remove(Envs.DB_EXPLAIN_QUERY_THRESHOLD.name());
    if (v instanceof Long) {
      PostgresClient.setExplainQueryThreshold((Long) v);
    }
    sharedPgPool |= config.containsKey(MAX_SHARED_POOL_SIZE);
    if (tenantId.equals(DEFAULT_SCHEMA) || sharedPgPool) {
      config.put(PASSWORD, decodePassword( config.getString(PASSWORD) ));
    } else {
      log.info("Using schema: " + tenantId);
      config.put(USERNAME, schemaName);
      config.put(PASSWORD, createPassword(tenantId));
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
    passwordRedacted.put(PASSWORD, "...");
    log.info("postgreSQLClientConfig = " + passwordRedacted.encode());
  }

  /**
   * Get connection configuration.
   * The following properties are returned (some of which are optional):
   * username, password, host, port, database, connectionReleaseDelay, maxPoolSize.
   * Originally based on driver
   * <a href="https://vertx.io/docs/vertx-mysql-postgresql-client/java/#_configuration">
   *   Configuration
   *   </a>.
   * which is no longer in actual use.
   *
   * @return
   */
  public JsonObject getConnectionConfig(){
    return postgreSQLClientConfig;
  }

  public static JsonObject pojo2JsonObject(Object entity) throws JsonProcessingException {
    if (entity == null) {
      throw new IllegalArgumentException("Entity can not be null");
    }
    if (entity instanceof JsonObject) {
      return ((JsonObject) entity);
    } else {
      return new JsonObject(MAPPER.writeValueAsString(entity));
    }
  }

  /**
   * Start an SQL transaction.
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
   * @param done - the result is the current connection
   * @deprecated use {@link #withTrans(Function)}, {@link #withTrans(int, Function)},
   *             {@link #withConn(Function)}.{@link Conn#getPgConnection() getPgConnection()} or
   *             {@link #withConn(int, Function)}.{@link Conn#getPgConnection() getPgConnection()}
   *             instead
   */
  @Deprecated
  public void startTx(Handler<AsyncResult<SQLConnection>> done) {
    getConnection(res -> {
      if (res.failed()) {
        log.error(res.cause().getMessage(), res.cause());
        done.handle(Future.failedFuture(res.cause()));
        return;
      }
      try {
        res.result().begin()
        .map(transaction -> new SQLConnection(res.result(), transaction, null))
        .onComplete(done);
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        done.handle(Future.failedFuture(e.getCause()));
      }
    });
  }

  static void finalizeTx(AsyncResult<Void> txResult, PgConnection conn, Handler<AsyncResult<Void>> done ) {
    if (conn != null) {
      conn.close();
    }
    if (txResult.failed() && !"Transaction already completed".equals(txResult.cause().getMessage())) {
      done.handle(Future.failedFuture(txResult.cause()));
      return;
    }
    done.handle(Future.succeededFuture());
  }

  /**
   * Rollback an SQL transaction started on the connection. This closes the connection.
   *
   * @see #startTx(Handler)
   * @param trans the connection with an open transaction
   * @param done  success or failure
   * @deprecated use {@link #withTrans(Function)}, {@link #withTrans(int, Function)},
   *             {@link #withConn(Function)}.{@link Conn#getPgConnection() getPgConnection()} or
   *             {@link #withConn(int, Function)}.{@link Conn#getPgConnection() getPgConnection()}
   *             instead
   */
  @Deprecated
  public void rollbackTx(AsyncResult<SQLConnection> trans, Handler<AsyncResult<Void>> done) {
    try {
      if (trans.failed()) {
        done.handle(Future.failedFuture(trans.cause()));
        return;
      }
      trans.result().tx.rollback(res -> finalizeTx(res, trans.result().conn, done));
    } catch (Exception e) {
      done.handle(Future.failedFuture(e));
    }
  }

  /**
   * Ends an SQL transaction (commit) started on the connection. This closes the connection.
   *
   * @see #startTx(Handler)
   * @param trans  the connection with an open transaction
   * @param done  success or failure
   * @deprecated use {@link #withTrans(Function)}, {@link #withTrans(int, Function)},
   *             {@link #withConn(Function)}.{@link Conn#getPgConnection() getPgConnection()} or
   *             {@link #withConn(int, Function)}.{@link Conn#getPgConnection() getPgConnection()}
   *             instead
   */
  @Deprecated
  public void endTx(AsyncResult<SQLConnection> trans, Handler<AsyncResult<Void>> done) {
    try {
      if (trans.failed()) {
        done.handle(Future.failedFuture(trans.cause()));
        return;
      }
      trans.result().tx.commit(res -> finalizeTx(res, trans.result().conn, done));
    } catch (Exception e) {
      done.handle(Future.failedFuture(e));
    }
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
  <T> Handler<AsyncResult<T>> closeAndHandleResult(
      AsyncResult<SQLConnection> conn, Handler<AsyncResult<T>> handler) {

    return ar -> {
      if (conn.failed()) {
        log.error("Opening SQLConnection failed: " + conn.cause().getMessage(), conn.cause());
        handler.handle(ar);
        return;
      }
      conn.result().close(vertx);
      handler.handle(ar);
    };
  }

  /**
   * Insert entity into table. Create a new id UUID and return it via replyHandler.
   * @param table database table (without schema)
   * @param entity a POJO (plain old java object), an existing id will be overwritten
   * @return new id
   */
  public Future<String> save(String table, Object entity) {
    return withConn(conn -> conn.save(table, entity));
  }

  /**
   * Insert entity into table. Create a new id UUID and return it via replyHandler.
   * @param table database table (without schema)
   * @param entity a POJO (plain old java object), an existing id will be overwritten
   * @param replyHandler returns any errors and the result.
   */
  public void save(String table, Object entity, Handler<AsyncResult<String>> replyHandler) {
    save(table, entity).onComplete(replyHandler);
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param entity a POJO (plain old java object)
   * @param returnId true to return the id of the inserted record, false to return an empty string
   */
  public Future<String> save(String table, Object entity, boolean returnId) {
    return withConn(conn -> conn.save(table, entity, returnId));
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param entity a POJO (plain old java object)
   * @param returnId true to return the id of the inserted record, false to return an empty string
   * @param replyHandler returns any errors and the result.
   */
  public void save(String table, Object entity, boolean returnId, Handler<AsyncResult<String>> replyHandler) {
    save(table, entity, returnId).onComplete(replyHandler);
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity a POJO (plain old java object)
   * @return id
   */
  public Future<String> save(String table, String id, Object entity) {
    return withConn(conn -> conn.save(table, id, entity));
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity a POJO (plain old java object)
   * @param replyHandler returns any errors and the result (see returnId).
   */
  public void save(String table, String id, Object entity, Handler<AsyncResult<String>> replyHandler) {
    save(table, id, entity).onComplete(replyHandler);
  }

  /**
   * Insert entity into table and return the updated entity.
   * @param table database table (without schema)
   * @param id primary key for the record
   * @param entity a POJO (plain old java object)
   * @param replyHandler returns any errors and the entity after applying any database INSERT triggers
   * @return the entity after applying any database INSERT triggers
   */
  public <T> Future<T> saveAndReturnUpdatedEntity(String table, String id, T entity) {
    return withConn(conn -> conn.saveAndReturnUpdatedEntity(table, id, entity));
  }

  /**
   * Insert entity into table and return the updated entity.
   * @param table database table (without schema)
   * @param id primary key for the record
   * @param entity a POJO (plain old java object)
   * @param replyHandler returns any errors and the entity after applying any database INSERT triggers
   */
  <T> void saveAndReturnUpdatedEntity(String table, String id, T entity, Handler<AsyncResult<T>> replyHandler) {
    saveAndReturnUpdatedEntity(table, id, entity).onComplete(replyHandler);
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity a POJO (plain old java object)
   * @param returnId true to return the id of the inserted record, false to return an empty string
   */
  public Future<String> save(String table, String id, Object entity, boolean returnId) {
    return withConn(conn -> conn.save(table, id, entity, returnId));
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity a POJO (plain old java object)
   * @param returnId true to return the id of the inserted record, false to return an empty string
   * @param replyHandler returns any errors and the result (see returnId).
   */
  public void save(String table, String id, Object entity,
      boolean returnId, Handler<AsyncResult<String>> replyHandler) {
    save(table, id, entity, returnId).onComplete(replyHandler);
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity a POJO (plain old java object)
   * @param returnId true to return the id of the inserted record, false to return an empty string
   * @param upsert whether to update if the record with that id already exists (INSERT or UPDATE)
   */
  public Future<String> save(String table, String id, Object entity,
      boolean returnId, boolean upsert) {
    return withConn(conn -> conn.save(table, id, entity, returnId, upsert));
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity a POJO (plain old java object)
   * @param returnId true to return the id of the inserted record, false to return an empty string
   * @param upsert whether to update if the record with that id already exists (INSERT or UPDATE)
   * @param replyHandler returns any errors and the result (see returnId).
   */
  public void save(String table, String id, Object entity,
      boolean returnId, boolean upsert, Handler<AsyncResult<String>> replyHandler) {
    save(table, id, entity, returnId, upsert).onComplete(replyHandler);
  }

  /**
   * Insert entity into table, or update it if it already exists.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity a POJO (plain old java object)
   * @return id of the entity
   */
  public Future<String> upsert(String table, String id, Object entity) {
    return withConn(conn -> conn.upsert(table, id, entity));
  }

  /**
   * Insert entity into table, or update it if it already exists.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity a POJO (plain old java object)
   * @param replyHandler returns any errors and the id of the entity.
   */
  public void upsert(String table, String id, Object entity, Handler<AsyncResult<String>> replyHandler) {
    upsert(table, id, entity).onComplete(replyHandler);
  }

  /**
   * Insert or update.
   *
   * <p>Needed if upserting binary data as base64 where converting it to a json will corrupt the data
   * otherwise this function is not needed as the default is true
   * example:
   *     byte[] data = ......;
   *     JsonArray jsonArray = new JsonArray().add(data);
   *     conn.upsert(TABLE_NAME, id, jsonArray, false)

   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity either a POJO, or a JsonArray containing a byte[] element, see convertEntity
   * @param convertEntity true if entity is a POJO, false if entity is a JsonArray
   * @return id
   */
  public Future<String> upsert(String table, String id, Object entity, boolean convertEntity) {
    return withConn(conn -> conn.upsert(table, id, entity, convertEntity));
  }

  /**
   * Insert or update.
   *
   * <p>Needed if upserting binary data as base64 where converting it to a json will corrupt the data
   * otherwise this function is not needed as the default is true
   * example:
   *     byte[] data = ......;
   *     JsonArray jsonArray = new JsonArray().add(data);
   *     .upsert(TABLE_NAME, id, jsonArray, false, replyHandler -> {

   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity either a POJO, or a JsonArray containing a byte[] element, see convertEntity
   * @param convertEntity true if entity is a POJO, false if entity is a JsonArray
   * @param replyHandler returns any errors and the result (see returnId).
   */
  public void upsert(String table, String id, Object entity, boolean convertEntity,
      Handler<AsyncResult<String>> replyHandler) {
    upsert(table, id, entity, convertEntity).onComplete(replyHandler);
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity either a POJO, or a JsonArray containing a byte[] element, see convertEntity
   * @param returnId true to return the id of the inserted record, false to return an empty string
   * @param upsert whether to update if the record with that id already exists (INSERT or UPDATE)
   * @param convertEntity true if entity is a POJO, false if entity is a JsonArray
   */
  public Future<String> save(String table, String id, Object entity, boolean returnId, boolean upsert, boolean convertEntity) {
    return withConn(conn -> conn.save(table, id, entity, returnId, upsert, convertEntity));
  }

  /**
   * Insert entity into table.
   * @param table database table (without schema)
   * @param id primary key for the record, or null if one should be created
   * @param entity either a POJO, or a JsonArray containing a byte[] element, see convertEntity
   * @param returnId true to return the id of the inserted record, false to return an empty string
   * @param upsert whether to update if the record with that id already exists (INSERT or UPDATE)
   * @param convertEntity true if entity is a POJO, false if entity is a JsonArray
   * @param replyHandler returns any errors and the result (see returnId).
   */
  public void save(String table, String id, Object entity, boolean returnId, boolean upsert, boolean convertEntity,
      Handler<AsyncResult<String>> replyHandler) {
    save(table, id, entity, returnId, upsert, convertEntity).onComplete(replyHandler);
  }

  /**
   * Save entity in table using the sqlConnection. Return the
   * created id via the replyHandler.
   *
   * @param sqlConnection connection with transaction
   * @param table where to insert the entity record
   * @param entity the record to insert, a POJO (plain old java object)
   * @param replyHandler where to report success status and the created id
   */
  public void save(AsyncResult<SQLConnection> sqlConnection, String table, Object entity,
    Handler<AsyncResult<String>> replyHandler) {
    save(sqlConnection, table, /* id */ null, entity,
        /* returnId */ true, /* upsert */ false, /* convertEntity */ true, replyHandler);
  }

  /**
   * Save entity in table. Use the transaction of sqlConnection. Return the id
   * of the id field (primary key) via the replyHandler. If id (primary key) and
   * the id of entity (jsonb field) are different you may need a trigger in the
   * database to sync them.
   *
   * @param sqlConnection connection (for example with transaction)
   * @param table where to insert the entity record
   * @param id  the value for the id field (primary key); if null a new random UUID is created for it.
   * @param entity  the record to insert, a POJO (plain old java object)
   * @param replyHandler  where to report success status and the final id of the id field
   */
  public void save(AsyncResult<SQLConnection> sqlConnection, String table, String id, Object entity,
      Handler<AsyncResult<String>> replyHandler) {
    save(sqlConnection, table, id, entity,
        /* returnId */ true, /* upsert */ false, /* convertEntity */ true, replyHandler);
  }

  /**
   * Save entity in table. Use the transaction of sqlConnection. Return the id
   * of the id field (primary key) via the replyHandler. If id (primary key) and
   * the id of entity (jsonb field) are different you may need a trigger in the
   * database to sync them.
   *
   * @param sqlConnection connection (for example with transaction)
   * @param table where to insert the entity record
   * @param id  the value for the id field (primary key); if null a new random UUID is created for it.
   * @param entity  the record to insert, a POJO (plain old java object)
   * @param returnId true to return the id of the inserted record, false to return an empty string
   * @param upsert whether to update if the record with that id already exists (INSERT or UPDATE)
   * @param replyHandler  where to report success status and the final id of the id field
   */
  public void save(AsyncResult<SQLConnection> sqlConnection, String table, String id, Object entity,
      boolean returnId, boolean upsert,
      Handler<AsyncResult<String>> replyHandler) {
    save(sqlConnection, table, id, entity, returnId, upsert, /* convertEntity */ true, replyHandler);
  }

  /**
   * Save entity in table. Use the transaction of sqlConnection. Return the id
   * of the id field (primary key) via the replyHandler. If id (primary key) and
   * the id of entity (jsonb field) are different you may need a trigger in the
   * database to sync them.
   *
   * @param sqlConnection connection (for example with transaction)
   * @param table where to insert the entity record
   * @param id  the value for the id field (primary key); if null a new random UUID is created for it.
   * @param entity  the record to insert, either a POJO or a JsonArray, see convertEntity
   * @param returnId true to return the id of the inserted record, false to return an empty string
   * @param upsert whether to update if the record with that id already exists (INSERT or UPDATE)
   * @param convertEntity true if entity is a POJO, false if entity is a JsonArray
   * @param replyHandler  where to report success status and the final id of the id field
   */
  @SuppressWarnings({"squid:S00107"})   // Method has more than 7 parameters
  public void save(AsyncResult<SQLConnection> sqlConnection, String table, String id, Object entity,
                   boolean returnId, boolean upsert, boolean convertEntity,
                   Handler<AsyncResult<String>> replyHandler) {

    if (log.isDebugEnabled()) {
      log.debug("save (with connection and id) called on " + table);
    }
    withConn(sqlConnection, conn -> conn.save(table, id, entity, returnId, upsert, convertEntity))
    .onComplete(replyHandler);
  }

  /**
   * Insert the entities into table using a single transaction.
   * @param table  destination table to insert into
   * @param entities  each array element is a String with the content for the JSONB field of table; if id is missing a random id is generated
   * @return one result row per inserted row, containing the id field
   */
  public Future<RowSet<Row>> saveBatch(String table, JsonArray entities) {
    return withTrans(conn -> conn.saveBatch(table, entities));
  }

  /**
   * Insert the entities into table using a single transaction.
   * @param table  destination table to insert into
   * @param entities  each array element is a String with the content for the JSONB field of table; if id is missing a random id is generated
   * @param replyHandler  result, containing the id field for each inserted element of entities
   */
  public void saveBatch(String table, JsonArray entities, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    saveBatch(table, entities).onComplete(replyHandler);
  }

  /**
   * Update the entities using a single transaction, the id property is used for matching.
   * @param table  table to update
   * @param entities  each array element is a String with the content for the JSONB field of table
   * @return one {@link RowSet} per array element with {@link RowSet#rowCount()} information
   */
  public Future<RowSet<Row>> updateBatch(String table, JsonArray entities) {
    return withTrans(conn -> conn.updateBatch(table, entities));
  }

  /**
   * Update the entities using a single transaction, the id property is used for matching.
   * @param table  table to update
   * @param entities  each array element is a String with the content for the JSONB field of table
   * @param replyHandler  one {@link RowSet} per array element with {@link RowSet#rowCount()} information
   */
  public void updateBatch(String table, JsonArray entities, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    updateBatch(table, entities).onComplete(replyHandler);
  }

  /**
   * Upsert the entities into table using a single transaction.
   * @param table  destination table to insert into
   * @param entities  each array element is a String with the content for the JSONB field of table; if id is missing a random id is generated
   * @return one result row per inserted row, containing the id field
   */
  public Future<RowSet<Row>> upsertBatch(String table, JsonArray entities) {
    return withTrans(conn -> conn.upsertBatch(table, entities));
  }

  /**
   * Upsert the entities into table using a single transaction.
   * @param table  destination table to insert into
   * @param entities  each array element is a String with the content for the JSONB field of table; if id is missing a random id is generated
   * @param replyHandler  one result row per inserted row, containing the id field
   */
  public void upsertBatch(String table, JsonArray entities, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    upsertBatch(table, entities).onComplete(replyHandler);
  }

  /**
   * Insert the entities into table using a single transaction.
   * @param sqlConnection  the connection to run on, may be on a transaction
   * @param table  destination table to insert into
   * @param entities  each array element is a String with the content for the JSONB field of table; if id is missing a random id is generated
   * @param replyHandler  result, containing the id field for each inserted element of entities
   */
  public void saveBatch(AsyncResult<SQLConnection> sqlConnection, String table,
      JsonArray entities, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    saveBatch(sqlConnection, /* upsert */ false, table, entities, replyHandler);
  }

  /**
   * Upsert the entities into table using a single transaction.
   * @param sqlConnection  the connection to run on, may be on a transaction
   * @param table  destination table to insert into
   * @param entities  each array element is a String with the content for the JSONB field of table; if id is missing a random id is generated
   * @param replyHandler  result, containing the id field for each inserted element of entities
   */
  public void upsertBatch(AsyncResult<SQLConnection> sqlConnection, String table,
      JsonArray entities, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    saveBatch(sqlConnection, /* upsert */ true, table, entities, replyHandler);
  }

  /**
   * Insert or upsert the entities into table using a single transaction.
   * @param sqlConnection  the connection to run on, must be on a transaction so that SELECT ... FOR UPDATE works
   * @param upsert  true for upsert, false for insert with fail on duplicate id
   * @param table  destination table to insert into
   * @param entities  each array element is a String with the content for the JSONB field of table; if id is missing a random id is generated
   * @param replyHandler  result, containing the id field for each inserted element of entities
   */
  private void saveBatch(AsyncResult<SQLConnection> sqlConnection, boolean upsert, String table,
      JsonArray entities, Handler<AsyncResult<RowSet<Row>>> replyHandler) {

    withConn(sqlConnection, conn -> conn.saveBatch(upsert, table, entities))
    .onComplete(replyHandler);
  }

  /**
   * Save a list of POJOs.
   * POJOs are converted to a JSON String and saved in a single transaction.
   * A random id is generated if POJO's id is null.
   * Call {@link MetadataUtil#populateMetadata(List, Map)} before if applicable.
   * @param table  destination table to insert into
   * @param entities  each list element is a POJO
   * @return one result row per inserted row, containing the id field
   */
  public <T> Future<RowSet<Row>> saveBatch(String table, List<T> entities) {
    return withTrans(conn -> conn.saveBatch(table, entities));
  }

  /**
   * Save a list of POJOs.
   * POJOs are converted to a JSON String and saved in a single transaction.
   * A random id is generated if POJO's id is null.
   * Call {@link MetadataUtil#populateMetadata(List, Map)} before if applicable.
   * @param table  destination table to insert into
   * @param entities  each list element is a POJO
   * @param replyHandler result, containing the id field for each inserted POJO
   */
  public <T> void saveBatch(String table, List<T> entities,
      Handler<AsyncResult<RowSet<Row>>> replyHandler) {

    saveBatch(table, entities).onComplete(replyHandler);
  }

  /**
   * Update a list of POJOs in a single transaction.
   * Call {@link MetadataUtil#populateMetadata(List, Map)} before if applicable.
   * @param table  destination table to update
   * @param entities  each list element is a POJO
   * @return one {@link RowSet} per list element with {@link RowSet#rowCount()} information
   */
  public <T> Future<RowSet<Row>> updateBatch(String table, List<T> entities) {
    return withTrans(conn -> conn.updateBatch(table, entities));
  }

  /**
   * Update a list of POJOs in a single transaction.
   * Call {@link MetadataUtil#populateMetadata(List, Map)} before if applicable.
   * @param table  destination table to update
   * @param entities  each list element is a POJO
   * @param replyHandler one {@link RowSet} per list element with {@link RowSet#rowCount()} information
   */
  public <T> void updateBatch(String table, List<T> entities,
      Handler<AsyncResult<RowSet<Row>>> replyHandler) {

    updateBatch(table, entities).onComplete(replyHandler);
  }

  /**
   * Upsert a list of POJOs in a single transaction.
   * POJOs are converted to a JSON String.
   * A random id is generated if POJO's id is null.
   * If a record with the id already exists it is updated (upsert).
   * Call {@link MetadataUtil#populateMetadata(List, Map)} before if applicable.
   * @param table  destination table to insert into
   * @param entities  each list element is a POJO
   * @return one result row per inserted row, containing the id field
   */
  public <T> Future<RowSet<Row>> upsertBatch(String table, List<T> entities) {
    return withTrans(conn -> conn.upsertBatch(table, entities));
  }

  /***
   * Upsert a list of POJOs in a single transaction.
   * POJOs are converted to a JSON String.
   * A random id is generated if POJO's id is null.
   * If a record with the id already exists it is updated (upsert).
   * Call {@link MetadataUtil#populateMetadata(List, Map)} before if applicable.
   * @param table  destination table to insert into
   * @param entities  each list element is a POJO
   * @param replyHandler one result row per inserted row, containing the id field
   */
  public <T> void upsertBatch(String table, List<T> entities,
      Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    upsertBatch(table, entities).onComplete(replyHandler);
  }

  /***
   * Save a list of POJOs.
   * POJOs are converted to a JSON String.
   * A random id is generated if POJO's id is null.
   * Call {@link MetadataUtil#populateMetadata(List, Map)} before if applicable.
   * @param sqlConnection  the connection to run on, must be on a transaction so that SELECT ... FOR UPDATE works
   * @param table  destination table to insert into
   * @param entities  each list element is a POJO
   * @param replyHandler result, containing the id field for each inserted POJO
   */
  public <T> void saveBatch(AsyncResult<SQLConnection> sqlConnection, String table,
      List<T> entities, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    saveBatch(sqlConnection, /* upsert */ false, table, entities, replyHandler);
  }

  /***
   * Upsert a list of POJOs.
   * POJOs are converted to a JSON String.
   * A random id is generated if POJO's id is null.
   * If a record with the id already exists it is updated (upsert).
   * Call {@link MetadataUtil#populateMetadata(List, Map)} before if applicable.
   * @param sqlConnection  the connection to run on, must be on a transaction so that SELECT ... FOR UPDATE works
   * @param table  destination table to insert into
   * @param entities  each list element is a POJO
   * @param replyHandler result, containing the id field for each inserted POJO
   */
  public <T> void upsertBatch(AsyncResult<SQLConnection> sqlConnection, String table,
      List<T> entities, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    saveBatch(sqlConnection, /* upsert */ true, table, entities, replyHandler);
  }

  private <T> void saveBatch(AsyncResult<SQLConnection> sqlConnection,
      boolean upsert, String table, List<T> entities, Handler<AsyncResult<RowSet<Row>>> replyHandler) {

    withConn(sqlConnection, conn -> conn.saveBatch(upsert, table, entities))
    .onComplete(replyHandler);
  }

  /**
   * Update a specific record associated with the key passed in the id arg
   * @param table - table to save to (must exist)
   * @param entity - pojo to save
   * @param id - key of the entity being updated
   * @return empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public Future<RowSet<Row>> update(String table, Object entity, String id) {
    return withConn(conn -> conn.update(table, entity, id));
  }

  /**
   * Update a specific record associated with the key passed in the id arg
   * @param table - table to save to (must exist)
   * @param entity - pojo to save
   * @param id - key of the entity being updated
   * @param replyHandler
   */
  public void update(String table, Object entity, String id, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    update(table, entity, id).onComplete(replyHandler);
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
   * @param filter - determines which records to update
   * @param returnUpdatedIds - return ids of updated records
   * @return ids of updated records if {@code returnUpdatedIds} is true
   *
   */
  public Future<RowSet<Row>> update(String table, Object entity, Criterion filter, boolean returnUpdatedIds) {
    return withConn(conn -> conn.update(table, entity, filter, returnUpdatedIds));
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
   * @param filter - determines which records to update
   * @param returnUpdatedIds - return ids of updated records
   */
  public void update(String table, Object entity, Criterion filter, boolean returnUpdatedIds,
                     Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    update(table, entity, filter, returnUpdatedIds).onComplete(replyHandler);
  }

  /**
   * Update all records in {@code table} that match the {@code CQLWrapper} query.
   * @param entity new content for the matched records
   * @return one row with the id for each updated record if returnUpdatedIds is true
   */
  public Future<RowSet<Row>> update(String table, Object entity, CQLWrapper filter, boolean returnUpdatedIds) {
    return withConn(conn -> conn.update(table, entity, filter, returnUpdatedIds));
  }

  /**
   * Update all records in {@code table} that match the {@code CQLWrapper} query.
   * @param entity new content for the matched records
   * @param replyHandler one row with the id for each updated record if returnUpdatedIds is true
   */
  public void update(String table, Object entity, CQLWrapper filter, boolean returnUpdatedIds,
      Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    update(table, entity, filter, returnUpdatedIds).onComplete(replyHandler);
  }

  /**
   * Update all records in {@code table} that match the {@code CQLWrapper} query.
   * @param entity new content for the matched records
   * @param replyHandler one row with the id for each updated record if returnUpdatedIds is true
   */
  public void update(AsyncResult<SQLConnection> sqlConnection, String table, Object entity, CQLWrapper filter,
                     boolean returnUpdatedIds, Handler<AsyncResult<RowSet<Row>>> replyHandler) {

    withConn(sqlConnection, conn -> conn.update(table, entity, filter, returnUpdatedIds))
    .onComplete(replyHandler);
  }

  /**
   * Update all records in {@code table} that match the SQL {@code whereClause}.
   *
   * <p>Danger: The {@code whereClause} is prone to SQL injection. Consider using
   * an {@code update} method that takes {@link CQLWrapper} or {@link Criterion}.
   *
   * @param entity new content for the jsonbField of the matched records
   * @param replyHandler one row with the id for each updated record if returnUpdatedIds is true
   */
  public void update(AsyncResult<SQLConnection> sqlConnection, String table, Object entity, String jsonbField,
                     String whereClause, boolean returnUpdatedIds, Handler<AsyncResult<RowSet<Row>>> replyHandler) {

    withConn(sqlConnection, conn -> conn.update(table, entity, jsonbField, whereClause, returnUpdatedIds))
    .onComplete(replyHandler);
  }

  /**
   * Update all records in {@code table} that match the SQL {@code whereClause} query.
   *
   * <p>Danger: The {@code whereClause} is prone to SQL injection. Consider using
   * an {@code update} method that takes {@link CQLWrapper} or {@link Criterion}.
   *
   * @param entity new content for the jsonbField of the matched records
   * @return one row with the id for each updated record if returnUpdatedIds is true
   */
  public Future<RowSet<Row>> update(String table, Object entity, String jsonbField, String whereClause, boolean returnUpdatedIds) {
    return withConn(conn -> conn.update(table, entity, jsonbField, whereClause, returnUpdatedIds));
  }

  /**
   * Update all records in {@code table} that match the SQL {@code whereClause} query.
   *
   * <p>Danger: The {@code whereClause} is prone to SQL injection. Consider using
   * an {@code update} method that takes {@link CQLWrapper} or {@link Criterion}.
   *
   * @param entity new content for the jsonbField of the matched records
   * @param replyHandler one row with the id for each updated record if returnUpdatedIds is true
   */
  public void update(String table, Object entity, String jsonbField, String whereClause, boolean returnUpdatedIds,
                     Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    update(table, entity, jsonbField, whereClause, returnUpdatedIds)
    .onComplete(replyHandler);
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
   * @param returnUpdatedIds
   *
   */
  public Future<RowSet<Row>> update(String table, UpdateSection section, Criterion when, boolean returnUpdatedIds) {
    return withConn(conn -> conn.update(table, section,  when, returnUpdatedIds));
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
  public void update(String table, UpdateSection section, Criterion when, boolean returnUpdatedIds,
                     Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    update(table, section, when, returnUpdatedIds)
    .onComplete(replyHandler);
  }

  /**
   * Delete by id.
   * @param table table name without schema
   * @param id primary key value of the record to delete
   * @return empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public Future<RowSet<Row>> delete(String table, String id) {
    return withConn(conn -> conn.delete(table, id));
  }

  /**
   * Delete by id.
   * @param table table name without schema
   * @param id primary key value of the record to delete
   * @param replyHandler empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public void delete(String table, String id, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    delete(table, id).onComplete(replyHandler);
  }

  /**
   * Delete by id.
   * @param connection where to run, can be within a transaction
   * @param table table name without schema
   * @param id primary key value of the record to delete
   * @param replyHandler empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public void delete(AsyncResult<SQLConnection> connection, String table, String id,
      Handler<AsyncResult<RowSet<Row>>> replyHandler) {

    withConn(connection, conn -> conn.delete(table, id))
    .onComplete(replyHandler);
  }

  /**
   * Delete by CQL wrapper.
   * @param table table name without schema
   * @param cql which records to delete
   * @return empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public Future<RowSet<Row>> delete(String table, CQLWrapper cql) {
    return withConn(conn -> conn.delete(table, cql));
  }

  /**
   * Delete by CQL wrapper.
   * @param table table name without schema
   * @param cql which records to delete
   * @param replyHandler empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public void delete(String table, CQLWrapper cql, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    delete(table, cql).onComplete(replyHandler);
  }

  /**
   * Delete by CQL wrapper.
   * @param connection where to run, can be within a transaction
   * @param table table name without schema
   * @param cql which records to delete
   * @param replyHandler empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public void delete(AsyncResult<SQLConnection> connection, String table, CQLWrapper cql,
      Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    withConn(connection, conn -> conn.delete(table, cql))
    .onComplete(replyHandler);
  }

  /**
   * Delete based on filter
   * @param table table name without schema
   * @param filter which records to delete
   * @return empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public Future<RowSet<Row>> delete(String table, Criterion filter) {
    return withConn(conn -> conn.delete(table, filter));
  }

  /**
   * Delete based on filter
   * @param table table name without schema
   * @param filter which records to delete
   * @param replyHandler empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public void delete(String table, Criterion filter, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    delete(table, filter).onComplete(replyHandler);
  }

  /**
   * Delete as part of a transaction
   * @param sqlConnection where to run, can be within a transaction
   * @param table table name without schema
   * @param filter which records to delete
   * @param replyHandler empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public void delete(AsyncResult<SQLConnection> sqlConnection, String table, Criterion filter,
      Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    withConn(sqlConnection, conn -> conn.delete(table, filter))
    .onComplete(replyHandler);
  }

  /**
   * delete based on jsons matching the field/value pairs in the pojo (which is first converted to json and then similar jsons are searched)
   *  --> do not use on large tables without checking as the @> will not use a btree
   * @return empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public Future<RowSet<Row>> delete(String table, Object entity) {
    return withConn(conn -> conn.delete(table, entity));
  }

  /**
   * delete based on jsons matching the field/value pairs in the pojo (which is first converted to json and then similar jsons are searched)
   *  --> do not use on large tables without checking as the @> will not use a btree
   * @param replyHandler empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public void delete(String table, Object entity, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    delete(table, entity).onComplete(replyHandler);
  }

  /**
   * delete based on jsons matching the field/value pairs in the pojo (which is first converted to json and then similar jsons are searched)
   *  --> do not use on large tables without checking as the @> will not use a btree
   * @param replyHandler empty {@link RowSet} with {@link RowSet#rowCount()} information
   */
  public void delete(AsyncResult<SQLConnection> connection, String table, Object entity,
      Handler<AsyncResult<RowSet<Row>>> replyHandler) {

    withConn(connection, conn -> conn.delete(table, entity))
    .onComplete(replyHandler);
  }

  static class QueryHelper {
    String table;
    String selectQuery;
    String countQuery;
    int offset;
    int limit;
    public QueryHelper(String table) {
      this.table = table;
    }
  }

  static class TotaledResults {
    final RowSet<Row> set;
    final Integer estimatedTotal;
    public TotaledResults(RowSet<Row> set, Integer estimatedTotal) {
      this.set = set;
      this.estimatedTotal = estimatedTotal;
    }
  }


  /**
   * Stream GET with CQLWrapper, no facets {@link org.folio.rest.persist.PostgresClientStreamResult}
   * @param <T>
   * @param table
   * @param clazz
   * @param fieldName
   * @param filter
   * @param returnIdField
   * @param distinctOn may be null
   * @param replyHandler AsyncResult; on success with result {@link org.folio.rest.persist.PostgresClientStreamResult}
   */
  public <T> void streamGet(String table, Class<T> clazz, String fieldName,
    CQLWrapper filter, boolean returnIdField, String distinctOn,
    Handler<AsyncResult<PostgresClientStreamResult<T>>> replyHandler) {

    streamGet(table, clazz, fieldName, filter, returnIdField, distinctOn,
      null, 0, replyHandler);
  }

  /**
   * Stream GET with CQLWrapper, no facets {@link org.folio.rest.persist.PostgresClientStreamResult}
   * @param <T>
   * @param table
   * @param clazz
   * @param fieldName
   * @param filter
   * @param returnIdField
   * @param distinctOn may be null
   * @param queryTimeout query timeout in milliseconds, or 0 for no timeout
   * @param replyHandler AsyncResult; on success with result {@link PostgresClientStreamResult}
   */
  @SuppressWarnings({"squid:S00107"})    // Method has >7 parameters
  public <T> void streamGet(String table, Class<T> clazz, String fieldName,
       CQLWrapper filter, boolean returnIdField, String distinctOn,
       int queryTimeout, Handler<AsyncResult<PostgresClientStreamResult<T>>> replyHandler) {

    streamGet(table, clazz, fieldName, filter, returnIdField, distinctOn,
      null, queryTimeout, replyHandler);
  }

  /**
   * Close conn before calling {@link PostgresClientStreamResult#endHandler(Handler)} or
   * {@link PostgresClientStreamResult#exceptionHandler(Handler)}, and on failed result.
   * @param conn the connection to close
   * @return a handler that ensures that the connection gets closed
   */
  <T> Handler<AsyncResult<PostgresClientStreamResult<T>>> closeAtEnd(
      AsyncResult<SQLConnection> conn, Handler<AsyncResult<PostgresClientStreamResult<T>>> replyHandler) {
    return asyncResult -> {
      try {
        if (asyncResult.succeeded()) {
          asyncResult.result().setCloseHandler(close -> conn.result().close(vertx));
        } else {
          if (conn != null && conn.succeeded()) {
            conn.result().close(vertx);
          }
        }
        replyHandler.handle(asyncResult);
      } catch (Exception e) {
        replyHandler.handle(Future.failedFuture(e));
      }
    };
  }

  /**
   * Stream GET with CQLWrapper and facets {@link org.folio.rest.persist.PostgresClientStreamResult}
   * @param <T>
   * @param table
   * @param clazz
   * @param fieldName
   * @param filter
   * @param returnIdField must be true if facets are in passed
   * @param distinctOn may be null
   * @param facets for no facets: null or Collections.emptyList()
   * @param replyHandler AsyncResult; on success with result {@link org.folio.rest.persist.PostgresClientStreamResult}
   */
  @SuppressWarnings({"squid:S00107"})    // Method has >7 parameters
  public <T> void streamGet(String table, Class<T> clazz, String fieldName,
    CQLWrapper filter, boolean returnIdField, String distinctOn,
    List<FacetField> facets, Handler<AsyncResult<PostgresClientStreamResult<T>>> replyHandler) {

    getSQLReadConnection(0, conn ->
        streamGet(conn, table, clazz, fieldName, filter, returnIdField,
            distinctOn, facets, closeAtEnd(conn, replyHandler)));
  }

  /**
   * Stream GET with CQLWrapper and facets {@link org.folio.rest.persist.PostgresClientStreamResult}
   * @param <T>
   * @param table
   * @param clazz
   * @param fieldName
   * @param filter
   * @param returnIdField must be true if facets are in passed
   * @param distinctOn may be null
   * @param facets for no facets: null or Collections.emptyList()
   * @param queryTimeout query timeout in milliseconds, or 0 for no timeout
   * @return AsyncResult; on success with result {@link PostgresClientStreamResult}
   */
  @SuppressWarnings({"squid:S00107"})    // Method has >7 parameters
  public <T> Future<PostgresClientStreamResult<T>> streamGet(String table, Class<T> clazz, String fieldName,
      CQLWrapper filter, boolean returnIdField, String distinctOn,
      List<FacetField> facets, int queryTimeout) {

    return Future.future(promise -> streamGet(table, clazz, fieldName, filter, returnIdField,
            distinctOn, facets, queryTimeout, promise));
  }

  /**
   * Stream GET with CQLWrapper and facets {@link org.folio.rest.persist.PostgresClientStreamResult}
   * @param <T>
   * @param table
   * @param clazz
   * @param fieldName
   * @param filter
   * @param returnIdField must be true if facets are in passed
   * @param distinctOn may be null
   * @param facets for no facets: null or Collections.emptyList()
   * @param queryTimeout query timeout in milliseconds, or 0 for no timeout
   * @param replyHandler AsyncResult; on success with result {@link PostgresClientStreamResult}
   */
  @SuppressWarnings({"squid:S00107"})    // Method has >7 parameters
  public <T> void streamGet(String table, Class<T> clazz, String fieldName,
      CQLWrapper filter, boolean returnIdField, String distinctOn,
      List<FacetField> facets, int queryTimeout,
      Handler<AsyncResult<PostgresClientStreamResult<T>>> replyHandler) {

    getSQLReadConnection(queryTimeout, conn ->
        streamGet(conn, table, clazz, fieldName, filter, returnIdField,
            distinctOn, facets, closeAtEnd(conn, replyHandler)));
  }

  /**
   * Stream records selected by CQLWrapper.
   *
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - which records to select
   * @param replyHandler AsyncResult; on success with result {@link PostgresClientStreamResult}
   */
  public <T> Future<Void> streamGet(String table, Class<T> clazz, CQLWrapper filter,
      Handler<AsyncResult<PostgresClientStreamResult<T>>> replyHandler) {

    return withReadTrans(conn -> conn.streamGet(table, clazz, filter, replyHandler));
  }

  /**
   * Stream records selected by CQLWrapper.
   *
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - which records to select
   * @param fieldName - database column to return, for example {@link PostgresClient#DEFAULT_JSONB_FIELD_NAME}
   * @param returnIdField - if the id field should also be returned, must be true for facets
   * @param distinctOn - database column to calculate the number of distinct values for, null or empty string for none
   * @param facets - fields to calculate counts for
   * @param replyHandler AsyncResult; on success with result {@link PostgresClientStreamResult}
   */
  public <T> Future<Void> streamGet(String table, Class<T> clazz, CQLWrapper filter, String fieldName,
      boolean returnIdField, String distinctOn, List<FacetField> facets,
      Handler<AsyncResult<PostgresClientStreamResult<T>>> replyHandler) {

    return withReadTrans(conn -> conn.streamGet(table, clazz, fieldName, filter,
        returnIdField, distinctOn, facets, replyHandler));
  }

  /**
   * streamGet with existing transaction/connection
   * @param <T>
   * @param connResult
   * @param table
   * @param clazz
   * @param fieldName
   * @param wrapper
   * @param returnIdField
   * @param distinctOn
   * @param facets
   * @param replyHandler
   */
  @SuppressWarnings({"squid:S00107"})    // Method has >7 parameters
  <T> void streamGet(AsyncResult<SQLConnection> connResult,
      String table, Class<T> clazz, String fieldName, CQLWrapper wrapper,
      boolean returnIdField, String distinctOn, List<FacetField> facets,
      Handler<AsyncResult<PostgresClientStreamResult<T>>> replyHandler) {

    if (connResult.failed()) {
      log.error(connResult.cause().getMessage(), connResult.cause());
      replyHandler.handle(Future.failedFuture(connResult.cause()));
      return;
    }
    doStreamGetCount(connResult.result().conn, connResult.result().tx == null,
        table, clazz, fieldName, wrapper, returnIdField, distinctOn, facets, replyHandler);
  }

  /**
   * private for now, might be public later (and renamed)
   * @param <T>
   * @param connection
   * @param table
   * @param clazz
   * @param fieldName
   * @param wrapper
   * @param returnIdField
   * @param distinctOn
   * @param facets
   * @param replyHandler
   */
  @SuppressWarnings({"squid:S00107"})    // Method has >7 parameters
  <T> void doStreamGetCount(PgConnection connection, boolean startTransaction,
    String table, Class<T> clazz, String fieldName, CQLWrapper wrapper,
    boolean returnIdField, String distinctOn, List<FacetField> facets,
    Handler<AsyncResult<PostgresClientStreamResult<T>>> replyHandler) {

    try {
      QueryHelper queryHelper = buildQueryHelper(table,
        fieldName, wrapper, returnIdField, facets, distinctOn);

      Future<Integer> countQuery;
      if (wrapper == null || wrapper.hasReturnCount()) {
        countQuery = connection.query(queryHelper.countQuery).execute()
            .map(result -> result.iterator().next().getInteger(0));
      } else {
        countQuery = Future.succeededFuture(null);
      }

      countQuery
      .onSuccess(count -> {
        ResultInfo resultInfo = new ResultInfo();
        resultInfo.setTotalRecords(count);
        doStreamGetQuery(connection, startTransaction, queryHelper, resultInfo, clazz, replyHandler);
      })
      .onFailure(e -> {
        log.error(e.getMessage(), e);
        replyHandler.handle(Future.failedFuture(e));
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  private <T> void doStreamGetQuery(PgConnection connection, boolean startTransaction, QueryHelper queryHelper,
      ResultInfo resultInfo, Class<T> clazz, Handler<AsyncResult<PostgresClientStreamResult<T>>> replyHandler) {

    if (! startTransaction) {
      // If a transaction is already running we don't need to close it.
      executeGetQuery(connection, queryHelper, resultInfo, clazz, replyHandler, null);
      return;
    }
    // Start a transaction that we need to close.
    connection.begin()
    .onFailure(cause -> {
      log.error(cause.getMessage(), cause);
      replyHandler.handle(Future.failedFuture(cause));
    }).onSuccess(trans ->
      executeGetQuery(connection, queryHelper, resultInfo, clazz, replyHandler, trans)
    );
  }

  private <T> void executeGetQuery(PgConnection connection, QueryHelper queryHelper,
      ResultInfo resultInfo, Class<T> clazz,
      Handler<AsyncResult<PostgresClientStreamResult<T>>> replyHandler, Transaction transaction) {

    connection.prepare(queryHelper.selectQuery, prepareRes -> {
      if (prepareRes.failed()) {
        closeIfNonNull(transaction).onComplete(ignore -> {
          log.error(prepareRes.cause().getMessage(), prepareRes.cause());
          replyHandler.handle(Future.failedFuture(prepareRes.cause()));
        });
        return;
      }
      PreparedStatement preparedStatement = prepareRes.result();
      RowStream<Row> rowStream = new PreparedRowStream(
          preparedStatement, STREAM_GET_DEFAULT_CHUNK_SIZE, Tuple.tuple());
      PostgresClientStreamResult<T> streamResult = new PostgresClientStreamResult<>(resultInfo);
      doStreamRowResults(rowStream, clazz, transaction, queryHelper,
          streamResult, replyHandler);
    });
  }

  private static List<String> getColumnNames(Row row)
  {
    List<String> columnNames = new ArrayList<>();
    for (int i = 0; row.getColumnName(i) != null; i++) {
      columnNames.add(row.getColumnName(i));
    }
    return columnNames;
  }

  private Future<Void> closeIfNonNull(Transaction transaction) {
    if (transaction == null) {
      return Future.succeededFuture();
    }
    return transaction.commit();
  }

  /**
   * @param transaction the transaction to close, null if not to close
   */
  <T> void doStreamRowResults(RowStream<Row> rowStream, Class<T> clazz,
      Transaction transaction, QueryHelper queryHelper,
      PostgresClientStreamResult<T> streamResult,
      Handler<AsyncResult<PostgresClientStreamResult<T>>> replyHandler) {

    ResultInfo resultInfo = streamResult.resultInfo();
    Promise<PostgresClientStreamResult<T>> promise = Promise.promise();
    ResultsHelper<T> resultsHelper = new ResultsHelper<>(clazz);
    boolean isAuditFlavored = isAuditFlavored(resultsHelper.clazz);
    Map<String, Method> externalColumnSetters = new HashMap<>();
    AtomicInteger resultCount = new AtomicInteger();
    rowStream.handler(r -> {
      try {
        // for first row, get column names
        if (resultsHelper.offset == 0) {
          List<String> columnNames = getColumnNames(r);
          collectExternalColumnSetters(columnNames,
              resultsHelper.clazz, isAuditFlavored, externalColumnSetters);
        }
        @SuppressWarnings("unchecked")
        T objRow = (T) deserializeRow(resultsHelper, externalColumnSetters, isAuditFlavored, r);
        if (!resultsHelper.facet) {
          resultCount.incrementAndGet();
          if (!promise.future().isComplete()) { // end of facets (if any) .. produce result
            resultsHelper.facets.forEach((k, v) -> resultInfo.getFacets().add(v));
            promise.complete(streamResult);
            replyHandler.handle(promise.future());
          }
          streamResult.fireHandler(objRow);
        }
        resultsHelper.offset++;
      } catch (Exception e) {
        streamResult.handler(null);
        log.error(e.getMessage(), e);
        if (!promise.future().isComplete()) {
          promise.complete(streamResult);
          replyHandler.handle(promise.future());
        }
        rowStream.close();
        closeIfNonNull(transaction)
            .onComplete((AsyncResult<Void> voidRes) -> streamResult.fireExceptionHandler(e));
      }
    }).endHandler(v2 -> {
      rowStream.close();
      closeIfNonNull(transaction).onComplete(ignore -> {
        resultInfo.setTotalRecords(
            getTotalRecords(resultCount.get(),
                resultInfo.getTotalRecords(),
                queryHelper.offset, queryHelper.limit));
        try {
          if (!promise.future().isComplete()) {
            promise.complete(streamResult);
            replyHandler.handle(promise.future());
          }
          streamResult.fireEndHandler();
        } catch (Exception ex) {
          streamResult.fireExceptionHandler(ex);
        }
      });
    }).exceptionHandler(e -> {
      rowStream.close();
      closeIfNonNull(transaction).onComplete(ignore -> {
        if (!promise.future().isComplete()) {
          promise.complete(streamResult);
          replyHandler.handle(promise.future());
        }
        streamResult.fireExceptionHandler(e);
      });
    });
  }

  QueryHelper buildQueryHelper(
    String table, String fieldName, CQLWrapper wrapper,
    boolean returnIdField, List<FacetField> facets,
    String distinctOn) throws IOException, TemplateException {

    if (wrapper == null) {
      wrapper = new CQLWrapper();
    }

    String addIdField = "";
    if (returnIdField) {
      addIdField = COMMA + ID_FIELD;
    }

    if (!"null".equals(fieldName) && fieldName.contains("*")) {
      // if we are requesting all fields (*) , then dont add the id field to the select
      // this will return two id columns which will create ambiguity in facet queries
      addIdField = "";
    }

    QueryHelper queryHelper = new QueryHelper(table);

    String countOn = "*";
    String distinctOnClause = "";
    if (distinctOn != null && !distinctOn.isEmpty()) {
      distinctOnClause = String.format("DISTINCT ON(%s) ", distinctOn);
      countOn = String.format("DISTINCT(%s)", distinctOn);
    }
    queryHelper.selectQuery = SELECT + distinctOnClause + fieldName + addIdField
      + FROM + schemaName + DOT + table + SPACE + wrapper.toString();
    queryHelper.countQuery = SELECT + "COUNT(" + countOn + ")"
      + FROM + schemaName + DOT + table + SPACE + wrapper.getWhereClause();

    if (facets != null && !facets.isEmpty()) {
      String mainQuery = SELECT + distinctOnClause + fieldName + addIdField
          + FROM + schemaName + DOT + table + SPACE + wrapper.getWithoutLimOff();

      FacetManager facetManager = buildFacetManager(wrapper, queryHelper, mainQuery, facets);
      // this method call invokes freemarker templating
      queryHelper.selectQuery = facetManager.generateFacetQuery();
    }
    int offset = wrapper.getOffset().get();
    if (offset != -1) {
      queryHelper.offset = offset;
    }
    int limit = wrapper.getLimit().get();
    queryHelper.limit = limit != -1 ? limit : Integer.MAX_VALUE;
    // with where, but without order by, offset, limit
    String query = SELECT + distinctOnClause + fieldName + addIdField
        + FROM + schemaName + DOT + table + SPACE + wrapper.getWhereClause();
    if (limit == 0) {
      // calculate exact total count without returning records
      queryHelper.countQuery = SELECT + "count(*) FROM (" + query + ") x";
    } else if (!wrapper.getWhereClause().isEmpty()) {
      // only do estimation when filter is in use (such as CQL).
      queryHelper.countQuery = SELECT + schemaName + DOT + "count_estimate('"
        + query.replace("'", "''")
        + "')";
    }
    return queryHelper;
  }

  <T> Future<T> processQueryWithCount(
      PgConnection connection, QueryHelper queryHelper, String statMethod,
      Function<TotaledResults, T> resultSetMapper) {
    long start = log.isDebugEnabled() ? System.nanoTime() : 0;

    log.debug("Attempting count query: " + queryHelper.countQuery);
    return connection.query(queryHelper.countQuery).execute()
    .compose(countQueryResult -> {
      log.debug(() -> "timer: get " + queryHelper.countQuery + " " + (System.nanoTime() - start) + " ns");
      int estimatedTotal = countQueryResult.iterator().next().getInteger(0);
      return Future.<T>future(promise -> processQuery(connection, queryHelper, estimatedTotal, statMethod, resultSetMapper, promise));
    })
    .onFailure(e -> log.error("query with count: {} - {}", e.getMessage(), queryHelper.countQuery, e));
  }

  <T> void processQuery(
    PgConnection connection, QueryHelper queryHelper, Integer estimatedTotal, String statMethod,
    Function<TotaledResults, T> resultSetMapper, Handler<AsyncResult<T>> replyHandler
  ) {
    try {
      queryAndAnalyze(connection, queryHelper.selectQuery, statMethod, query -> {
        if (query.failed()) {
          replyHandler.handle(Future.failedFuture(query.cause()));
          return;
        }
        replyHandler.handle(Future.succeededFuture(resultSetMapper.apply(new TotaledResults(query.result(), estimatedTotal))));
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  private FacetManager buildFacetManager(CQLWrapper wrapper, QueryHelper queryHelper,
    String mainQuery, List<FacetField> facets) {

    FacetManager fm = new FacetManager(schemaName + DOT + queryHelper.table);
    if (wrapper.getWhereClause().isEmpty()) {
      fm.setWhere(" " + wrapper.getWhereClause());
    }
    fm.setSupportFacets(facets);
    fm.setIdField(ID_FIELD);
    fm.setLimitClause(wrapper.getLimit().toString());
    fm.setOffsetClause(wrapper.getOffset().toString());
    fm.setMainQuery(mainQuery);
    fm.setSchema(schemaName);
    fm.setCountQuery(queryHelper.countQuery);
    return fm;
  }

  /**
   * Returns records selected by {@link Criterion} filter.
   *
   * <p>Doesn't calculate totalRecords, the number of matching records when disabling OFFSET and LIMIT.
   *
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - which records to select
   * @return {@link Results} with the entities found
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz, Criterion filter) {
    return withReadConn(conn -> conn.get(table, clazz, filter));
  }

  /**
   * Returns records selected by {@link Criterion} filter.
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - which records to select
   * @param returnCount - whether to return totalRecords, the number of matching records
   *         when disabling OFFSET and LIMIT
   * @return {@link Results} with the entities found
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz, Criterion filter, boolean returnCount) {
    return withReadConn(conn -> conn.get(table, clazz, filter, returnCount));
  }

  /**
   * select query
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - see Criterion class
   * @param returnCount - whether to return the amount of records matching the query
   * @param replyHandler
   */
  public <T> void get(String table, Class<T> clazz, Criterion filter, boolean returnCount,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, filter, returnCount).onComplete(replyHandler);
  }

  /**
   * @param setId - unused, the database trigger will always set jsonb->'id' automatically
   */
  public <T> void get(String table, Class<T> clazz, String[] fields, CQLWrapper filter,
      boolean returnCount, boolean setId,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, fields, filter, returnCount, setId, null /*facets*/, replyHandler);
  }

  /**
   * Return records that match the {@link CQLWrapper} filter.
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - which records to match
   * @param returnCount - whether to calculate totalRecords
   *            (the number of records that match when disabling offset and limit)
   * @param setId - unused, the database trigger will always set jsonb->'id' automatically
   * @param facets - fields to calculate counts for
   * @return {@link Results} with the entities found
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz, String[] fields, CQLWrapper filter,
    boolean returnCount, boolean setId, List<FacetField> facets) {

    return Future.future(promise -> get(table, clazz, fields, filter, returnCount, setId, facets, promise));
  }

  /**
   * @param setId - unused, the database trigger will always set jsonb->'id' automatically
   */
  public <T> void get(String table, Class<T> clazz, String[] fields, CQLWrapper filter,
    boolean returnCount, boolean setId, List<FacetField> facets,
    Handler<AsyncResult<Results<T>>> replyHandler) {

    get(table, clazz, fields, filter, returnCount, true, facets, null, replyHandler);
  }

  <T> void get(String table, Class<T> clazz, String[] fields, CQLWrapper filter,
    boolean returnCount, boolean returnIdField, List<FacetField> facets, String distinctOn,
    Handler<AsyncResult<Results<T>>> replyHandler) {

    String fieldsStr = Arrays.toString(fields);
    String fieldName = fieldsStr.substring(1, fieldsStr.length() - 1);
    get(table, clazz, fieldName, filter, returnCount, returnIdField, facets, distinctOn, replyHandler);
  }
/**
 * Return records that match the {@link CQLWrapper} filter using the readonly PgConnection
 * @param table - table to query
 * @param clazz - class of objects to be returned
 * @param fieldName - fieldnName to query
 * @param filter - which records to match
 * @param returnCount - whether to calculate totalRecords
 *            (the number of records that match when disabling offset and limit)
 * @param returnIdField -
 * @param facets
 * @param distinctOn
 * @param replyHandler - return {@link Results} with the entities found
 **/
  <T> void get(String table, Class<T> clazz, String fieldName, CQLWrapper filter,
    boolean returnCount, boolean returnIdField, List<FacetField> facets, String distinctOn,
    Handler<AsyncResult<Results<T>>> replyHandler) {

    withReadConn(conn
      -> conn.get(table, clazz, fieldName, filter, returnCount, returnIdField, facets, distinctOn))
    .onComplete(replyHandler);
  }

  /**
   * Return records that match the {@link CQLWrapper} filter.
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param fields - the fields to return, for example {@link #DEFAULT_JSONB_FIELD_NAME}
   * @param filter - which records to match
   * @param returnCount - whether to calculate totalRecords
   *            (the number of records that match when disabling offset and limit)
   * @return {@link Results} with the entities found
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz, String[] fields, CQLWrapper filter,
      boolean returnCount) {
    return Future.future(promise -> get(table, clazz, fields, filter, returnCount, promise));
  }

  public <T> void get(String table, Class<T> clazz, String[] fields, CQLWrapper filter,
      boolean returnCount, Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, fields, filter, returnCount, false /* setId */, replyHandler);
  }

  /**
   * Return records that match the {@link CQLWrapper} filter.
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - which records to match
   * @param returnCount - whether to calculate totalRecords
   *            (the number of records that match when disabling offset and limit)
   * @return {@link Results} with the entities found
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz, CQLWrapper filter, boolean returnCount) {
    return withConn(conn -> conn.get(table, clazz, filter, returnCount));
  }

  /**
   * Return records that match the {@link CQLWrapper} filter.
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - which records to match
   * @param returnCount - whether to calculate totalRecords
   *            (the number of records that match when disabling offset and limit)
   * @param replyHandler - returns {@link Results} with the entities found
   */
  /* PGUTIL USED VERSION */
  public <T> void get(String table, Class<T> clazz, CQLWrapper filter, boolean returnCount,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, new String[]{DEFAULT_JSONB_FIELD_NAME}, filter, returnCount, false /*setId*/, replyHandler);
  }

  /**
   * @param setId - unused, the database trigger will always set jsonb->'id' automatically
   * @deprecated use {@link #get(String, Class, CQLWrapper, boolean, Handler)} instead.
   */
  @Deprecated
  public <T> void get(String table, Class<T> clazz, CQLWrapper filter, boolean returnCount, boolean setId,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, new String[]{DEFAULT_JSONB_FIELD_NAME}, filter, returnCount, setId, replyHandler);
  }

  /**
   * Return records that match the {@link CQLWrapper} filter.
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - which records to match
   * @param returnCount - whether to calculate totalRecords
   *            (the number of records that match when disabling offset and limit)
   * @param facets - fields to calculate counts for
   * @return {@link Results} with the entities found
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz, CQLWrapper filter,
      boolean returnCount, List<FacetField> facets) {
    return Future.future(promise -> get(table, clazz, filter, returnCount, facets, promise));
  }

  public <T> void get(String table, Class<T> clazz, CQLWrapper filter,
      boolean returnCount, List<FacetField> facets,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, new String[]{DEFAULT_JSONB_FIELD_NAME}, filter, returnCount, false /* setId */, facets, replyHandler);
  }

  /**
   * @param setId - unused, the database trigger will always set jsonb->'id' automatically
   * @deprecated use {@link #get(String, Class, CQLWrapper, boolean, List, Handler)} instead.
   */
  @Deprecated
  public <T> void get(String table, Class<T> clazz, CQLWrapper filter,
      boolean returnCount, boolean setId, List<FacetField> facets,
      Handler<AsyncResult<Results<T>>> replyHandler) {
    get(table, clazz, filter, returnCount, facets, replyHandler);
  }

  /**
   * Return records that match the {@link Criterion} filter using the readonly PgConnection
   * @param setId - unused, the database trigger will always set jsonb->'id' automatically
   */
  public <T> void get(String table, Class<T> clazz, Criterion filter, boolean returnCount, boolean setId,
      Handler<AsyncResult<Results<T>>> replyHandler) {

    withReadConn(conn -> conn.get(table, clazz, filter, returnCount))
    .onComplete(replyHandler);
  }

  /**
   * @param setId - unused, the database trigger will always set jsonb->'id' automatically
   */
  public <T> void get(AsyncResult<SQLConnection> sqlConnection, String table, Class<T> clazz, Criterion filter,
    boolean returnCount, boolean setId,
      Handler<AsyncResult<Results<T>>> replyHandler) {

    if (sqlConnection == null) {
      get(table, clazz, filter, returnCount, replyHandler);
      return;
    }

    withConn(sqlConnection, conn -> conn.get(table, clazz, filter, returnCount))
    .onComplete(replyHandler);
  }

  /**
   * Returns records selected by {@link Criterion} filter
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - see Criterion class
   * @param returnCount - whether to return the amount of records matching the query
   * @param setId - unused, the database trigger will always set jsonb->'id' automatically
   * @param facets - fields to calculate counts for
   */
  public <T> Future<Results<T>> get(String table, Class<T> clazz, Criterion filter, boolean returnCount, boolean setId,
      List<FacetField> facets) {
    return Future.future(promise -> get(null, table, clazz, filter, returnCount, setId, facets, promise));
  }

  /**
   * select query
   * @param table - table to query
   * @param clazz - class of objects to be returned
   * @param filter - see Criterion class
   * @param returnCount - whether to return the amount of records matching the query
   * @param setId - unused, the database trigger will always set jsonb->'id' automatically
   * @param facets - fields to calculate counts for
   */
  public <T> void get(String table, Class<T> clazz, Criterion filter, boolean returnCount, boolean setId,
      List<FacetField> facets, Handler<AsyncResult<Results<T>>> replyHandler) {

    get(null, table, clazz, filter, returnCount, setId, facets, replyHandler);
  }

  /**
   * Returns records selected by {@link Criterion} filter.
   * @param setId - unused, the database trigger will always set jsonb->'id' automatically
   */
  @SuppressWarnings({"squid:S00107"})   // Method has more than 7 parameters
  public <T> void get(AsyncResult<SQLConnection> sqlConnection, String table, Class<T> clazz,
    Criterion filter, boolean returnCount, boolean setId,
    List<FacetField> facets, Handler<AsyncResult<Results<T>>> replyHandler) {

    if (sqlConnection == null) {
      withReadConn(conn -> conn.get(table, clazz, filter, returnCount, facets))
      .onComplete(replyHandler);
      return;
    }

    withConn(sqlConnection, conn -> conn.get(table, clazz, filter, returnCount, facets))
    .onComplete(replyHandler);
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
   * Get the jsonb by id using the readonly connection
   * @param conn  if provided, the connection to use;
   *              if null, a new connection is created and automatically released afterwards;
   *              if failed, the replyHandler is failed
   * @param lock  whether to use SELECT FOR UPDATE to lock the selected row
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param function  how to convert the (String encoded) JSON
   * @param replyHandler  the result after applying function
   */
  private <R> void getById(final AsyncResult<SQLConnection> conn, boolean lock, String table, String id,
                           FunctionWithException<String, R, Exception> function,
                           Handler<AsyncResult<R>> replyHandler) {
    Promise<PgConnection> promise = Promise.promise();
    if (conn != null) {
      promise.handle(conn.map(sqlConnection -> sqlConnection.conn));
    } else {
      getReadConnection(promise);
    }
    promise.future()
    .compose(connection -> new Conn(this, connection)
        .getById(lock, table, id, function)
        .onComplete(x -> {
          if (conn == null) {
            connection.close();
          }
        })
    )
    .onComplete(replyHandler);
  }

  /**
   * Get the jsonb by id with the readonly connection and return it as a String
   * @param table  the table to search in
   * @param id  the value of the id field
   * @return the JSON encoded as a String
   */
  public Future<String> getByIdAsString(String table, String id) {
    return withReadConn(conn -> conn.getByIdAsString(table, id));
  }

  /**
   * Get the jsonb by id and return it as a String.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param replyHandler  the result; the JSON is encoded as a String
   */
  public void getByIdAsString(String table, String id, Handler<AsyncResult<String>> replyHandler) {
    getByIdAsString(table, id).onComplete(replyHandler);
  }

  /**
   * Get the jsonb by id and return it as a String.
   * @param conn  if provided, the connection on which to execute the query on.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param replyHandler  the result; the JSON is encoded as a String
   */
  public void getByIdAsString(AsyncResult<SQLConnection> conn,
      String table, String id, Handler<AsyncResult<String>> replyHandler) {
    getById(conn, false, table, id, string -> string, replyHandler);
  }

  /**
   * Lock the row using <code>select ... for update</code> and return jsonb as String.
   * @param conn  if provided, the connection on which to execute the query on.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param replyHandler  the result; the JSON is encoded as a String
   */
  public void getByIdAsStringForUpdate(AsyncResult<SQLConnection> conn,
      String table, String id, Handler<AsyncResult<String>> replyHandler) {
    getById(conn, true, table, id, string -> string, replyHandler);
  }

  /**
   * Get the jsonb by id and return it as a JsonObject.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @return the JSON is encoded as a JsonObject
   */
  public Future<JsonObject> getById(String table, String id) {
    return withReadConn(conn -> conn.getById(table, id));
  }

  /**
   * Get the jsonb by id and return it as a JsonObject.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param replyHandler  the result; the JSON is encoded as a JsonObject
   */
  public void getById(String table, String id, Handler<AsyncResult<JsonObject>> replyHandler) {
    getById(table, id).onComplete(replyHandler);
  }

  /**
   * Get the jsonb by id and return it as a JsonObject.
   * @param conn  if provided, the connection on which to execute the query on.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param replyHandler  the result; the JSON is encoded as a JsonObject
   */
  public void getById(AsyncResult<SQLConnection> conn,
      String table, String id, Handler<AsyncResult<JsonObject>> replyHandler) {
    getById(conn, false, table, id, JsonObject::new, replyHandler);
  }

  /**
   * Lock the row using <code>select ... for update</code> and return jsonb as a JsonObject.
   * @param conn  if provided, the connection on which to execute the query on.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param replyHandler  the result; the JSON is encoded as a JsonObject
   */
  public void getByIdForUpdate(AsyncResult<SQLConnection> conn,
      String table, String id, Handler<AsyncResult<JsonObject>> replyHandler) {
    getById(conn, true, table, id, JsonObject::new, replyHandler);
  }

  /**
   * Get the jsonb by id with the readonly connectino and return it as a pojo of type T.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param clazz  the type of the pojo
   * @return the JSON converted into a T pojo.
   */
  public <T> Future<T> getById(String table, String id, Class<T> clazz) {
    return withReadConn(conn -> conn.getById(table, id, clazz));
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
    getById(table, id, clazz).onComplete(replyHandler);
  }

  /**
   * Get the jsonb by id and return it as a pojo of type T.
   * @param conn  if provided, the connection on which to execute the query on.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param clazz  the type of the pojo
   * @param replyHandler  the result; the JSON is converted into a T pojo.
   */
  public <T> void getById(AsyncResult<SQLConnection> conn,
      String table, String id, Class<T> clazz, Handler<AsyncResult<T>> replyHandler) {
    getById(conn, false, table, id, json -> MAPPER.readValue(json, clazz), replyHandler);
  }

  /**
   * Lock the row using <code>select ... for update</code> and return jsonb as a pojo of type T.
   * @param conn  if provided, the connection on which to execute the query on.
   * @param table  the table to search in
   * @param id  the value of the id field
   * @param clazz  the type of the pojo
   * @param replyHandler  the result; the JSON is converted into a T pojo.
   */
  public <T> void getByIdForUpdate(AsyncResult<SQLConnection> conn,
      String table, String id, Class<T> clazz, Handler<AsyncResult<T>> replyHandler) {
    getById(conn, true, table, id, json -> MAPPER.readValue(json, clazz), replyHandler);
  }

  /**
   * Get jsonb by id with the readonly connection for a list of ids.
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
    getReadConnection(res -> {
      if (res.failed()) {
        replyHandler.handle(Future.failedFuture(res.cause()));
        return;
      }
      Tuple list = Tuple.tuple();
      for (int i = 0; i < ids.size(); i++) {
        list.addUUID(UUID.fromString(ids.getString(i)));
      }

      PgConnection connection = res.result();
      StringBuilder sql = new StringBuilder()
          .append(SELECT).append(ID_FIELD).append(", ").append(DEFAULT_JSONB_FIELD_NAME)
          .append(FROM).append(schemaName).append(DOT).append(table)
          .append(WHERE).append(ID_FIELD).append(" IN ($1");
      for (int i = 2; i <= ids.size(); i++) {
        sql.append(", $" + i);
      }
      sql.append(")");
      connection.preparedQuery(sql.toString()).execute(list, query -> {
        connection.close();
        if (query.failed()) {
          replyHandler.handle(Future.failedFuture(query.cause()));
          return;
        }
        try {
          Map<String,R> result = new HashMap<>();
          Iterator<Row> iterator = query.result().iterator();
          while (iterator.hasNext()) {
            Row row = iterator.next();
            result.put(row.getValue(0).toString(), function.apply(row.getValue(1).toString()));
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
    getById(table, ids, json -> MAPPER.readValue(json, clazz), replyHandler);
  }

  static class ResultsHelper<T> {
    final List<T> list;
    final Map<String, org.folio.rest.jaxrs.model.Facet> facets;
    final RowSet<Row> resultSet;
    final Class<T> clazz;
    Integer total;
    int offset;
    boolean facet;
    public ResultsHelper(RowSet<Row> resultSet, Integer total, Class<T> clazz) {
      this.list = new ArrayList<>();
      this.facets = new HashMap<>();
      this.resultSet = resultSet;
      this.clazz= clazz;
      this.total = total;
      this.offset = 0;
    }
    public ResultsHelper(Class<T> clazz) {
      this.list = new ArrayList<>();
      this.facets = new HashMap<>();
      this.resultSet = null;
      this.clazz= clazz;
      this.offset = 0;
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
   * @return
   */
  <T> Results<T> processResults(RowSet<Row> rs, Integer total, int offset, int limit, Class<T> clazz) {
    long start = System.nanoTime();

    ResultsHelper<T> resultsHelper = new ResultsHelper<>(rs, total, clazz);

    deserializeResults(resultsHelper);

    ResultInfo resultInfo = new ResultInfo();
    resultsHelper.facets.forEach((k , v) -> resultInfo.getFacets().add(v));
    Integer totalRecords = getTotalRecords(resultsHelper.list.size(),
        resultsHelper.total, offset, limit);
    resultInfo.setTotalRecords(totalRecords);

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

    if (resultsHelper.resultSet == null) {
      return;
    }
    boolean isAuditFlavored = isAuditFlavored(resultsHelper.clazz);

    Map<String, Method> externalColumnSetters = new HashMap<>();
    collectExternalColumnSetters(
        resultsHelper.resultSet.columnsNames(),
        resultsHelper.clazz,
        isAuditFlavored,
        externalColumnSetters
    );
    RowIterator<Row> iterator = resultsHelper.resultSet.iterator();
    while (iterator.hasNext()) {
      Row row = iterator.next();
      try {
        T objRow = (T) deserializeRow(resultsHelper, externalColumnSetters, isAuditFlavored, row);
        if (!resultsHelper.facet) {
          resultsHelper.list.add(objRow);
        }
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        resultsHelper.list.add(null);
      }
    }
  }

  /**
   *
   * @param resultsHelper
   * @param externalColumnSetters
   * @param isAuditFlavored
   * @param row
   */
  <T> Object deserializeRow(
    ResultsHelper<T> resultsHelper, Map<String, Method> externalColumnSetters,
    boolean isAuditFlavored, Row row
  ) throws IOException, InstantiationException, IllegalAccessException, InvocationTargetException {
    int columnIndex = row.getColumnIndex(DEFAULT_JSONB_FIELD_NAME);
    Object jo = columnIndex == -1 ? null : row.getValue(columnIndex);
    Object o;
    resultsHelper.facet = false;

    if (!isAuditFlavored && jo != null) {
      try {
        // is this a facet entry - if so process it, otherwise will throw an exception
        // and continue trying to map to the pojos
        o =  MAPPER.readValue(jo.toString(), org.folio.rest.jaxrs.model.Facet.class);
        org.folio.rest.jaxrs.model.Facet of = (org.folio.rest.jaxrs.model.Facet) o;
        org.folio.rest.jaxrs.model.Facet facet = resultsHelper.facets.get(of.getType());
        if (facet == null) {
          resultsHelper.facets.put(of.getType(), of);
        } else {
          facet.getFacetValues().add(of.getFacetValues().get(0));
        }
        resultsHelper.facet = true;
        return o;
      } catch (Exception e) {
        o = MAPPER.readValue(jo.toString(), resultsHelper.clazz);
      }
    } else {
      o = resultsHelper.clazz.newInstance();
    }

    populateExternalColumns(externalColumnSetters, o, row);

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
   * @param externalColumnSetters
   */
  <T> void collectExternalColumnSetters(List<String> columnNames, Class<T> clazz, boolean isAuditFlavored,
                                        Map<String, Method> externalColumnSetters) {
    for (String columnName : columnNames) {
      if ((isAuditFlavored || !columnName.equals(DEFAULT_JSONB_FIELD_NAME)) && !columnName.equals(ID_FIELD)) {
        String methodName = databaseFieldToPojoSetter(columnName);
        for (Method method : clazz.getMethods()) {
          if (method.getName().equals(methodName)) {
            externalColumnSetters.put(columnName, method);
          }
        }
      }
    }
  }

  private boolean isStringArrayType(Object value) {
    // https://github.com/eclipse-vertx/vertx-sql-client/blob/4.0.0.CR1/vertx-sql-client/src/main/java/io/vertx/sqlclient/Tuple.java#L910
    return value instanceof String[] ||
        value instanceof Enum[] ||
        (value != null && value.getClass() == Object[].class);
  }

  /**
   * populate jsonb object with values from external columns - for example:
   * if there is an update_date column in the record - try to populate a field updateDate in the
   * jsonb object - this allows to use the DB for things like triggers to populate the update_date
   * automatically, but still push them into the jsonb object - the json schema must declare this field
   * as well - also support the audit mode descrbed above.
   * NOTE: that the query must request any field it wants to get populated into the jsonb obj
   *
   * @param externalColumnSetters
   * @param o
   * @param row
   */
  void populateExternalColumns(Map<String, Method> externalColumnSetters, Object o, Row row)
      throws InvocationTargetException, IllegalAccessException {
    for (Map.Entry<String, Method> entry : externalColumnSetters.entrySet()) {
      String columnName = entry.getKey();
      Method method = entry.getValue();
      int columnIndex = row.getColumnIndex(columnName);
      Object value = columnIndex == -1 ? null : row.getValue(columnIndex);
      if (isStringArrayType(value)) {
        method.invoke(o, Arrays.asList(row.getArrayOfStrings(columnIndex)));
      } else {
        method.invoke(o, value);
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
  public void select(String sql, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    getSQLConnection(conn -> select(conn, sql, closeAndHandleResult(conn, replyHandler)));
  }

  /**
   * Run a select query.
   *
   * <p>To update see {@link #execute(String, Handler)}.
   *
   * @param sql - the sql query to run
   * @return future result
   */
  public Future<RowSet<Row>> select(String sql) {
    return Future.future(promise -> select(sql, promise));
  }

  /**
   * Run an SQL statement that updates data and returns data,
   * for example {@code SELECT nextval('foo')} or {@code UPDATE ... RETURNING ...}.
   *
   * @see #selectRead(String, int, Handler)
   * @see #execute(String, Handler)
   * @param sql - the sql query to run
   * @param queryTimeout query timeout in milliseconds, or 0 for no timeout
   * @param replyHandler the query result or the failure
   */
  public void select(String sql, int queryTimeout, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    getSQLConnection(queryTimeout,
        conn -> select(conn, sql, closeAndHandleResult(conn, replyHandler))
        );
  }

  /**
   * Execute a read-only SQL statement that returns data.
   * @see #select(String, int, Handler)
   * @see #execute(String, Handler)
   * @param sql - the sql query to run
   * @param queryTimeout query timeout in milliseconds, or 0 for no timeout
   * @param replyHandler the query result or the failure
   */
  public void selectRead(String sql, int queryTimeout, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    getSQLReadConnection(queryTimeout,
        conn -> select(conn, sql, closeAndHandleResult(conn, replyHandler))
    );
  }

  static void queryAndAnalyze(PgConnection conn, String sql, String statMethod,
    Handler<AsyncResult<RowSet<Row>>> replyHandler) {

    long start = System.nanoTime();
    conn.query(sql).execute(res -> {
      long queryTime = (System.nanoTime() - start);
      if (res.failed()) {
        log.error("queryAndAnalyze: " + res.cause().getMessage() + " - "
          + sql, res.cause());
        replyHandler.handle(Future.failedFuture(res.cause()));
        return;
      }
      if (queryTime >= explainQueryThreshold * 1000000) {
        final String explainQuery = "EXPLAIN ANALYZE " + sql;
        conn.query(explainQuery).execute(explain -> {
          replyHandler.handle(res); // not before, so we have conn if it gets closed
          if (explain.failed()) {
            log.warn(explainQuery + ": ", explain.cause().getMessage(), explain.cause());
            return;
          }
          StringBuilder e = new StringBuilder(explainQuery);
          RowIterator<Row> iterator = explain.result().iterator();
          while (iterator.hasNext()) {
            Row row = iterator.next();
            e.append('\n').append(row.getValue(0));
          }
          log.warn(e.toString());
        });
      } else {
        replyHandler.handle(res);
      }
    });
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
  public void select(AsyncResult<SQLConnection> conn, String sql, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    try {
      if (conn.failed()) {
        replyHandler.handle(Future.failedFuture(conn.cause()));
        return;
      }
      queryAndAnalyze(conn.result().conn, sql, GET_STAT_METHOD, replyHandler);
    } catch (Exception e) {
      log.error("select sql: " + e.getMessage() + " - " + sql, e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Run an prepared/parameterized SQL statement that updates data and returns data,
   * for example {@code SELECT nextval($1)} or {@code UPDATE ... RETURNING ...}.
   *
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   * @see #selectRead(String, Tuple, Handler)
   * @see #execute(String, Tuple, Handler)
   */
  public void select(String sql, Tuple params, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    getSQLConnection(conn -> select(conn, sql, params, closeAndHandleResult(conn, replyHandler)));
  }

  /**
   * Run a parameterized/prepared select query using the readonly connection.
   *
   * <p>To update see {@link #execute(String, Tuple, Handler)}.
   *
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   */
  public void selectRead(String sql, Tuple params, Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    getSQLReadConnection(conn -> select(conn, sql, params, closeAndHandleResult(conn, replyHandler)));
  }

  /**
   * Run a parameterized/prepared select query.
   *
   * <p>This never closes the connection conn.
   *
   * <p>To update see {@link #execute(AsyncResult, String, Tuple, Handler)}.
   *
   * @param conn  The connection on which to execute the query on.
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   */
  public void select(AsyncResult<SQLConnection> conn, String sql, Tuple params,
      Handler<AsyncResult<RowSet<Row>>> replyHandler) {
    try {
      if (conn.failed()) {
        replyHandler.handle(Future.failedFuture(conn.cause()));
        return;
      }
      conn.result().conn.preparedQuery(sql).execute(params, replyHandler);
    } catch (Exception e) {
      log.error("select sql: " + e.getMessage() + " - " + sql, e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Run an SQL statement that may write data and returns data,
   * for example {@code SELECT nextval('foo')} or {@code UPDATE ... RETURNING ...}.
   *
   * @param sql  The sql query to run.
   * @param replyHandler  The query result or the failure.
   * @see #selectSingleRead(String, Handler)
   */
  public void selectSingle(String sql, Handler<AsyncResult<Row>> replyHandler) {
    getSQLConnection(conn -> selectSingle(conn, sql, closeAndHandleResult(conn, replyHandler)));
  }

  /**
   * @param sql  The sql query to run.
   * @param replyHandler  The query result or the failure.
   * @see #selectSingle(String, Handler)
   */
  public void selectSingleRead(String sql, Handler<AsyncResult<Row>> replyHandler) {
    getSQLReadConnection(conn -> selectSingle(conn, sql, closeAndHandleResult(conn, replyHandler)));
  }

  /**
   * Run a select query and return the first record, or null if there is no result.
   *
   * <p>To update see {@link #execute(String, Handler)}.
   *
   * @param sql  The sql query to run.
   * @return future
   */
  public Future<Row> selectSingle(String sql) {
    return selectSingle(sql, Tuple.tuple());
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
  public void selectSingle(AsyncResult<SQLConnection> conn, String sql, Handler<AsyncResult<Row>> replyHandler) {
    selectSingle(conn, sql, Tuple.tuple(), replyHandler);
  }

  /**
   * Run an SQL statement that may write data and returns data, for example
   * {@code SELECT nextval('foo')} or {@code UPDATE ... RETURNING ...}.
   * The first record is returned, or null if there is no result.
   *
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   * @see #selectSingleRead(String, Tuple, Handler)
   */
  public void selectSingle(String sql, Tuple params, Handler<AsyncResult<Row>> replyHandler) {
    getSQLConnection(conn -> selectSingle(conn, sql, params, closeAndHandleResult(conn, replyHandler)));
  }

  /**
   * Run a parameterized/prepared select query with the readonly connection
   * and return the first record, or null if there is no result.
   *
   * @see #selectSingle(String, Tuple, Handler)
   *
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   */
  public void selectSingleRead(String sql, Tuple params, Handler<AsyncResult<Row>> replyHandler) {
    getSQLReadConnection(conn -> selectSingle(conn, sql, params, closeAndHandleResult(conn, replyHandler)));
  }

  /**
   * Run a parameterized/prepared select query and return the first record, or null if there is no result.
   *
   * @param sql The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @return future.
   */
  public Future<Row> selectSingle(String sql, Tuple params) {
    return Future.future(promise -> selectSingle(sql, params, promise));
  }

  static void selectReturn(AsyncResult<RowSet<Row>> res, Handler<AsyncResult<Row>> replyHandler) {
    if (res.failed()) {
      replyHandler.handle(Future.failedFuture(res.cause()));
      return;
    }
    try {
      if (!res.result().iterator().hasNext()) {
        replyHandler.handle(Future.succeededFuture(null));
        return;
      }
      replyHandler.handle(Future.succeededFuture(res.result().iterator().next()));
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
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
  public void selectSingle(AsyncResult<SQLConnection> conn, String sql, Tuple params,
                           Handler<AsyncResult<Row>> replyHandler) {
    try {
      if (conn.failed()) {
        replyHandler.handle(Future.failedFuture(conn.cause()));
        return;
      }
      if (params.size() == 0) {
        conn.result().conn.query(sql).execute(res -> selectReturn(res, replyHandler));
      } else {
        conn.result().conn.preparedQuery(sql).execute(params, res -> selectReturn(res, replyHandler));
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Get a stream of the results of the {@code sql} query.
   *
   * Sample usage:
   *
   * <pre>
   * postgresClient.selectStream("SELECT i FROM numbers WHERE i > $1", Tuple.tuple(5), 100,
   *         rowStream -> rowStream.handler(row -> task.process(row))))
   * .compose(x -> ...
   * </pre>
   *
   * @param params arguments for {@code $} placeholders in {@code sql}
   * @param chunkSize cursor fetch size
   */
  public Future<Void> selectStream(String sql, Tuple params, int chunkSize, Handler<RowStream<Row>> rowStreamHandler) {
    return withTrans(trans -> trans.selectStream(sql, params, chunkSize, rowStreamHandler));
  }

  /**
   * Get a stream of the results of the {@code sql} read-only query (no nextval(), no UPDATE, etc..)
   * using the readonly connection.
   *
   * Sample usage:
   *
   * <pre>
   * postgresClient.selectReadStream("SELECT i FROM numbers WHERE i > $1", Tuple.tuple(5), 100,
   *         rowStream -> rowStream.handler(row -> task.process(row))))
   * .compose(x -> ...
   * </pre>
   *
   * @param params arguments for {@code $} placeholders in {@code sql}
   * @param chunkSize cursor fetch size
   */
  public Future<Void> selectReadStream(String sql, Tuple params, int chunkSize, Handler<RowStream<Row>> rowStreamHandler) {
    return withReadTrans(trans -> trans.selectStream(sql, params, chunkSize, rowStreamHandler));
  }

  /**
   * Get a stream of the results of the {@code sql} query.
   *
   * The chunk size is {@link PostgresClient#STREAM_GET_DEFAULT_CHUNK_SIZE}.
   *
   * Sample usage:
   *
   * <pre>
   * postgresClient.selectStream("SELECT i FROM numbers WHERE i > $1", Tuple.tuple(5),
   *         rowStream -> rowStream.handler(row -> task.process(row))))
   * .compose(x -> ...
   * </pre>
   *
   * @param params arguments for {@code $} placeholders in {@code sql}
   */
  public Future<Void> selectStream(String sql, Tuple params, Handler<RowStream<Row>> rowStreamHandler) {
    return withTrans(trans -> trans.selectStream(sql, params, rowStreamHandler));
  }

  /**
   * Run a parameterized/prepared select query returning with a {@link RowStream<Row>}.
   *
   * <p>This never closes the connection conn.
   *
   * <p>Always call {@link RowStream#close()} or {@link RowStream#close(Handler)}
   * to release the underlying prepared statement.
   *
   * @param conn  The connection on which to execute the query on.
   * @param sql  The sql query to run.
   * @param replyHandler  The query result or the failure.
   */
  public void selectStream(AsyncResult<SQLConnection> conn, String sql,
      Handler<AsyncResult<RowStream<Row>>> replyHandler) {
    selectStream(conn, sql, Tuple.tuple(), replyHandler);
  }

  /**
   * Run a parameterized/prepared select query returning with a {@link RowStream<Row>}.
   *
   * <p>This never closes the connection conn.
   *
   * <p>Always call {@link RowStream#close()} or {@link RowStream#close(Handler)}
   * to release the underlying prepared statement.
   *
   * @param conn  The connection on which to execute the query on.
   * @param sql  The sql query to run.
   * @param params  The parameters for the placeholders in sql.
   * @param replyHandler  The query result or the failure.
   */
  public void selectStream(AsyncResult<SQLConnection> conn, String sql, Tuple params,
                           Handler<AsyncResult<RowStream<Row>>> replyHandler) {
     selectStream(conn, sql, params, STREAM_GET_DEFAULT_CHUNK_SIZE, replyHandler);
  }

  /**
   * Always call {@link RowStream#close()} or {@link RowStream#close(Handler)}
   * to release the underlying prepared statement.
   */
  void selectStream(AsyncResult<SQLConnection> sqlConnection, String sql, Tuple params, int chunkSize,
      Handler<AsyncResult<RowStream<Row>>> replyHandler) {

    withConn(sqlConnection, conn -> conn.selectStream(sql, params, chunkSize, rowStream -> {
      replyHandler.handle(Future.succeededFuture(rowStream));
    })).onFailure(t -> replyHandler.handle(Future.failedFuture(t)));
  }

  /**
   * Execute an INSERT, UPDATE or DELETE statement.
   * @param sql - the sql to run
   * @param replyHandler - the result handler with UpdateResult
   */
  public void execute(String sql, Handler<AsyncResult<RowSet<Row>>> replyHandler)  {
    execute(sql, Tuple.tuple(), replyHandler);
  }

  /**
   * Execute an INSERT, UPDATE or DELETE statement.
   * @param sql - the sql to run
   * @return future result
   */
  public Future<RowSet<Row>> execute(String sql) {
    return execute(sql, Tuple.tuple());
  }

  /**
   * Get vertx-pg-client connection using the Writer client
   */
  public Future<PgConnection> getConnection() {
    return getConnection(getClient())
        .recover(e -> {
          if (! "Timeout".equals(e.getMessage())) {
            return Future.failedFuture(e);
          }
          return Future.failedFuture("Timeout for DB_HOST:DB_PORT="
              + postgreSQLClientConfig.getString(HOST) + ":"
              + postgreSQLClientConfig.getString(PORT));
        });
  }

  /**
   * Get read-only vertx-pg-client connection using the Reader client
   */
  public Future<PgConnection> getReadConnection() {
    return getConnection(getReaderClient())
        .recover(e -> {
          if (! "Timeout".equals(e.getMessage())) {
            return Future.failedFuture(e);
          }
          if (postgreSQLClientConfig.containsKey(HOST_READER) &&
              postgreSQLClientConfig.containsKey(PORT_READER)) {
            return Future.failedFuture("Timeout for DB_HOST_READER:DB_PORT_READER="
                + postgreSQLClientConfig.getString(HOST_READER) + ":"
                + postgreSQLClientConfig.getString(PORT_READER));
          }
          return Future.failedFuture("Timeout for DB_HOST:DB_PORT="
                + postgreSQLClientConfig.getString(HOST) + ":"
                + postgreSQLClientConfig.getString(PORT));
        });
  }

  /**
   * Get vertx-pg-client connection
   *
   * @param client pgPool (read or write) client
   * @see #withConn(Function)
   * @see #withConnection(Function)
   * @see #withTrans(Function)
   * @see #withTransaction(Function)
   */
  public Future<PgConnection> getConnection(PgPool client) {
    Future<SqlConnection> future = client.getConnection();
    if (! sharedPgPool) {
      return future.map(sqlConnection -> (PgConnection) sqlConnection);
    }
    // running the two SET queries adds about 1.5 ms execution time
    // "SET SCHEMA ..." sets the search_path because neither "SET ROLE" nor "SET SESSION AUTHORIZATION" set it
    String sql = DEFAULT_SCHEMA.equals(tenantId)
        ? "SET ROLE NONE; SET SCHEMA ''"
        : "SET ROLE '" + schemaName + "'; SET SCHEMA '" + schemaName + "'";
    return future.compose(sqlConnection ->
        sqlConnection.query(sql).execute()
            .map((PgConnection) sqlConnection));
  }
  /**
   * Get Vert.x {@link PgConnection}.
   *
   * @see #withConn(Function)
   * @see #withConnection(Function)
   * @see #withTrans(Function)
   * @see #withTransaction(Function)
   */
  public void getConnection(Handler<AsyncResult<PgConnection>> replyHandler) {
    getConnection().onComplete(replyHandler);
  }

  /**
   * Get readonly Vert.x {@link PgConnection}.
   *
   * @see #withReadConn(Function)
   * @see #withReadConnection(Function)
   * @see #withReadTrans(Function)
   * @see #withReadTransaction(Function)
   */
  public void getReadConnection(Handler<AsyncResult<PgConnection>> replyHandler) {
    getReadConnection().onComplete(replyHandler);
  }

  /**
   * Don't forget to close the connection!
   *
   * <p>Use closeAndHandleResult as replyHandler, for example:
   *
   * <pre>getSQLConnection(conn -> execute(conn, sql, params, closeAndHandleResult(conn, replyHandler)))</pre>
   *
   * <p>Or avoid this method and use the preferred {@link #withConn(Function)}.
   *
   * @see #withConn(Function)
   * @see #withConnection(Function)
   * @see #withTrans(Function)
   * @see #withTransaction(Function)
   */
  void getSQLConnection(Handler<AsyncResult<SQLConnection>> handler) {
    getSQLConnection(0, handler);
  }

  /**
   * Don't forget to close the connection!
   *
   * <p>Use closeAndHandleResult as replyHandler, for example:
   *
   * <pre>getSQLReadConnection(conn -> execute(conn, sql, params, closeAndHandleResult(conn, replyHandler)))</pre>
   *
   * <p>Or avoid this method and use the preferred {@link #withConn(Function)}.
   *
   * @see #withReadConn(Function)
   * @see #withReadConnection(Function)
   * @see #withReadTrans(Function)
   * @see #withReadTransaction(Function)
   */
  void getSQLReadConnection(Handler<AsyncResult<SQLConnection>> handler) {
    getSQLReadConnection(0, handler);
  }


  /**
   * Don't forget to close the connection!
   *
   * <p>Use closeAndHandleResult as replyHandler, for example:
   *
   * <pre>getSQLConnection(timeout, conn -> execute(conn, sql, params, closeAndHandleResult(conn, replyHandler)))</pre>
   *
   * <p>Or avoid this method and use the preferred {@link #withConn(int, Function)}.
   *
   * @see #withConn(Function)
   * @see #withConnection(Function)
   * @see #withTrans(Function)
   * @see #withTransaction(Function)
   */
  void getSQLConnection(AsyncResult<PgConnection> res, int queryTimeout, Handler<AsyncResult<SQLConnection>> handler) {
    if (res.failed()) {
      handler.handle(Future.failedFuture(res.cause()));
      return;
    }

    PgConnection pgConnection = res.result();

    if (queryTimeout == 0) {
      handler.handle(Future.succeededFuture(new SQLConnection(pgConnection, null, null)));
      return;
    }

    long timerId = vertx.setTimer(queryTimeout, id -> pgConnection.cancelRequest(ar -> {
      if (ar.succeeded()) {
        log.warn(
            String.format("Cancelling request due to timeout after : %d ms",
                queryTimeout));
      } else {
        log.warn("Failed to send cancelling request", ar.cause());
      }
    }));

    SQLConnection sqlConnection = new SQLConnection(pgConnection, null, timerId);
    handler.handle(Future.succeededFuture(sqlConnection));
  }

  /**
   * Get the SQL Write connection
   *
   * @see #withConn(Function)
   * @see #withConnection(Function)
   * @see #withTrans(Function)
   * @see #withTransaction(Function)
   */
  void getSQLConnection(int queryTimeout, Handler<AsyncResult<SQLConnection>> handler) {
    getConnection(res -> getSQLConnection(res, queryTimeout, handler));
  }

  /**
   * Get the SQL Read connection
   *
   * @see #withReadConn(Function)
   * @see #withReadConnection(Function)
   * @see #withReadTrans(Function)
   * @see #withReadTransaction(Function)
   */
  void getSQLReadConnection(int queryTimeout, Handler<AsyncResult<SQLConnection>> handler) {
    getReadConnection(res -> getSQLConnection(res, queryTimeout, handler));
  }

  /**
   * Execute the given function within a transaction.
   * <p>Similar to {@link #withTransaction(Function)} but with RMB specific {@link Conn}.
   * <ul>
   *   <li>The connection is automatically closed in all cases when the function exits.</li>
   *   <li>The transaction is automatically committed if the function returns a succeeded Future.</li>
   *   <li>The transaction is automatically roll-backed if the function returns a failed Future or throws a Throwable.</li>
   *   <li>The method returns a succeeded Future if the commit is successful, otherwise a failed Future.</li>
   * </ul>
   *
   * @param function code to execute
   */
  public <T> Future<T> withTrans(Function<Conn, Future<T>> function) {
    return withTrans(0, function);
  }

  /**
   * Execute the given function within a transaction (read only).
   * <p>Similar {@link #withTrans(Function)}
   */
  public <T> Future<T> withReadTrans(Function<Conn, Future<T>> function) {
    return withReadTransaction(pgConnection -> withTimeout(pgConnection, 0, function));
  }

  private <T> Future<T> withTimeout(PgConnection pgConnection, int queryTimeout,
      Function<Conn, Future<T>> function) {
    if (queryTimeout == 0) {
      return function.apply(new Conn(this, pgConnection));
    }

    long timerId = vertx.setTimer(queryTimeout, id -> pgConnection.cancelRequest(ar -> {
      if (ar.succeeded()) {
        log.warn("Cancelling request due to timeout after {} ms", queryTimeout);
      } else {
        log.warn("Failed to send cancelling request", ar.cause());
      }
    }));

    return function.apply(new Conn(this, pgConnection))
        .onComplete(done -> vertx.cancelTimer(timerId));
  }

  /**
   * Execute the given function within a transaction and with query timeout.
   * <p>Similar to {@link #withTransaction(Function)} but with RMB specific {@link Conn}.
   * <ul>
   *   <li>The connection is automatically closed in all cases when the function exits.</li>
   *   <li>The transaction is automatically committed if the function returns a succeeded Future.</li>
   *   <li>The transaction is automatically roll-backed if the function returns a failed Future or throws a Throwable.</li>
   *   <li>The method returns a succeeded Future if the commit is successful, otherwise a failed Future.</li>
   * </ul>
   *
   * @param queryTimeout in milliseconds, 0 for no timeout
   * @param function code to execute
   */
  public <T> Future<T> withTrans(int queryTimeout, Function<Conn, Future<T>> function) {
    return withTransaction(pgConnection -> withTimeout(pgConnection, queryTimeout, function));
  }

  /**
   * Execute the given function within a transaction.
   * <p>Similar to {@link PgPool#withTransaction(Function)}
   * <ul>
   *   <li>The connection is automatically closed in all cases when the function exits.</li>
   *   <li>The transaction is automatically committed if the function returns a succeeded Future.
   *   The transaction is automatically roll-backed if the function returns a failed Future or throws a Throwable.</li>
   *   <li>The method returns a succeeded Future if the commit is successful, otherwise a failed Future.</li>
   * </ul>
   *
   * <p>Use {@link #withTrans(Function)} or {@link #withTrans(int, Function)} instead
   * if you need the RMB specific methods that {@link Conn} provides.
   *
   * @param function code to execute
   */
  public <T> Future<T> withTransaction(Function<PgConnection, Future<T>> function) {
    return withTransaction(getConnection(), function);
  }

  /**
   * Execute the given function within a transaction using reader connection.
   * <p>Similar to {@link #withTransaction(Function)}
   */
  public <T> Future<T> withReadTransaction(Function<PgConnection, Future<T>> function) {
    return withTransaction(getReadConnection(), function);
  }

  <T> Future<T> withTransaction(Future<PgConnection> fPgConnection, Function<PgConnection, Future<T>> function) {
    return fPgConnection
        .flatMap(conn -> conn
            .begin()
            .flatMap(tx -> function
                .apply(conn)
                .compose(
                    res -> tx
                        .commit()
                        .flatMap(v -> Future.succeededFuture(res)),
                    err -> tx
                        .rollback()
                        .compose(v -> Future.failedFuture(err), failure -> Future.failedFuture(err))))
            .onComplete(ar -> conn.close()));
  }

  /**
   * Get a {@link PgConnection} from the pool and execute the given function.
   * <p>Similar to {@link PgPool#withConnection(Function)} but with RMB specific {@link Conn}.
   * <ul>
   *   <li>The connection is automatically closed in all cases when the function exits.</li>
   *   <li>The method returns the Future returned by the function, or a failed Future with the Throwable
   *   thrown by the function.</li>
   * </ul>
   *
   * @param function code to execute
   */
  public <T> Future<T> withConn(Function<Conn, Future<T>> function) {
    return withConn(0, function);
  }

  /**
   * Get a readonly {@link PgConnection} from the pool and execute the given function.
   * <p>Similar to {@link PgPool#withConnection(Function)} but with RMB specific readonly {@link Conn}.
   * <ul>
   *   <li>The connection is automatically closed in all cases when the function exits.</li>
   *   <li>The method returns the Future returned by the function, or a failed Future with the Throwable
   *   thrown by the function.</li>
   * </ul>
   *
   * @param function code to execute
   */
  public <T> Future<T> withReadConn(Function<Conn, Future<T>> function) {
    return withReadConn(0, function);
  }

  /**
   * Execute the given function on a {@link Conn} and with query timeout.
   * <p>Similar to {@link #withConnection(Function)} but with RMB specific {@link Conn}.
   * <ul>
   *   <li>The connection is automatically closed in all cases when the function exits.</li>
   *   <li>The method returns the Future returned by the function, or a failed Future with the Throwable
   *   thrown by the function.</li>
   * </ul>
   *
   * @param queryTimeout in milliseconds, 0 for no timeout
   * @param function code to execute
   */
  public <T> Future<T> withConn(int queryTimeout, Function<Conn, Future<T>> function) {
    return withConnection(pgConnection -> withTimeout(pgConnection, queryTimeout, function));
  }

  /**
   * Execute the given function on a {@link Conn} and with query timeout.
   * <p>Similar to readonly {@link #withConnection(Function)} but with RMB specific readonly {@link Conn}.
   * <ul>
   *   <li>The connection is automatically closed in all cases when the function exits.</li>
   *   <li>The method returns the Future returned by the function, or a failed Future with the Throwable
   *   thrown by the function.</li>
   * </ul>
   *
   * @param queryTimeout in milliseconds, 0 for no timeout
   * @param function code to execute
   */
  public <T> Future<T> withReadConn(int queryTimeout, Function<Conn, Future<T>> function) {
    return withReadConnection(pgConnection -> withTimeout(pgConnection, queryTimeout, function));
  }

  /**
   * Take the connection from the {@link SQLConnection}, wrap it into a {@link Conn} and execute the function.
   *
   * @return the result from the function, the failure of sqlConnection or any thrown Throwable.
   */
  @SuppressWarnings("squid:S1181")  // suppress "Throwable and Error should not be caught"
  // because a Future also handles Throwable, this is required for asynchronous reporting
  public <T> Future<T> withConn(AsyncResult<SQLConnection> sqlConnection, Function<Conn, Future<T>> function) {
    try {
      if (sqlConnection.failed()) {
        return Future.failedFuture(sqlConnection.cause());
      }
      return function.apply(new Conn(this, sqlConnection.result().conn));
    } catch (Throwable e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  /**
   * Get a {@link PgConnection} from the pool and execute the given function.
   * <p>Similar to {@link PgPool#withConnection(Function)}
   * <ul>
   *   <li>The connection is automatically closed in all cases when the function exits.</li>
   *   <li>The method returns the Future returned by the function, or a failed Future with the Throwable
   *   thrown by the function.</li>
   * </ul>
   *
   * <p>Use {@link #withConn(Function)} or {@link #withConn(int, Function)} instead
   * if you need the RMB specific methods that {@link Conn} provides.
   *
   * @param function code to execute
   */
  public <T> Future<T> withConnection(Function<PgConnection, Future<T>> function) {
    return getConnection().flatMap(conn -> function.apply(conn).onComplete(ar -> conn.close()));
  }

  /**
   * Get a readonly {@link PgConnection} from the pool and execute the given function.
   * <p>Similar to {@link PgPool#withConnection(Function)}
   * <ul>
   *   <li>The connection is automatically closed in all cases when the function exits.</li>
   *   <li>The method returns the Future returned by the function, or a failed Future with the Throwable
   *   thrown by the function.</li>
   * </ul>
   *
   * <p>Use {@link #withConn(Function)} or {@link #withConn(int, Function)} instead
   * if you need the RMB specific methods that {@link Conn} provides.
   *
   * @param function code to execute
   */
  public <T> Future<T> withReadConnection(Function<PgConnection, Future<T>> function) {
    return getReadConnection().flatMap(conn -> function.apply(conn).onComplete(ar -> conn.close()));
  }

  /**
   * Execute a parameterized/prepared INSERT, UPDATE or DELETE statement.
   * @param sql  The SQL statement to run.
   * @param params The parameters for the placeholders in sql.
   * @return async result.
   */
  public Future<RowSet<Row>> execute(String sql, Tuple params) {
    return withConn(conn -> conn.execute(sql, params));
  }

  /**
   * Execute a parameterized/prepared INSERT, UPDATE or DELETE statement.
   * @param sql  The SQL statement to run.
   * @param params The parameters for the placeholders in sql.
   * @param replyHandler
   */
  public void execute(String sql, Tuple params, Handler<AsyncResult<RowSet<Row>>> replyHandler)  {
    execute(sql, params)
    .onComplete(replyHandler);
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
   * @param sqlConnection - connection, see {@link #startTx(Handler)}
   * @param sql - the sql to run
   * @param replyHandler - reply handler with UpdateResult
   */
  public void execute(AsyncResult<SQLConnection> sqlConnection, String sql,
                      Handler<AsyncResult<RowSet<Row>>> replyHandler){
    withConn(sqlConnection, conn -> conn.execute(sql))
    .onComplete(replyHandler);
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
   * @param sqlConnection - connection, see {@link #startTx(Handler)}
   * @param sql - the sql to run
   * @param replyHandler - reply handler with UpdateResult
   */
  public void execute(AsyncResult<SQLConnection> sqlConnection, String sql, Tuple params,
                      Handler<AsyncResult<RowSet<Row>>> replyHandler) {

    withConn(sqlConnection, conn -> conn.execute(sql, params))
    .onComplete(replyHandler);
  }

  private static Handler<AsyncResult<RowSet<Row>>> rowSet2listRowSet(
      Handler<AsyncResult<List<RowSet<Row>>>> replyHandler) {

    return handler -> {
      if (handler.failed()) {
        replyHandler.handle(Future.failedFuture(handler.cause()));
        return;
      }
      RowSet<Row> rowSet = handler.result();
      List<RowSet<Row>> list = new ArrayList<>();
      while (rowSet != null) {
        list.add(rowSet);
        rowSet = rowSet.next();
      }
      replyHandler.handle(Future.succeededFuture(list));
    };
  }

  /**
   * Create a parameterized/prepared INSERT, UPDATE or DELETE statement and
   * run it for each {@link Tuple} of {@code params}.
   *
   * <p>Example:
   * <pre>
   *  postgresClient.startTx(beginTx -> {
   *        try {
   *          postgresClient.execute(beginTx, sql, params, reply -> {...
   * </pre>
   * @param sqlConnection - connection, see {@link #startTx(Handler)}
   * @param sql - the sql to run
   * @param params - there is one list entry for each SQL invocation containing the parameters for the placeholders.
   *                    If params is empty no SQL is run and an empty list is returned.
   * @param replyHandler - reply handler with one list element for each list element of params.
   */
  public void execute(AsyncResult<SQLConnection> sqlConnection, String sql, List<Tuple> params,
                      Handler<AsyncResult<List<RowSet<Row>>>> replyHandler) {

    withConn(sqlConnection, conn -> conn.execute(sql, params))
    .onComplete(rowSet2listRowSet(replyHandler));
  }

  /**
   * Run a parameterized/prepared SQL statement with a list of sets of parameters.
   * This is atomic, if one Tuple fails the complete list fails: all or nothing.
   *
   * @param sql - the SQL command to run
   * @param params - there is one list entry for each SQL invocation containing the
   *                    parameters for the {@code $} placeholders.
   *                    If params is empty no SQL is run and null is returned.
   * @return the reply from the database, one RowSet per params Tuple
   */
  public Future<RowSet<Row>> execute(String sql, List<Tuple> params) {
    return withConn(conn -> conn.execute(sql, params));
  }

  /**
   * Create a parameterized/prepared INSERT, UPDATE or DELETE statement and
   * run it for each {@link Tuple} of {@code params}. Wrap all in a transaction.
   *
   * @param sql - the sql to run
   * @param params - there is one list entry for each sql invocation containing the parameters for the placeholders.
   *                    If params is empty no SQL is run and an empty list is returned.
   * @param replyHandler - reply handler with one list element for each list element of params
   */
  public void execute(String sql, List<Tuple> params, Handler<AsyncResult<List<RowSet<Row>>>> replyHandler) {
    execute(sql, params)
    .onComplete(rowSet2listRowSet(replyHandler));
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
  public void persistentlyCacheResult(String cacheName, String tableName,
                                      CQLWrapper filter, Handler<AsyncResult<Integer>> replyHandler) {
    String where = "";
    if (filter != null) {
      try {
        where = filter.toString();
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        replyHandler.handle(Future.failedFuture(e));
        return;
      }
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
  public void persistentlyCacheResult(String cacheName, String tableName,
                                      Criterion filter, Handler<AsyncResult<Integer>> replyHandler) {
    String where = "";
    if (filter != null) {
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
  public void persistentlyCacheResult(String cacheName, String sql2cache, Handler<AsyncResult<Integer>> replyHandler) {
    getSQLConnection(conn -> persistentlyCacheResult(conn, cacheName, sql2cache, closeAndHandleResult(conn, replyHandler)));
  }

  private void persistentlyCacheResult(AsyncResult<SQLConnection> conn, String cacheName, String sql2cache, Handler<AsyncResult<Integer>> replyHandler) {
    try {
      if (conn.failed()) {
        replyHandler.handle(Future.failedFuture(conn.cause()));
        return;
      }
      long start = System.nanoTime();
      PgConnection connection = conn.result().conn;
      String q = "CREATE UNLOGGED TABLE IF NOT EXISTS "
          + schemaName + DOT + cacheName +" AS " + sql2cache;
      log.info(q);
      connection.query(q).execute(
          query -> {
            statsTracker("persistentlyCacheResult", "CREATE TABLE AS", start);
            if (query.failed()) {
              replyHandler.handle(Future.failedFuture(query.cause()));
            } else {
              replyHandler.handle(Future.succeededFuture(query.result().rowCount()));
            }
          });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  public void removePersistentCacheResult(String cacheName, Handler<AsyncResult<Integer>> replyHandler) {
    getSQLConnection(conn -> removePersistentCacheResult(conn, cacheName, closeAndHandleResult(conn, replyHandler)));
  }

  private void removePersistentCacheResult(AsyncResult<SQLConnection> conn, String cacheName,
                                           Handler<AsyncResult<Integer>> replyHandler){
    try {
      if (conn.failed()) {
        replyHandler.handle(Future.failedFuture(conn.cause()));
        return;
      }
      long start = System.nanoTime();
      PgConnection connection = conn.result().conn;
      connection.query("DROP TABLE " + schemaName + DOT + cacheName).execute(query -> {
        statsTracker("removePersistentCacheResult", "DROP TABLE " + cacheName, start);
        if (query.failed()) {
          replyHandler.handle(Future.failedFuture(query.cause()));
        } else {
          replyHandler.handle(Future.succeededFuture(query.result().rowCount()));
        }
      });
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Split string into lines.
   */
  private static List<String> lines(String string) {
    return Arrays.asList(string.split("\\r\\n|\\n|\\r"));
  }

  /**
   * Split the sqlFile into SQL statements.
   *
   * <a href="https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING">
   * Dollar-quoted string constants</a> with $$ or $[0-9a-zA-Z_]+$ are preserved.
   */
  static String [] splitSqlStatements(String sqlFile) {
    List<String> lines = new ArrayList<>();
    Matcher matcher = POSTGRES_DOLLAR_QUOTING.matcher(sqlFile);
    int searchStart = 0;
    while (matcher.find()) {
      lines.addAll(lines(sqlFile.substring(searchStart, matcher.start())));
      lines.add(matcher.group());
      searchStart = matcher.end();
    }
    lines.addAll(lines(sqlFile.substring(searchStart)));
    return lines.toArray(new String [0]);
  }

  @SuppressWarnings("checkstyle:EmptyBlock")
  static String [] preprocessSqlStatements(String sqlFile) throws Exception {
    StringBuilder singleStatement = new StringBuilder();
    String[] allLines = splitSqlStatements(sqlFile);
    List<String> execStatements = new ArrayList<>();
    boolean inCopy = false;
    for (int i = 0; i < allLines.length; i++) {
      if (allLines[i].toUpperCase().matches("^\\s*(CREATE USER|CREATE ROLE).*") && AES.getSecretKey() != null) {
        final Pattern pattern = Pattern.compile("PASSWORD\\s*'(.+?)'\\s*", Pattern.CASE_INSENSITIVE);
        final Matcher matcher = pattern.matcher(allLines[i]);
        if (matcher.find()){
          /* password argument indicated in the create user / role statement */
          String newPassword = createPassword(matcher.group(1));
          allLines[i] = matcher.replaceFirst(" PASSWORD '" + newPassword +"' ");
        }
      }
      if (allLines[i].trim().startsWith("\ufeff--") || allLines[i].trim().length() == 0 || allLines[i].trim().startsWith("--")) {
        // this is an sql comment, skip
      } else if (POSTGRES_COPY_FROM_STDIN.matcher(allLines[i]).matches()) {
        singleStatement.append(allLines[i]);
        inCopy = true;
      } else if (inCopy && (allLines[i].trim().equals("\\."))) {
        inCopy = false;
        execStatements.add( singleStatement.toString() );
        singleStatement = new StringBuilder();
      } else if (allLines[i].trim().endsWith(SEMI_COLON) && !inCopy) {
        execStatements.add( singleStatement.append(SPACE + allLines[i]).toString() );
        singleStatement = new StringBuilder();
      } else {
        if (inCopy)  {
          singleStatement.append("\n");
        } else {
          singleStatement.append(SPACE);
        }
        singleStatement.append(allLines[i]);
      }
    }
    String lastStatement = singleStatement.toString();
    if (! lastStatement.trim().isEmpty()) {
      execStatements.add(lastStatement);
    }
    return execStatements.toArray(new String[]{});
  }

  /**
   * Will connect to a specific database and execute the commands in sqlFile.
   * <p>
   * NOTE: NOT tested on all types of statements - but on a lot
   * <p>
   * Returns a failed Future if an SQL statement fails, this
   * is different from {@link #runSQLFile(String, boolean)} and
   * {@link #runSQLFile(String, boolean, Handler)}.
   *
   * @param sqlFile - string of sql statements
   */
  public Future<Void> runSqlFile(String sqlFile) {
    return Future.<List<String>>future(promise -> runSQLFile(sqlFile, true, promise))
        .compose(errors -> {
          if (errors.isEmpty()) {
            return Future.succeededFuture();
          } else {
            return Future.failedFuture(errors.get(0));
          }
        });
  }

  /**
   * Will connect to a specific database and execute the commands in the .sql file
   * against that database.<p />
   * NOTE: NOT tested on all types of statements - but on a lot
   *
   * @param sqlFile - string of sqls with executable statements
   * @param stopOnError - stop on first error
   * @return Future with list of failures, each failure is a string of the
   *     statement that failed and the error message; the list may be empty
   * @deprecated Use {@link #runSqlFile(String)} instead, unlike this method it returns a failed
   *     Future on SQL error.
   */
  @SuppressWarnings("java:S1845")  // suppress "Methods should not differ only by capitalization", the
  // non-deprecated method has correct camel case: https://google.github.io/styleguide/javaguide.html#s5.3-camel-case
  @Deprecated
  public Future<List<String>> runSQLFile(String sqlFile, boolean stopOnError) {
    return Future.future(promise -> runSQLFile(sqlFile, stopOnError, promise));
  }

  /**
   * Will connect to a specific database and execute the commands in the .sql file
   * against that database.<p />
   * NOTE: NOT tested on all types of statements - but on a lot
   *
   * @param sqlFile - string of sqls with executable statements
   * @param stopOnError - stop on first error
   * @param replyHandler - the handler's result is the list of failures, each failure is a string of the
   *     statement that failed and the error message; the list may be empty
   * @deprecated Use {@link #runSqlFile(String)} instead, unlike this method it returns a failed
   *     Future on SQL error.
   */
  @SuppressWarnings("java:S1845")  // suppress "Methods should not differ only by capitalization", the
  // non-deprecated method has correct camel case: https://google.github.io/styleguide/javaguide.html#s5.3-camel-case
  @Deprecated
  public void runSQLFile(String sqlFile, boolean stopOnError,
      Handler<AsyncResult<List<String>>> replyHandler){
    try {
      execute(preprocessSqlStatements(sqlFile), stopOnError, replyHandler);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      replyHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Execute multiple SQL commands in a transaction as superuser.
   *
   * @param sql SQL lines
   * @param stopOnError whether to ignore errors
   * @param replyHandler succeeding AsyncResult with empty list on success, succeeding AsyncResult
   *   with list of failures, each failure is a string with the SQL command that failed and the error message,
   *   failing AsyncResult if connection to database fails
   */
  private void execute(String[] sql, boolean stopOnError,
                       Handler<AsyncResult<List<String>>> replyHandler) {

    long s = System.nanoTime();
    log.info("Executing multiple statements with id " + Arrays.hashCode(sql));
    List<String> results = new LinkedList<>();
    PostgresClient postgresClient = getInstance(vertx);
    if (postgresClient == null) {
      replyHandler.handle(Future.failedFuture("Cannot create PostgresClient instance"));
      return;
    }
    postgresClient.getConnection()
        .compose(conn -> conn.begin()
            .compose(tx -> {
              Future<Void> future = Future.succeededFuture();
              for (int i = 0; i < sql.length; i++) {
                String stmt = sql[i];
                future = future.compose(x -> {
                  log.info("trying to execute: {}" + stmt);
                  return conn.query(stmt).execute()
                      .compose(good -> {
                        log.info("Successfully executed {}", stmt);
                        return Future.succeededFuture();
                      }, res -> {
                        log.error(res.getMessage(), res);
                        results.add(stmt + "\n" + res.getMessage());
                        if (stopOnError) {
                          return Future.failedFuture(stmt);
                        } else {
                          return Future.succeededFuture();
                        }
                      }).mapEmpty();
                });
              }
              return future
                  .compose(x -> {
                    if (results.isEmpty()) {
                      return tx.commit();
                    } else {
                      return tx.rollback();
                    }
                  }, x -> tx.rollback())
                  .eventually(y -> conn.close());
            }))
        .onComplete(x -> {
          if (x.failed()) {
            log.error(x.cause().getMessage(), x.cause());
          }
          logTimer(EXECUTE_STAT_METHOD, "" + Arrays.hashCode(sql), s);
          replyHandler.handle(Future.succeededFuture(results));
        });
  }

  /**
   * Start a Postgres Tester,
   * Assumes postgresTester is enabled.
   * Changes HOST and PORT oc configuration
   */
  public void startPostgresTester() throws PostgresTesterStartException {
    // starting Postgres
    if (!postgresTester.isStarted()) {
      log.info("Starting postgres tester");
      String username = postgreSQLClientConfig.getString(USERNAME);
      String password = postgreSQLClientConfig.getString(PASSWORD);
      String database = postgreSQLClientConfig.getString(DATABASE);

      postgresTester.start(database, username, password);
      Runtime.getRuntime().addShutdownHook(new Thread(PostgresClient::stopPostgresTester));
    }
    postgreSQLClientConfig.put(PORT, postgresTester.getPort());
    postgreSQLClientConfig.put(HOST, postgresTester.getHost());

    postgreSQLClientConfig.put(HOST_READER, postgresTester.getReadHost());
    postgreSQLClientConfig.put(PORT_READER, postgresTester.getReadPort());
  }

  /**
   * Stop the PostgresTester instance.
   *
   * <p>Does nothing, if postgresTester is not enabled or stopPostgresTester() has already been called.
   *
   * <p>After running this method a {@link #setPostgresTester(PostgresTester)} call with the same or a different
   * PostgresTester instance is needed if PostgresClient should continue using a PostgresTester.
   *
   * <p>Clients usually don't need to call this method because {@link #setPostgresTester(PostgresTester)}
   * automatically calls it if needed and both PostgresClient and Testcontainers core will take care of stopping
   * the container at the end of the test suite.
   */
  public static void stopPostgresTester() {
    if (postgresTester != null) {
      log.info("Stopping postgres tester");
      closeAllClients();
      postgresTester.close();
      postgresTester = null;
    }
  }

  public static String convertToPsqlStandard(String tenantId){
    return tenantId.toLowerCase() + "_" + MODULE_NAME;
  }

  /**
   * Name of the back-end module, usually determined and stored in
   * {@link org.folio.rest.tools.utils.ModuleName} by domain-models-maven-plugin at build time.
   */
  public static String getModuleName() {
    return MODULE_NAME;
  }

  static String getModuleName(final String className) {
    try {
      // there might be multiple class loaders: raml-module-builder, folio-some-library, mod-example
      StackWalker stackWalker = StackWalker.getInstance(Option.RETAIN_CLASS_REFERENCE);
      ClassLoader classLoader = stackWalker.getCallerClass().getClassLoader();
      while (classLoader != null) {
        try {
          Class<?> moduleNameClass = Class.forName(className, true, classLoader);
          return moduleNameClass.getMethod("getModuleName").invoke(null).toString();
        } catch (ClassNotFoundException e) {
          classLoader = classLoader.getParent();
        }
      }
      throw new ClassNotFoundException(className);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(
          "A module should generate " + className + " using domain-models-maven-plugin, "
          + "a library should provide src/test/java/" + className.replace('.', '/') + ".java for unit tests.", e);
    }
  }

  /**
   * @return the tenantId of this PostgresClient
   */
  public String getTenantId() {
    return tenantId;
  }

  /**
   * The PostgreSQL schema name for the tenantId and the module name of this PostgresClient.
   * A PostgreSQL schema name is of the form tenant_module and is used to address tables:
   * "SELECT * FROM tenant_module.table"
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * Function to correct estimated result count:
   * If the resultsCount is equal to 0, the result should be not more than offset
   * If the resultsCount is equal to limit, the result should be not less than offset + limit
   * Otherwise it should be equal to offset + resultsCount
   *
   * @param resultsCount the count of rows, that are returned from database
   * @param estimateCount the estimate result count from returned by database
   * @param offset database offset
   * @param limit database limit
   * @return corrected results count
   */
  static Integer getTotalRecords(int resultsCount, Integer estimateCount, int offset, int limit) {
    if (estimateCount == null) {
      return null;
    }
    if (limit == 0) {
      return estimateCount;
    }
    if (resultsCount == 0) {
      return Math.min(offset, estimateCount);
    } else if (resultsCount == limit) {
      return Math.max(offset + limit, estimateCount);
    }
    return offset + resultsCount;
  }


}
