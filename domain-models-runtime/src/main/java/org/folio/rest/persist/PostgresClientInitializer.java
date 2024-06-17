package org.folio.rest.persist;

import io.netty.handler.ssl.OpenSsl;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JdkSSLEngineOptions;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.SslMode;
import io.vertx.sqlclient.PoolOptions;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PostgresClientInitializer {
  /** default release delay in milliseconds; after this time an idle database connection is closed */
  public static final int DEFAULT_CONNECTION_RELEASE_DELAY = 60000;
  static final String HOST_READER_ASYNC = "host_reader_async";
  static final String PORT_READER_ASYNC = "port_reader_async";

  private static final Logger LOG = LogManager.getLogger(PostgresClientInitializer.class);
  private static final String CONNECTION_RELEASE_DELAY = "connectionReleaseDelay";
  private static final String MAX_POOL_SIZE = "maxPoolSize";
  private static final String RECONNECT_ATTEMPTS = "reconnectAttempts";
  private static final String RECONNECT_INTERVAL = "reconnectInterval";
  private static final String SERVER_PEM = "server_pem";

  private final PgPool client;
  private PgPool readClient;
  private PgPool readClientAsync;

  /**
   * Defines the various clients (PgPool instances) based on their configured hosts in any supported combination.
   * @param vertx A reference to the current vertex instance.
   * @param configuration A reference to the current database configuration.
   */
  protected PostgresClientInitializer(Vertx vertx, JsonObject configuration) {
    client = createPgPool(vertx, configuration, PostgresClient.HOST, PostgresClient.PORT);
    readClient = createPgPool(vertx, configuration, PostgresClient.HOST_READER, PostgresClient.PORT_READER);
    readClientAsync = createPgPool(vertx, configuration, HOST_READER_ASYNC, PORT_READER_ASYNC);

    // If neither read clients are defined, they must both use the r/w client.
    if (readClient == null && readClientAsync == null) {
      readClient = client;
      readClientAsync = client;
    }
    // If there is only the synchronous read client, then it is fine that the async read client code can use it.
    if (readClient != null && readClientAsync == null) {
      readClientAsync = readClient;
    }
    // If only the async read client is defined (as will be the desired state for AWS RDS), then any code
    // which uses the sync read client must use the w/r client.
    if (readClient == null && readClientAsync != null) {
      readClient = client;
    }
  }

  public PgPool getClient() {
    return client;
  }

  public PgPool getReadClient() {
    return  readClient;
  }

  public PgPool getReadClientAsync() {
    return readClientAsync;
  }

  private static PgPool createPgPool(Vertx vertx,
                                     JsonObject configuration,
                                     String hostToResolve,
                                     String portToResolve) {
    var connectOptions = createPgConnectOptions(configuration, hostToResolve, portToResolve);

    if (connectOptions == null) {
      return null;
    }

    var poolOptions = new PoolOptions();
    poolOptions.setMaxSize(configuration.getInteger(PostgresClient.MAX_SHARED_POOL_SIZE,
            configuration.getInteger(MAX_POOL_SIZE, PostgresClient.DEFAULT_MAX_POOL_SIZE)));

    poolOptions.setIdleTimeoutUnit(TimeUnit.MILLISECONDS);
    if (PostgresClient.isSharedPool()) {
      poolOptions.setIdleTimeout(0); // The connection manager fully manages this.
    } else {
      var connectionReleaseDelay = configuration.getInteger(CONNECTION_RELEASE_DELAY, DEFAULT_CONNECTION_RELEASE_DELAY);
      poolOptions.setIdleTimeout(connectionReleaseDelay);
    }

    return PgPool.pool(vertx, connectOptions, poolOptions);
  }

  static PgConnectOptions createPgConnectOptions(JsonObject sqlConfig, String hostToResolve, String portToResolve) {
    var pgConnectOptions = new PgConnectOptions();
    pgConnectOptions.addProperty("application_name", PostgresClient.PG_APPLICATION_NAME);

    if (!trySetHostAndPort(pgConnectOptions, sqlConfig, hostToResolve, portToResolve)) {
      return null;
    }

    var username = sqlConfig.getString(PostgresClient.USERNAME);
    if (username != null) {
      pgConnectOptions.setUser(username);
    }
    var password = sqlConfig.getString(PostgresClient.PASSWORD);
    if (password != null) {
      pgConnectOptions.setPassword(password);
    }
    var database = sqlConfig.getString(PostgresClient.DATABASE);
    if (database != null) {
      pgConnectOptions.setDatabase(database);
    }
    var reconnectAttempts = sqlConfig.getInteger(RECONNECT_ATTEMPTS);
    if (reconnectAttempts != null) {
      pgConnectOptions.setReconnectAttempts(reconnectAttempts);
    }
    var reconnectInterval = sqlConfig.getLong(RECONNECT_INTERVAL);
    if (reconnectInterval != null) {
      pgConnectOptions.setReconnectInterval(reconnectInterval);
    }
    var serverPem = sqlConfig.getString(SERVER_PEM);
    if (serverPem != null) {
      setUpSsl(pgConnectOptions, serverPem);
    }
    return pgConnectOptions;
  }

  private static boolean trySetHostAndPort(PgConnectOptions pgConnectOptions,
                                           JsonObject sqlConfig,
                                           String hostToResolve,
                                           String portToResolve) {
    var host = sqlConfig.getString(hostToResolve);
    if (host != null) {
      pgConnectOptions.setHost(host);
    }

    Integer port;
    port = sqlConfig.getInteger(portToResolve);

    if (port != null) {
      pgConnectOptions.setPort(port);
    }

    return !isReaderHost(hostToResolve) || (host != null && port != null);
  }

  private static void setUpSsl(PgConnectOptions pgConnectOptions, String serverPem) {
    pgConnectOptions.setSslMode(SslMode.VERIFY_FULL);
    pgConnectOptions.setHostnameVerificationAlgorithm("HTTPS");
    pgConnectOptions.setPemTrustOptions(
        new PemTrustOptions().addCertValue(Buffer.buffer(serverPem)));
    pgConnectOptions.setEnabledSecureTransportProtocols(Collections.singleton("TLSv1.3"));
    if (OpenSSLEngineOptions.isAvailable()) {
      pgConnectOptions.setOpenSslEngineOptions(new OpenSSLEngineOptions());
    } else {
      pgConnectOptions.setJdkSslEngineOptions(new JdkSSLEngineOptions());
      LOG.error("Cannot run OpenSSL, using slow JDKSSL. Is netty-tcnative-boringssl-static for windows-x86_64, "
          + "osx-x86_64 or linux-x86_64 installed? https://netty.io/wiki/forked-tomcat-native.html "
          + "Is libc6-compat installed (if required)? https://github.com/pires/netty-tcnative-alpine");
    }
    LOG.debug("Enforcing SSL encryption for PostgreSQL connections, "
        + "requiring TLSv1.3 with server name certificate, "
        + "using {}", (OpenSSLEngineOptions.isAvailable() ? ("OpenSSL " + OpenSsl.versionString()) : "JDKSSL"));
  }

  private static boolean isReaderHost(String hostToResolve) {
    return hostToResolve.equals(PostgresClient.HOST_READER) || hostToResolve.equals(HOST_READER_ASYNC);
  }
}

