package org.folio.rest.persist;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.ClientSSLOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.SslMode;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class PostgresClientInitializer {
  /** default release delay in milliseconds; after this time an idle database connection is closed */
  public static final int DEFAULT_CONNECTION_RELEASE_DELAY = 60000;
  static final String HOST_READER_ASYNC = "host_reader_async";
  static final String PORT_READER_ASYNC = "port_reader_async";

  private static final String CONNECTION_RELEASE_DELAY = "connectionReleaseDelay";
  private static final String MAX_POOL_SIZE = "maxPoolSize";
  private static final String RECONNECT_ATTEMPTS = "reconnectAttempts";
  private static final String RECONNECT_INTERVAL = "reconnectInterval";
  private static final String SERVER_PEM = "server_pem";

  private final Pool client;
  private Pool syncReadClient;
  private Pool asyncReadClient;

  /**
   * Defines the various clients (PgPool instances) based on their configured hosts in any supported combination.
   * @param vertx A reference to the current vertex instance.
   * @param configuration A reference to the current database configuration.
   */
  protected PostgresClientInitializer(Vertx vertx, JsonObject configuration) {
    client = createPool(vertx, configuration, PostgresClient.HOST, PostgresClient.PORT);
    syncReadClient = createPool(vertx, configuration, PostgresClient.HOST_READER, PostgresClient.PORT_READER);
    asyncReadClient = createPool(vertx, configuration, HOST_READER_ASYNC, PORT_READER_ASYNC);

    // If there is no read client defined, then use the r/w client for it.
    // If there is no async read client defined, then use the sync read client for it if it exists,
    // otherwise all 3 clients are the r/w client.
    if (syncReadClient == null) {
      syncReadClient = client;
    }
    if (asyncReadClient == null) {
      asyncReadClient = syncReadClient;
    }
  }

  public Pool getClient() {
    return client;
  }

  public Pool getSyncReadClient() {
    return syncReadClient;
  }

  public Pool getAsyncReadClient() {
    return asyncReadClient;
  }

  private static Pool createPool(Vertx vertx,
      JsonObject configuration, String hostToResolve, String portToResolve) {

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

    return PgBuilder.pool().using(vertx).connectingTo(connectOptions).with(poolOptions).build();
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

    var clientSslOptions = new ClientSSLOptions();
    clientSslOptions.setHostnameVerificationAlgorithm("HTTPS");
    clientSslOptions.setTrustOptions(
        new PemTrustOptions().addCertValue(Buffer.buffer(serverPem)));
    clientSslOptions.setEnabledSecureTransportProtocols(Collections.singleton("TLSv1.3"));
    pgConnectOptions.setSslOptions(clientSslOptions);
  }

  private static boolean isReaderHost(String hostToResolve) {
    return hostToResolve.equals(PostgresClient.HOST_READER) || hostToResolve.equals(HOST_READER_ASYNC);
  }
}

