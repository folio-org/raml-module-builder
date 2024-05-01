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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class PostgresClientInitializer {
  static Logger log = LogManager.getLogger(PostgresClientInitializer.class);
  private static final String    CONNECTION_RELEASE_DELAY = "connectionReleaseDelay";
  private static final String    MAX_POOL_SIZE = "maxPoolSize";
  private static final int       DEFAULT_CONNECTION_RELEASE_DELAY = 60000;
  private static final String    RECONNECT_ATTEMPTS = "reconnectAttempts";
  private static final String    RECONNECT_INTERVAL = "reconnectInterval";
  private static final String    SERVER_PEM = "server_pem";

  private Vertx vertx;
  private JsonObject configuration;

  public PostgresClientInitializer(Vertx vertx, JsonObject configuration) {
    this.vertx = vertx;
    this.configuration = configuration;
  }

  public PgPool getClient() {
    return createPgPool(vertx, configuration, false);
  }

  public PgPool getReadClient() {
    return createPgPool(vertx, configuration, true);
  }

  public PgPool getReadAsyncClient() {
    return null;
  }

  private static PgPool createPgPool(Vertx vertx, JsonObject configuration, Boolean isReader) {
    var connectOptions = createPgConnectOptions(configuration, isReader);

    if (connectOptions == null) {
      return null;
    }

    var poolOptions = new PoolOptions();
    poolOptions.setMaxSize(
        configuration.getInteger(PostgresClient.MAX_SHARED_POOL_SIZE, configuration.getInteger(MAX_POOL_SIZE, 4)));
    Integer connectionReleaseDelay = configuration.getInteger(CONNECTION_RELEASE_DELAY, DEFAULT_CONNECTION_RELEASE_DELAY);
    poolOptions.setIdleTimeout(connectionReleaseDelay);
    poolOptions.setIdleTimeoutUnit(TimeUnit.MILLISECONDS);

    return PgPool.pool(vertx, connectOptions, poolOptions);
  }

  static PgConnectOptions createPgConnectOptions(JsonObject sqlConfig, boolean isReader) {
    PgConnectOptions pgConnectOptions = new PgConnectOptions();
    String hostToResolve = PostgresClient.HOST;
    String portToResolve = PostgresClient.PORT;

    if (isReader) {
      hostToResolve = PostgresClient.HOST_READER;
      portToResolve = PostgresClient.PORT_READER;
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

    String username = sqlConfig.getString(PostgresClient.USERNAME);
    if (username != null) {
      pgConnectOptions.setUser(username);
    }
    String password = sqlConfig.getString(PostgresClient.PASSWORD);
    if (password != null) {
      pgConnectOptions.setPassword(password);
    }
    String database = sqlConfig.getString(PostgresClient.DATABASE);
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

}

