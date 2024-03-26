package org.folio.rest.persist.cache;

import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Pool;
import io.vertx.core.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.Envs;

import java.util.Optional;

public class CachedConnectionManager {
  private static final Logger LOG = LogManager.getLogger(CachedConnectionManager.class);
  private final ConnectionCache connectionCache = new ConnectionCache(this);
  private final int maxPoolSize;
  private int connectionReleaseDelaySeconds = -1;

  public CachedConnectionManager() {
    this.maxPoolSize = getMaxSharedPoolSize();
  }

  public int getCacheSize() {
    return connectionCache.size();
  }

  public void clearCache() {
    connectionCache.clear();
    LOG.debug("Cleared connection manager");
  }

  public void removeFromCache(CachedPgConnection connection)  {
    connectionCache.remove(connection);
  }

  public void tryAddToCache(CachedPgConnection connection) {
    connectionCache.tryAdd(connection);
    this.tryRemoveOldestAvailableConnectionAndClose();
  }

  public Future<PgConnection> getConnection(Vertx vertx, Pool pool, String schemaName, String tenantId) {
    if (! PostgresClient.getSharedPgPool()) {
      LOG.debug("Not in shared pool mode");
      return pool.getConnection().map(PgConnection.class::cast);
    }

    if (this.connectionReleaseDelaySeconds == -1) {
      throw new IllegalArgumentException("Connection release delay has not been set");
    }

    LOG.debug("In shared pool mode");
    return getOrCreateCachedConnection(vertx, pool, schemaName, tenantId);
  }

  public void setConnectionReleaseDelay(int seconds) {
    this.connectionReleaseDelaySeconds = seconds;
  }

  private static int getMaxSharedPoolSize() {
    return Envs.getEnv(Envs.DB_MAXSHAREDPOOLSIZE) == null ?
        PostgresClient.DEFAULT_MAX_POOL_SIZE :
        Integer.parseInt(Envs.getEnv(Envs.DB_MAXSHAREDPOOLSIZE));
  }

  private void tryRemoveOldestAvailableConnectionAndClose() {
    // The cache must not grow larger than the max pool size of the underlying pool.
    var cacheExhausted = this.connectionCache.size() >= maxPoolSize;
    if (! cacheExhausted) {
      LOG.debug("Cache is not yet exhausted");
      return;
    }

    connectionCache.tryRemoveOldestAvailableAndClose();
  }

  private Future<PgConnection> getOrCreateCachedConnection(Vertx vertx, Pool pool, String schemaName, String tenantId) {
    Optional<CachedPgConnection> connectionOptional = connectionCache.getConnection(tenantId);

    if (connectionOptional.isPresent()) {
      connectionCache.incrementHits();
      CachedPgConnection connection = connectionOptional.get();
      connection.setUnavailable();

      var event = String.format("cache hit %s %s", connection.getTenantId(), connection.getSessionId());
      connectionCache.log(event);

      return Future.succeededFuture(connection);
    }

    connectionCache.incrementMisses();
    var event = String.format("cache miss %s", tenantId);
    connectionCache.log(event);

    return createConnectionSession(vertx, pool, schemaName, tenantId);
  }

  private Future<PgConnection> createConnectionSession(Vertx vertx, Pool pool, String schemaName, String tenantId) {
    connectionCache.incrementActive();
    connectionCache.setPoolSizeMetric(pool.size());
    return pool.getConnection().compose(sqlConnection -> {
      String sql = PostgresClient.DEFAULT_SCHEMA.equals(tenantId)
          ? "SET ROLE NONE; SET SCHEMA ''"
          : ("SET ROLE '" + schemaName + "'; SET SCHEMA '" + schemaName + "'");
      var cachedConnection = new CachedPgConnection(tenantId, (PgConnection) sqlConnection,
          this, vertx, connectionReleaseDelaySeconds);
      return sqlConnection.query(sql).execute().map(x -> cachedConnection);
    });
  }
}
