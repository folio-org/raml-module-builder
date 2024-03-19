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
  public static final int OBSERVER_INTERVAL = 10000;
  private static final Logger LOG = LogManager.getLogger(CachedConnectionManager.class);
  private final ConnectionCache connectionCache = new ConnectionCache(this);
  private final int maxPoolSize;
  private int lowestReleaseDelayReceived;

  public CachedConnectionManager() {
    this.maxPoolSize = getMaxSharedPoolSize();
  }

  public int getCacheSize() {
    return connectionCache.size();
  }

  public int getLowestReleaseDelayReceived() {
    return lowestReleaseDelayReceived;
  }

  public void clearCache() {
    connectionCache.clear();
    LOG.debug("Cleared connection manager");
  }

  public void tryClose(CachedPgConnection connection) {
    this.tryAddToCache(connection);
    this.tryRemoveOldestAvailableConnectionAndClose();
  }

  public Future<PgConnection> getConnection(Pool pool, String schemaName, String tenantId) {
    if (! PostgresClient.getSharedPgPool()) {
      LOG.debug("Not in shared pool mode");
      return pool.getConnection().map(PgConnection.class::cast);
    }

    LOG.debug("In shared pool mode");
    return getOrCreateCachedConnection(pool, schemaName, tenantId);
  }

  public void setObserver(Vertx vertx,  int connectionReleaseDelay) {
    setObserver(vertx, connectionReleaseDelay, OBSERVER_INTERVAL);
  }
  public void setObserver(Vertx vertx, int connectionReleaseDelay, int observerInterval) {
    if (connectionReleaseDelay == 0) {
      // Zero means there is no timeout and connections should be kept open forever. See the RMB readme.
      return;
    }

    if (this.lowestReleaseDelayReceived == 0 || connectionReleaseDelay < lowestReleaseDelayReceived) {
      // Since there can be multiple clients, we let whichever one has provided the lowest release delay win.
      this.lowestReleaseDelayReceived = connectionReleaseDelay;
    }

    LOG.debug("Setting idle connection timeout observer with interval: {}", observerInterval);
    vertx.setPeriodic(observerInterval, id -> {
      LOG.debug("Observer firing:: connectionReleaseDelay: {}, observerInterval: {}, observer id {}",
          connectionReleaseDelay, observerInterval, id);
      connectionCache.log();
      removeCacheConnectionsBeforeTimeout(observerInterval);
    });
  }

  private void removeCacheConnectionsBeforeTimeout(int observerInterval) {
    connectionCache.removeBeforeTimeout(observerInterval);
  }

  private void tryAddToCache(CachedPgConnection connection) {
    connectionCache.tryAdd(connection);
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

  private Future<PgConnection> getOrCreateCachedConnection(Pool pool, String schemaName, String tenantId) {
    // Because the periodic timer may not be reliable we need to make sure that cached connections have not timed out
    // before giving them out.
    removeCacheConnectionsBeforeTimeout(0);
    long start = System.currentTimeMillis();

    Optional<CachedPgConnection> connectionOptional = connectionCache.getConnection(tenantId);

    if (connectionOptional.isPresent()) {
      connectionCache.incrementHits();
      CachedPgConnection connection = connectionOptional.get();
      connection.setUnavailable();

      long diff = System.currentTimeMillis() - start;
      var event = String.format("cache hit %s %s %s", connection.getTenantId(), connection.getSessionId(), diff);
      connectionCache.log(event);

      return Future.succeededFuture(connection);
    }

    connectionCache.incrementMisses();
    var event = String.format("cache miss %s", tenantId);
    connectionCache.log(event);

    return createConnectionSession(pool, schemaName, tenantId);
  }

  private Future<PgConnection> createConnectionSession(Pool pool, String schemaName, String tenantId) {
    connectionCache.incrementActive();
    connectionCache.setPoolSizeMetric(pool.size());
    return pool.getConnection().compose(sqlConnection -> {
      String sql = PostgresClient.DEFAULT_SCHEMA.equals(tenantId)
          ? "SET ROLE NONE; SET SCHEMA ''"
          : ("SET ROLE '" + schemaName + "'; SET SCHEMA '" + schemaName + "'");
      var cachedConnection = new CachedPgConnection(tenantId, (PgConnection) sqlConnection, this);
      return sqlConnection.query(sql).execute().map(x -> cachedConnection);
    });
  }
}
