package org.folio.rest.persist;

import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Pool;
import io.vertx.core.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.tools.utils.Envs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Optional;

public class PostgresConnectionManager {
  public static final int OBSERVER_INTERVAL = 10000;
  private static final Logger LOG = LogManager.getLogger(PostgresConnectionManager.class);

  private final Collection<CachedPgConnection> connectionCache = Collections.synchronizedCollection(new ArrayList<>());
  private final int maxPoolSize;

  private int cacheHits;
  private int cacheMisses;
  private int activeConnectionCount;
  private int lowestReleaseDelayReceived;

  public PostgresConnectionManager() {
    this.maxPoolSize = getMaxSharedPoolSize();
  }

  public int getCacheSize() {
    return connectionCache.size();
  }

  public void clearCache() {
    cacheHits = 0;
    cacheMisses = 0;
    activeConnectionCount = 0;
    connectionCache.clear();
    LOG.debug("Cleared connection manager");
  }

  public void tryClose(CachedPgConnection connection) {
    this.tryAddToCache(connection);
    this.tryRemoveOldestAvailableConnectionAndClose();
  }

  public Future<PgConnection> getConnection(Pool pool, String schemaName, String tenantId) {
    if (! PostgresClient.sharedPgPool) {
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
      LOG.debug("Observer firing: {}", id);
      removeCacheConnectionsBeforeTimeout(observerInterval);
    });
  }

  private void removeCacheConnectionsBeforeTimeout(int observerInterval) {
    connectionCache.removeIf(item -> {
      boolean remove = item.isAvailable() && isTooOld(item, observerInterval);
      if (remove) {
        LOG.debug("Connection cache item is available and too old, removing: {} {}",
            item.getTenantId(), item.getSessionId());
      }
      return remove;
    });
  }

  private boolean isTooOld(CachedPgConnection item, int observerInterval) {
    // Since we don't know when the timer is firing relative to the release delay (timeout) we subtract the timer
    // interval from the release delay.
    var timeoutWithInterval = this.lowestReleaseDelayReceived - observerInterval;
    var millisecondsSinceLastUse = System.currentTimeMillis() - item.getLastUsedAt();
    LOG.debug("Value to check: {} {}", millisecondsSinceLastUse, timeoutWithInterval);
    return millisecondsSinceLastUse > timeoutWithInterval;
  }

  private void tryAddToCache(CachedPgConnection connection) {
    if (connectionCache.contains(connection)) {
      LOG.debug("Item already exists in cache: {} {} {}",
          connection.getTenantId(), connection.getSessionId(), connection.isAvailable());
      return;
    }
    connectionCache.add(connection);
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

    connectionCache.stream()
        .filter(CachedPgConnection::isAvailable)
        .min(Comparator.comparingLong(CachedPgConnection::getLastUsedAt))
        .ifPresent(connection -> {
          LOG.debug("Removing and closing oldest available connection: {} {}",
              connection.getTenantId(), connection.getSessionId());
          activeConnectionCount--;
          connection.getWrappedConnection().close();
          connectionCache.remove(connection);
        });
  }

  private Future<PgConnection> getOrCreateCachedConnection(Pool pool, String schemaName, String tenantId) {
    Optional<CachedPgConnection> connectionOptional =
        connectionCache.stream().filter(item ->
            item.getTenantId().equals(tenantId) && item.isAvailable()).findFirst();
    if (connectionOptional.isPresent()) {
      cacheHits++;
      CachedPgConnection connection = connectionOptional.get();
      var ctx = String.format("cache hit %s %s", connection.getTenantId(), connection.getSessionId());
      logCache(ctx, pool.size());
      connection.setUnavailable();
      return Future.succeededFuture(connection);
    }

    var ctx = String.format("cache miss %s", tenantId);
    cacheMisses++;
    logCache(ctx, pool.size());

    return createConnectionSession(pool, schemaName, tenantId);
  }

  private Future<PgConnection> createConnectionSession(Pool pool, String schemaName, String tenantId) {
    activeConnectionCount++;
    return pool.getConnection().compose(sqlConnection -> {
      String sql = PostgresClient.DEFAULT_SCHEMA.equals(tenantId)
          ? "SET ROLE NONE; SET SCHEMA ''"
          : ("SET ROLE '" + schemaName + "'; SET SCHEMA '" + schemaName + "'");
      var cachedConnection = new CachedPgConnection(tenantId, (PgConnection) sqlConnection, this);
      return sqlConnection.query(sql).execute().map(x -> cachedConnection);
    });
  }

  private void logCache(String context, int poolSize) {
    LOG.debug("Current cache state: {}", context);

    synchronized (connectionCache) {
      connectionCache.forEach(c ->
          LOG.debug("{} {} {} {} {} {} {} {} {}", cacheHits, cacheMisses, connectionCache.size(),
              activeConnectionCount, poolSize, c.getSessionId(), c.getTenantId(), c.isAvailable(),
              c.getConnectionHash()));
    }
  }
}
