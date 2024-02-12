package org.folio.rest.persist;

import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Pool;
import io.vertx.core.Future;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.tools.utils.Envs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Collectors;

public class PostgresConnectionManager {
  public static final int OBSERVER_INTERVAL = 10000;
  private static final String LOGGER_LABEL = "CONNECTION MANAGER CACHE STATE";
  private static final Logger LOG = LogManager.getLogger(PostgresConnectionManager.class);
  private final Collection<CachedPgConnection> connectionCache = Collections.synchronizedCollection(new ArrayList<>());
  private final int maxPoolSize;
  private final ConnectionMetrics connectionMetrics = new ConnectionMetrics();
  private int lowestReleaseDelayReceived;

  public PostgresConnectionManager() {
    this.maxPoolSize = getMaxSharedPoolSize();
  }

  public int getCacheSize() {
    return connectionCache.size();
  }

  public void clearCache() {
    connectionMetrics.clear();
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
      logCache();
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

          connectionMetrics.activeConnectionCount--;
          connection.getWrappedConnection().close();
          connectionCache.remove(connection);
        });
  }

  private Future<PgConnection> getOrCreateCachedConnection(Pool pool, String schemaName, String tenantId) {
    connectionMetrics.poolSize = pool.size();

    Optional<CachedPgConnection> connectionOptional =
        connectionCache.stream().filter(item ->
            item.getTenantId().equals(tenantId) && item.isAvailable()).findFirst();

    if (connectionOptional.isPresent()) {
      connectionMetrics.incrementHits();
      CachedPgConnection connection = connectionOptional.get();
      connection.setUnavailable();

      var ctx = String.format("cache hit %s %s", connection.getTenantId(), connection.getSessionId());
      debugLogCache(ctx);

      return Future.succeededFuture(connection);
    }

    connectionMetrics.incrementMisses();
    var ctx = String.format("cache miss %s", tenantId);
    debugLogCache(ctx);

    return createConnectionSession(pool, schemaName, tenantId);
  }

  private Future<PgConnection> createConnectionSession(Pool pool, String schemaName, String tenantId) {
    connectionMetrics.activeConnectionCount++;
    return pool.getConnection().compose(sqlConnection -> {
      String sql = PostgresClient.DEFAULT_SCHEMA.equals(tenantId)
          ? "SET ROLE NONE; SET SCHEMA ''"
          : ("SET ROLE '" + schemaName + "'; SET SCHEMA '" + schemaName + "'");
      var cachedConnection = new CachedPgConnection(tenantId, (PgConnection) sqlConnection, this);
      return sqlConnection.query(sql).execute().map(x -> cachedConnection);
    });
  }

  private void debugLogCache(String event) {
    if (LOG.getLevel() == Level.DEBUG) {
      var msg = connectionMetrics.toStringDebug(String.format(LOGGER_LABEL + ": %s", event));
      LOG.debug(msg);
    }
  }

  private void logCache() {
    var msg = this.connectionMetrics.toString(LOGGER_LABEL);
    LOG.info(msg);
  }

  class ConnectionMetrics {
    int cacheHits;
    int cacheMisses;
    int activeConnectionCount;
    int poolSize;

    void clear() {
      cacheHits = 0;
      cacheMisses = 0;
      activeConnectionCount = 0;
    }

    void incrementHits() {
      cacheHits = (cacheHits == Integer.MAX_VALUE) ? 0 : (cacheHits + 1);
    }

    void incrementMisses() {
      cacheMisses = (cacheMisses == Integer.MAX_VALUE) ? 0 : (cacheMisses + 1);
    }

    String toString(String msg) {
      var header =
          String.format("%n%s%n%-11s %-11s %-11s %-11s %-11s%n", msg, "Hits", "Misses", "Size", "Active", "Pool");
      var body = String.format("%-11d %-11d %-11d %-11d %-11d",
          cacheHits, cacheMisses, connectionCache.size(), activeConnectionCount, poolSize);
      return header + body;
    }

    String toStringDebug(String msg) {
      var msg = toString(msg) + "\nCACHE ITEMS (DEBUG)\n" ;
      msg += String.format("%-36s %-10s %-20s %-20s%n", "Session", "Available", "Last Used", "Tenant");
      synchronized (connectionCache) {
        msg += connectionCache.stream()
            .map(item -> String.format("%-36s %-10s %-20s %-20s",
                item.getSessionId(),
                item.isAvailable(),
                item.getLastUsedAt(),
                item.getTenantId()
                ))
            .collect(Collectors.joining("\n"));
      }
      return msg;
    }
  }
}
