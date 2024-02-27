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
      LOG.debug("Observer firing:: connectionReleaseDelay: {}, observerInterval: {}, observer id {}",
          connectionReleaseDelay, observerInterval, id);
      logCache();
      removeCacheConnectionsBeforeTimeout(observerInterval);
    });
  }

  private void removeCacheConnectionsBeforeTimeout(int observerInterval) {
    synchronized (connectionCache) {
      long start = System.currentTimeMillis();
      connectionCache.removeIf(connection -> {
        boolean remove = connection.isAvailable() && isTooOld(connection, observerInterval);
        if (remove) {
          connectionMetrics.activeConnectionCount--;
          connection.getWrappedConnection().close();
          long diff = System.currentTimeMillis() - start;
          LOG.debug("Connection cache item is available and too old, removing and closing: {} {} {}",
              connection.getTenantId(), connection.getSessionId(), diff);
        }
        return remove;
      });
    }
  }

  private boolean isTooOld(CachedPgConnection item, int observerInterval) {
    if (this.lowestReleaseDelayReceived <= 0) {
      throw new IllegalArgumentException("Connection release delay has not been set");
    }
    // Since we don't know when the timer is firing relative to the release delay (timeout) we subtract the timer
    // interval from the release delay.
    var timeoutWithInterval = this.lowestReleaseDelayReceived - observerInterval;
    var millisecondsSinceLastUse = System.currentTimeMillis() - item.getLastUsedAt();
    return millisecondsSinceLastUse > timeoutWithInterval;
  }

  private void tryAddToCache(CachedPgConnection connection) {
    synchronized (connectionCache) {
      if (connectionCache.contains(connection)) {
        LOG.debug("Item already exists in cache: {} {} {}",
            connection.getTenantId(), connection.getSessionId(), connection.isAvailable());
        return;
      }
      connectionCache.add(connection);
    }
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

    synchronized (connectionCache) {
      long start = System.currentTimeMillis();
      connectionCache.stream()
          .filter(CachedPgConnection::isAvailable)
          .min(Comparator.comparingLong(CachedPgConnection::getLastUsedAt))
          .ifPresent(connection -> {
            connection.getWrappedConnection().close();
            connectionCache.remove(connection);
            connectionMetrics.activeConnectionCount--;
            long diff = System.currentTimeMillis() - start;
            LOG.debug("Removed and closed oldest available connection: {} {} {}",
                connection.getTenantId(), connection.getSessionId(), diff);
          });
    }
  }

  private Future<PgConnection> getOrCreateCachedConnection(Pool pool, String schemaName, String tenantId) {
    // Because the periodic timer may not be reliable we need to make sure that cached connections have not timed out
    // before giving them out.
    removeCacheConnectionsBeforeTimeout(0);
    long start = System.currentTimeMillis();

    Optional<CachedPgConnection> connectionOptional;
    synchronized (connectionCache) {
      connectionOptional =
          connectionCache.stream().filter(item ->
              item.getTenantId().equals(tenantId) && item.isAvailable()).findFirst();
    }

    if (connectionOptional.isPresent()) {
      connectionMetrics.incrementHits();
      CachedPgConnection connection = connectionOptional.get();
      connection.setUnavailable();

      long diff = System.currentTimeMillis() - start;
      var event = String.format("cache hit %s %s %s", connection.getTenantId(), connection.getSessionId(), diff);
      logCache(event);

      return Future.succeededFuture(connection);
    }

    connectionMetrics.incrementMisses();
    var event = String.format("cache miss %s", tenantId);
    logCache(event);

    return createConnectionSession(pool, schemaName, tenantId);
  }

  private Future<PgConnection> createConnectionSession(Pool pool, String schemaName, String tenantId) {
    connectionMetrics.activeConnectionCount++;
    connectionMetrics.poolSize = pool.size();
    return pool.getConnection().compose(sqlConnection -> {
      String sql = PostgresClient.DEFAULT_SCHEMA.equals(tenantId)
          ? "SET ROLE NONE; SET SCHEMA ''"
          : ("SET ROLE '" + schemaName + "'; SET SCHEMA '" + schemaName + "'");
      var cachedConnection = new CachedPgConnection(tenantId, (PgConnection) sqlConnection, this);
      return sqlConnection.query(sql).execute().map(x -> cachedConnection);
    });
  }

  private void logCache(String event) {
    if (LOG.getLevel() == Level.DEBUG) {
      var msg = this.connectionMetrics.toString(LOGGER_LABEL + ": " + event);
      var debugMsg = msg + connectionMetrics.toStringDebug();
      LOG.debug(debugMsg);
    }
  }

  private void logCache() {
    var msg = this.connectionMetrics.toString(LOGGER_LABEL + ": observer");
    if (LOG.getLevel() == Level.DEBUG) {
      var debugMsg = msg + connectionMetrics.toStringDebug();
      LOG.debug(debugMsg);
      return;
    }
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

    String toStringDebug() {
      var items = "\nCACHE ITEMS (DEBUG)\n" ;
      items += String.format("%-36s %-10s %-20s %-20s%n", "Session", "Available", "Last Used", "Tenant");
      synchronized (connectionCache) {
        items += connectionCache.stream()
            .map(item -> String.format("%-36s %-10s %-20s %-20s",
                item.getSessionId(),
                item.isAvailable(),
                item.getLastUsedAt(),
                item.getTenantId()
                ))
            .collect(Collectors.joining("\n"));
      }
      return items;
    }
  }
}
