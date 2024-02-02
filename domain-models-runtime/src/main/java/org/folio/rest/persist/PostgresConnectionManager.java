package org.folio.rest.persist;

import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Pool;
import io.vertx.core.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Optional;

public class PostgresConnectionManager {
  private static final Logger LOG = LogManager.getLogger(PostgresConnectionManager.class);
  private final Collection<CachedPgConnection> connectionCache = Collections.synchronizedCollection(new ArrayList<>());

  private int cacheHits;
  private int cacheMisses;
  private int activeConnectionCount;

  public PostgresConnectionManager() {
    // TODO
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

  private void tryAddToCache(CachedPgConnection connection) {
    if (connectionCache.contains(connection)) {
      LOG.debug("Item already exists in cache: {} {} {}",
          connection.getTenantId(), connection.getSessionId(), connection.isAvailable());
      return;
    }
    connectionCache.add(connection);
  }

  private void tryRemoveOldestAvailableConnectionAndClose() {
    // The cache must not grow larger than the max pool size of the underlying pool.
    // TODO get the pool size from the env.
    var cacheExhausted = this.connectionCache.size() >= PostgresClient.DEFAULT_MAX_POOL_SIZE;
    if (! cacheExhausted) {
      LOG.debug("Cache is not yet exhausted");
      return;
    }

    connectionCache.stream()
        .filter(CachedPgConnection::isAvailable)
        .min(Comparator.comparingLong(CachedPgConnection::getTimestamp))
        .ifPresent(connection -> {
          LOG.debug("Removing and closing oldest available connection: {} {}",
              connection.getTenantId(), connection.getSessionId());
          if (connection.getWrappedConnection() != null) {
            activeConnectionCount--;
            connection.getWrappedConnection().close();
          }
          connectionCache.remove(connection);
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
}
