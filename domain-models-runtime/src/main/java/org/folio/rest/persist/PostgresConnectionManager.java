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
import java.util.List;
import java.util.Optional;

public class PostgresConnectionManager {
  private static final Logger LOG = LogManager.getLogger(PostgresConnectionManager.class);
  private final Collection<CachedPgConnection> connectionCache;

  private int newConnections;
  private int addToCacheCount;

  private int cacheHits;

  private int activeConnectionCount;

  public PostgresConnectionManager() {
    this.connectionCache = Collections.synchronizedCollection(new ArrayList<>());
  }

  public PostgresConnectionManager(List<CachedPgConnection> connectionCache) {
    this.connectionCache = connectionCache;
  }

  public void clearCache() {
    cacheHits = 0;
    newConnections = 0;
    connectionCache.clear();

    LOG.debug("Cleared connection manager");
  }

  public Future<PgConnection> getConnection(Pool pool, String schemaName, String tenantId) {
    if (! PostgresClient.sharedPgPool) {
      LOG.debug("Not in shared pool mode");
      return pool.getConnection().map(PgConnection.class::cast);
    }

    LOG.debug("In shared pool mode");
    return getOrCreateCachedConnection(pool, schemaName, tenantId);
  }

  public void tryAddToCache(CachedPgConnection cachedPgConnection) {
    addToCacheCount++;
    LOG.debug("Add to cache count: {} {}", cachedPgConnection.getTenantId(), addToCacheCount);
    if (! connectionCache.contains(cachedPgConnection)) {
      connectionCache.add(cachedPgConnection);
      logCache("after add ", -1);
    }
  }

  public void tryRemoveOldestAvailableConnection() {
    var poolExhausted = this.connectionCache.size() >= 5;
    if (! poolExhausted) {
      return;
    }

    LOG.debug("Before remove called");
    logCache("before remove", -1);
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
    LOG.debug("After remove {}", connectionCache.size());
  }

  private void logCache(String context, int poolSize) {
    LOG.debug("Current cache state: {}", context);

    synchronized (connectionCache) {
      connectionCache.forEach(c ->
        LOG.debug("{} {} {} {} {} {} {} {} {}", cacheHits, newConnections, connectionCache.size(),
            activeConnectionCount, poolSize, c.getSessionId(), c.getTenantId(), c.isAvailable(),
            c.getConnectionHash()));
    }
  }

  private Future<PgConnection> getOrCreateCachedConnection(Pool pool, String schemaName, String tenantId) {
    logCache("before cache check: " + tenantId, pool.size());
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
    logCache(ctx, pool.size());

    return createConnectionSession(pool, schemaName, tenantId);
  }

  private Future<PgConnection> createConnectionSession(Pool pool, String schemaName, String tenantId) {
    newConnections++;
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
