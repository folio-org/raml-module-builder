package org.folio.rest.persist;

import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Pool;
import io.vertx.core.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.tools.utils.Envs;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PostgresConnectionManager {
  private static final Logger LOG = LogManager.getLogger(PostgresConnectionManager.class);
  private final List<CachedPgConnection> connectionCache = new ArrayList<>(0);

  private boolean poolExhausted;

  public PostgresConnectionManager() {
    // TODO
  }

  public synchronized void clearCache() {
    connectionCache.clear();

    LOG.debug("Cleared connection manager");
  }

  public synchronized Future<PgConnection> getConnection(Pool pool, String schemaName, String tenantId) {
    if (! PostgresClient.sharedPgPool) {
      LOG.debug("Not in shared pool mode");
      return pool.getConnection().map(PgConnection.class::cast);
    }

    LOG.debug("In shared pool mode");
    return getCachedConnection(pool, schemaName, tenantId);
  }

  public synchronized boolean shouldClose(CachedPgConnection connection) {
    if (poolExhausted) {
      LOG.debug("Pool exhausted; removing and closing");
      tryRemoveConnection(connection);
      return true;
    }

    connection.setAvailable();
    return false;
  }

  private synchronized void tryRemoveConnection(CachedPgConnection connection) {
    boolean removed = connectionCache.removeIf(entry ->
        entry.getSessionId().equals(connection.getSessionId()));
    if (removed) {
      LOG.debug("Removed {} {}", connection.getTenantId(), connection.getSessionId());
    }
  }

  // TODO Maybe use in logging or testing
  private synchronized long countTenantSessions(String tenantId) {
    return connectionCache.stream().filter(item -> item.getTenantId().equals(tenantId)).count();
  }

  private synchronized Future<PgConnection> getCachedConnection(Pool pool, String schemaName, String tenantId) {
    this.poolExhausted = pool.size() == 4;

    LOG.debug("Starting getConnection for tenant {}; poolExhausted {}", tenantId, this.poolExhausted);
    Optional<CachedPgConnection> connectionOptional =
        connectionCache.stream().filter(item ->
            item.getTenantId().equals(tenantId) && item.isAvailable()).findFirst();
    if (connectionOptional.isPresent()) {
      CachedPgConnection connection = connectionOptional.get();
      LOG.debug("Returning cached connection for tenant {}", tenantId);
      return Future.succeededFuture(connection.getConnectionAndSetUnavailable());
    }

    return createAndCacheConnection(pool, schemaName, tenantId);
  }

  private synchronized Future<PgConnection> createAndCacheConnection(Pool pool, String schemaName, String tenantId) {
    return pool.getConnection().compose(sqlConnection -> {
      String sql = PostgresClient.DEFAULT_SCHEMA.equals(tenantId)
          ? "SET ROLE NONE; SET SCHEMA ''"
          : ("SET ROLE '" + schemaName + "'; SET SCHEMA '" + schemaName + "'");
      return sqlConnection.query(sql).execute().map(x -> {
            LOG.debug("Adding to connection cache for tenant: {} {} {}",
                tenantId, connectionCache.size(), pool.size());
            var cachedPgConnection = new CachedPgConnection(tenantId, (PgConnection) sqlConnection, this);
            connectionCache.add(cachedPgConnection);
            return cachedPgConnection;
          });
    });
  }
}
