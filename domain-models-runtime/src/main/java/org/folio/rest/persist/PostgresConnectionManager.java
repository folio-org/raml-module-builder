package org.folio.rest.persist;

import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Pool;
import io.vertx.core.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.tools.utils.Envs;

import java.util.ArrayList;
import java.util.List;

public class PostgresConnectionManager {
  private static final Logger LOG = LogManager.getLogger(PostgresConnectionManager.class);
  private static final int DEFAULT_CACHE_SIZE = 5;
  private final int cacheSizeLimit;
  private final List<PgConnection> connectionCache = new ArrayList<>();
  private String currentTenant = "";
  private int currentIndex;

  public PostgresConnectionManager () {
    cacheSizeLimit = Envs.getEnv(Envs.DB_MAXSHAREDPOOLSIZE) != null ?
        Integer.parseInt(Envs.getEnv(Envs.DB_MAXSHAREDPOOLSIZE)) : DEFAULT_CACHE_SIZE;
  }

  public void clearCache() {
    currentTenant = "";
    currentIndex = 0;
    connectionCache.clear();

    LOG.debug("Cleared connection manager");
  }

  public Future<PgConnection> getConnection(Pool pool, String schemaName, String tenantId) {
    if (!PostgresClient.sharedPgPool) {
      LOG.debug("Not in shared pool mode");
      return pool.getConnection().map(PgConnection.class::cast);
    }

    return getCachedConnection(pool, schemaName, tenantId);
  }

  Future<PgConnection> getCachedConnection(Pool pool, String schemaName, String tenantId) {
    if (tenantId.equals(currentTenant)) {
      var connection = tryGetCachedConnection();
      if (connection != null) {
        return Future.succeededFuture(connection);
      }

      LOG.debug("Adding to connection cache for tenant {} {}", tenantId, connectionCache.size());
      return createAndCacheConnection(pool, schemaName, tenantId);
    }

    clearCache();

    LOG.debug("Starting new connection cache for tenant {} {}", tenantId, connectionCache.size());
    return createAndCacheConnection(pool, schemaName, tenantId);
  }

  PgConnection tryGetCachedConnection() {
    if (connectionCache.size() == cacheSizeLimit) {
      return getCachedConnection();
    }
    return null;
  }

  Future<PgConnection> createAndCacheConnection(Pool pool, String schemaName, String tenantId) {
    return pool.getConnection().compose(sqlConnection -> {
      String sql = PostgresClient.DEFAULT_SCHEMA.equals(tenantId)
          ? "SET ROLE NONE; SET SCHEMA ''"
          : ("SET ROLE '" + schemaName + "'; SET SCHEMA '" + schemaName + "'");

      return sqlConnection.query(sql).execute()
          .map(x -> {
            connectionCache.add((PgConnection) sqlConnection);
            return (PgConnection) sqlConnection;
          });
    });
  }

  PgConnection getCachedConnection() {
    if (connectionCache.isEmpty()) {
      throw new IllegalStateException("No connections available");
    }

    LOG.debug("Returning cache item {} for tenant {}", currentIndex, currentTenant);

    PgConnection nextConnection = connectionCache.get(currentIndex);
    currentIndex = (currentIndex + 1) % connectionCache.size();

    return nextConnection;
  }
}
