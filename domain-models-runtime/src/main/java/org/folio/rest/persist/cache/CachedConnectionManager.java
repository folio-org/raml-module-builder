package org.folio.rest.persist.cache;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientInitializer;
import org.folio.rest.tools.utils.Envs;
import io.vertx.core.Vertx;
import io.vertx.core.Future;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import java.util.Optional;

/**
 * Manages cached connections. Cached connections are tenant session connections. A session is a connection
 * which has the role and schema set for it for a given tenant. This allows for these session connections to be
 * reused to reduce round-trips to the database. The {@link ConnectionCache} is where these cached connections are
 * stored and which this manager manages.
 * @see ConnectionCache
 * @see ReleaseDelayObserver
 * @see CachedPgConnection
 */
public class CachedConnectionManager {
  private static final Logger LOG = LogManager.getLogger(CachedConnectionManager.class);
  private static final int MAX_POOL_SIZE =
      getIntFromEnvOrDefault(Envs.DB_MAXSHAREDPOOLSIZE, PostgresClient.DEFAULT_MAX_POOL_SIZE);
  private static final int CONNECTION_RELEASE_DELAY_SECONDS = getIntFromEnvOrDefault(Envs.DB_CONNECTIONRELEASEDELAY,
      PostgresClientInitializer.DEFAULT_CONNECTION_RELEASE_DELAY);

  private final ConnectionCache connectionCache = new ConnectionCache();

  public int getCacheSize() {
    return connectionCache.size();
  }

  public void clearCache() {
    connectionCache.clear();
    LOG.debug("Cleared connection cache");
  }

  public void removeFromCache(CachedPgConnection connection)  {
    connectionCache.remove(connection);
  }

  /**
   * Try to add the provided connection to the cache. If the connection has already been added do nothing. Also
   * attempts to remove the oldest available connection if the cache is full (for any tenant).
   */
  public void tryAddToCache(CachedPgConnection connection) {
    connectionCache.tryAdd(connection);
    limitCacheSize();
  }

  /**
   * Get a connection from the manager. If a connection for the tenant is not available, another available connection
   * will be recycled and used for the provided tenant.
   */
  public Future<PgConnection> getConnection(Vertx vertx, Pool pool, String schemaName, String tenantId) {
    Optional<CachedPgConnection> connectionOptional = connectionCache.getAvailableConnection(tenantId);

    if (connectionOptional.isPresent()) {
      connectionCache.incrementHits();
      CachedPgConnection connection = connectionOptional.get();

      // If it is being used from another tenant (recycled), we now need to set a new role and schema for it.
      if (!connection.getTenantId().equals(tenantId)) {
        connection.setTenantId(tenantId);
        return setRoleAndSchema(vertx, schemaName, tenantId, connection);
      }

      var event = String.format("cache hit %s %s", connection.getTenantId(), connection.getSessionId());
      connectionCache.log(event);

      return Future.succeededFuture(connection);
    }

    connectionCache.incrementMisses();
    var event = String.format("cache miss %s", tenantId);
    connectionCache.log(event);

    // Create a new session. If the underlying PgPool is maxed out this connection request will be added to its
    // wait queue and will be completed when a connection is available in the underlying PgPool.
    return createConnectionSession(vertx, pool, schemaName, tenantId);
  }

  private static int getIntFromEnvOrDefault(Envs envKey, int defaultValue) {
    String envValue = Envs.getEnv(envKey);
    if (envValue == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(envValue);
    } catch (NumberFormatException e) {
      LOG.error("Environment variable has wrong format:: variable: {}, value: {}", envKey.name(), envValue);
      return defaultValue;
    }
  }

  private void limitCacheSize() {
    // The cache must not grow larger than the max pool size of the underlying pool.
    var cacheExhausted = connectionCache.size() >= MAX_POOL_SIZE;
    if (!cacheExhausted) {
      LOG.debug("Cache is not yet exhausted");
      return;
    }
    connectionCache.removeOldestAvailableAndClose();
  }

  private Future<PgConnection> createConnectionSession(Vertx vertx, Pool pool, String schemaName, String tenantId) {
    connectionCache.setPoolSizeMetric(pool.size());
    return pool.getConnection().compose(sqlConnection -> setRoleAndSchema(vertx, schemaName, tenantId, sqlConnection));
  }

  private Future<PgConnection> setRoleAndSchema(Vertx vertx,
                                                String schemaName,
                                                String tenantId,
                                                SqlConnection sqlConnection) {
    connectionCache.incrementActive();
    String sql = PostgresClient.DEFAULT_SCHEMA.equals(tenantId)
        ? "SET ROLE NONE; SET SCHEMA ''"
        : ("SET ROLE '" + schemaName + "'; SET SCHEMA '" + schemaName + "'");
    var cachedConnection = new CachedPgConnection(tenantId, (PgConnection) sqlConnection,
        this, vertx, CONNECTION_RELEASE_DELAY_SECONDS);
    return sqlConnection.query(sql).execute().map(cachedConnection);
  }
}
