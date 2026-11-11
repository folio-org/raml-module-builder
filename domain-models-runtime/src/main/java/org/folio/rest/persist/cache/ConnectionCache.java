package org.folio.rest.persist.cache;

import io.vertx.sqlclient.Pool;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.tools.utils.ConnectionCacheMetrics;

/**
 * Provides a thread-safe cache that stores {@link CachedPgConnection} objects.
 * @see CachedConnectionManager
 */
public class ConnectionCache {
  private static final Logger LOG = LogManager.getLogger(ConnectionCache.class);
  private static final String LOGGER_LABEL = "CONNECTION MANAGER CACHE STATE";
  private static final int INFO_LOG_LIMIT = 100;
  private final List<CachedPgConnection> cache;
  private final ConnectionCacheMetrics metrics = new ConnectionCacheMetrics();

  public ConnectionCache() {
    cache = Collections.synchronizedList(new ArrayList<>());
  }

  public void remove(CachedPgConnection connection) {
    synchronized (cache) {
      cache.remove(connection);
      metrics.decrementActive();
      LOG.debug("Removed connection: {} {}",
          connection.getTenantId(), connection.getSessionId());
    }
  }

  /**
   * Add the connection to the cache if it does not already exist. If it exists do nothing.
   * @param connection The connection to try adding.
   */
  public void tryAdd(CachedPgConnection connection) {
    synchronized (cache) {
      if (cache.contains(connection)) {
        LOG.debug("Item already exists in cache: {} {} {}",
            connection.getTenantId(), connection.getSessionId(), connection.isAvailable());
        return;
      }
      cache.add(connection);
    }
  }

  /**
   * Remove the oldest available connection and close the underlying (wrapped) connection if it is the oldest available.
   */
  public void removeOldestAvailableAndClose() {
    Optional<CachedPgConnection> oldest;
    synchronized (cache) {
      oldest = cache.stream()
          .filter(CachedPgConnection::isAvailable)
          .min(Comparator.comparingLong(CachedPgConnection::getIdleSince));
    }
    // Never take the manager lock while holding the cache lock.
    oldest.ifPresent(connection -> {
      connection.expireIfIdle();
      synchronized (cache) {
        if (connection.isReturnedToPool() && cache.remove(connection)) {
          metrics.decrementActive();
        }
      }
    });
  }

  void replace(CachedPgConnection previous, CachedPgConnection current) {
    synchronized (cache) {
      cache.set(cache.indexOf(previous), current);
    }
  }

  /**
   * Get a connection where {@link CachedPgConnection#isAvailable()} is true. First it tries to get an available
   * connection for the provided tenant. If that fails it provides one for another tenant which is the oldest
   * available. If none are available it returns null. If the connection is found it is set to be unavailable using
   * {@link CachedPgConnection#setUnavailable()} in a thread safe way.
   * @param tenantId The tenant to check first.
   * @return An optional wrapping the potentially null connection.
   */
  public Optional<CachedPgConnection> getAvailableConnection(String tenantId, String schemaName) {
    return getAvailableConnection(tenantId, schemaName, null);
  }

  /** Select only sessions from the requested pool; null preserves the unfiltered public lookup. */
  Optional<CachedPgConnection> getAvailableConnection(String tenantId, String schemaName, Pool pool) {
    synchronized (cache) {
      // First attempt to find a connection for the tenant that is available.
      Optional<CachedPgConnection> connectionOptional =
          cache.stream()
              .filter(connection -> pool == null || connection.getPool() == pool)
              .filter(connection ->
              connection.getTenantId().equals(tenantId) &&
              connection.getSchemaName().equals(schemaName) &&
              connection.isAvailable())
              .findFirst();

      // If The first attempt fails, try to find the oldest connection for another tenant that is available.
      if (connectionOptional.isEmpty()) {
        connectionOptional = cache.stream()
            .filter(connection -> pool == null || connection.getPool() == pool)
            .filter(CachedPgConnection::isAvailable)
            .min(Comparator.comparingLong(CachedPgConnection::getIdleSince));
      }

      connectionOptional.ifPresent(CachedPgConnection::setUnavailable);

      return connectionOptional;
    }
  }

  public void clear() {
    synchronized (cache) {
      cache.clear();
      metrics.clear();
    }
  }

  public void closeAll() {
    List<CachedPgConnection> connections;
    synchronized (cache) {
      connections = new ArrayList<>(cache);
    }
    for (CachedPgConnection connection : connections) {
      connection.returnToPool();
      synchronized (cache) {
        if (cache.remove(connection)) {
          metrics.decrementActive();
        }
      }
    }
  }

  public int size() {
    synchronized (cache) {
      return cache.size();
    }
  }

  /**
   * Logs the current state of the cache. If debug logging is configured, iterates and prints the cache items. If only
   * info logging is enabled, then the state of the log is limited to being logged every hundred times the cache
   * is hit or missed.
   * @param context Any details that help contextualize the event.
   */
  public void log(String context) {
    Supplier<String> msgSupplier = () -> metrics.toString(LOGGER_LABEL + ": " + context, cache.size());
    Supplier<String> msgDebugSupplier = this::toStringDebug;

    if (LOG.isDebugEnabled()) {
      LOG.debug("{} {}", msgSupplier.get(), msgDebugSupplier.get());
    }

    if (LOG.isInfoEnabled() && (metrics.getHits() % INFO_LOG_LIMIT == 0 || metrics.getMisses() % INFO_LOG_LIMIT == 0)) {
      LOG.info(msgSupplier.get());
    }
  }

  public void incrementHits() {
    metrics.incrementHits();
  }

  public void incrementMisses() {
    metrics.incrementMisses();
  }

  public void incrementActive() {
    metrics.incrementActive();
  }

  public void decrementActive() {
    metrics.decrementActive();
  }

  public void incrementRecycled() {
    metrics.incrementRecycled();
  }

  public void incrementRecycleErrors() {
    metrics.incrementRecycleErrors();
  }

  public void incrementNewConnections() {
    metrics.incrementNewConnections();
  }

  public void incrementNewConnectionErrors() {
    metrics.incrementNewConnectionErrors();
  }

  public void setPoolSizeMetric(int size) {
    metrics.setPoolSize(size);
  }

  private String toStringDebug() {
    var items = "\nCONNECTION MANAGER CACHE ITEMS (DEBUG):\n" ;
    synchronized (cache) {
      items += cache.stream()
          .map(item -> String.format("%s %s %s %s %s",
              item.getSessionId(),
              item.isAvailable(),
              item.getIdleSince(),
              item.getTenantId(),
              item.getSchemaName()
          ))
          .collect(Collectors.joining("\n"));
    }
    return items;
  }
}
