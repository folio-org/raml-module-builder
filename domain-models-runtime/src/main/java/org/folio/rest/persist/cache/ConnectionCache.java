package org.folio.rest.persist.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Provides a thread-safe cache that stores {@link CachedPgConnection} objects.
 * @see CachedConnectionManager
 */
public class ConnectionCache {
  private static final Logger LOG = LogManager.getLogger(ConnectionCache.class);
  private static final String LOGGER_LABEL = "CONNECTION MANAGER CACHE STATE";

  private final List<CachedPgConnection> cache;
  private final Metrics metrics = new Metrics();

  public ConnectionCache() {
    cache = Collections.synchronizedList(new ArrayList<>());
  }

  public void remove(CachedPgConnection connection) {
    synchronized (cache) {
      cache.remove(connection);
      metrics.active--;
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
    synchronized (cache) {
      cache.stream()
          .filter(CachedPgConnection::isAvailable)
          .min(Comparator.comparingLong(CachedPgConnection::getIdleSince))
          .ifPresent(connection -> {
            connection.getWrappedConnection().close();
            cache.remove(connection);
            metrics.active--;
            LOG.debug("Removed and closed oldest available connection: {} {}",
                connection.getTenantId(), connection.getSessionId());
          });
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
  public Optional<CachedPgConnection> getAvailableConnection(String tenantId) {
    synchronized (cache) {
      // First attempt to find a connection for the tenant that is available.
      Optional<CachedPgConnection> connectionOptional =
          cache.stream().filter(connection ->
              connection.getTenantId().equals(tenantId) && connection.isAvailable()).findFirst();

      // If The first attempt fails, try to find the oldest connection for another tenant that is available.
      if (connectionOptional.isEmpty()) {
        connectionOptional = cache.stream()
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
    var threshold = 100;
    Supplier<String> msgSupplier = () -> metrics.toString(LOGGER_LABEL + ": " + context);
    Supplier<String> msgDebugSupplier = metrics::toStringDebug;

    if (LOG.isDebugEnabled() || LOG.isTraceEnabled()) {
      LOG.debug("{} {}", msgSupplier.get(), msgDebugSupplier.get());
    }

    if (LOG.isInfoEnabled() && (metrics.hits % threshold == 0 || metrics.misses % threshold == 0)) {
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
    metrics.active++;
  }

  public void setPoolSizeMetric(int size) {
    metrics.poolSize = size;
  }

  class Metrics {
    int hits;
    int misses;
    int active;
    int poolSize;

    void clear() {
      hits = 0;
      misses = 0;
      active = 0;
    }

    void incrementHits() {
      hits = (hits == Integer.MAX_VALUE) ? 0 : (hits + 1);
    }

    void incrementMisses() {
      misses = (misses == Integer.MAX_VALUE) ? 0 : (misses + 1);
    }

    String toString(String msg) {
      return msg + String.format(":: %s hits, %s misses, %s size, %s active, %s pool",
          hits, misses, cache.size(), active, poolSize);
    }

    String toStringDebug() {
      var items = "\nCONNECTION MANAGER CACHE ITEMS (DEBUG):\n" ;
      synchronized (cache) {
        items += cache.stream()
            .map(item -> String.format("%s %s %s %s",
                item.getSessionId(),
                item.isAvailable(),
                item.getIdleSince(),
                item.getTenantId()
            ))
            .collect(Collectors.joining("\n"));
      }
      return items;
    }
  }
}
