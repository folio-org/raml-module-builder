package org.folio.rest.persist.cache;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ConnectionCache {
  private static final Logger LOG = LogManager.getLogger(CachedConnectionManager.class);
  private static final String LOGGER_LABEL = "CONNECTION MANAGER CACHE STATE";

  private final List<CachedPgConnection> cache;
  private final Metrics metrics = new Metrics();
  private final CachedConnectionManager manager;

  public ConnectionCache(CachedConnectionManager connectionManager) {
    cache = Collections.synchronizedList(new ArrayList<>());
    manager = connectionManager;
  }

  public void removeBeforeTimeout(int observerInterval) {
    synchronized (cache) {
      cache.removeIf(connection -> {
        boolean remove = connection.isAvailable() && isTooOld(connection, observerInterval);
        if (remove) {
          metrics.active--;
          connection.getWrappedConnection().close();
          LOG.debug("Connection cache item is available and too old, removing and closing: {} {}",
              connection.getTenantId(), connection.getSessionId());
        }
        return remove;
      });
    }
  }

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

  public void tryRemoveOldestAvailableAndClose() {
    synchronized (cache) {
      cache.stream()
          .filter(CachedPgConnection::isAvailable)
          .min(Comparator.comparingLong(CachedPgConnection::getLastUsedAt))
          .ifPresent(connection -> {
            connection.getWrappedConnection().close();
            cache.remove(connection);
            metrics.active--;
            LOG.debug("Removed and closed oldest available connection: {} {}",
                connection.getTenantId(), connection.getSessionId());
          });
    }
  }

  public Optional<CachedPgConnection> getConnection(String tenantId) {
    Optional<CachedPgConnection> connectionOptional;
    synchronized (cache) {
      connectionOptional =
          cache.stream().filter(item ->
              item.getTenantId().equals(tenantId) && item.isAvailable()).findFirst();
    }
    return connectionOptional;
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

  public void log(String event) {
    if (LOG.getLevel() == Level.DEBUG) {
      var msg = this.metrics.toString(LOGGER_LABEL + ": " + event);
      var debugMsg = msg + metrics.toStringDebug();
      LOG.debug(debugMsg);
    }
  }

  public void log() {
    var msg = this.metrics.toString(LOGGER_LABEL + ": observer");
    if (LOG.getLevel() == Level.DEBUG) {
      var debugMsg = msg + metrics.toStringDebug();
      LOG.debug(debugMsg);
      return;
    }
    LOG.info(msg);
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

  private boolean isTooOld(CachedPgConnection item, int observerInterval) {
    if (this.manager.getLowestReleaseDelayReceived() <= 0) {
      throw new IllegalArgumentException("Connection release delay has not been set");
    }
    // Since we don't know when the timer is firing relative to the release delay (timeout) we subtract the timer
    // interval from the release delay.
    var timeoutWithInterval = this.manager.getLowestReleaseDelayReceived() - observerInterval;
    var millisecondsSinceLastUse = System.currentTimeMillis() - item.getLastUsedAt();
    return millisecondsSinceLastUse > timeoutWithInterval;
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
      return msg + String.format(":: %s (hits) %s (misses) %s (size) %s (active) %s (pool)",
          hits, misses, cache.size(), active, poolSize);
    }

    String toStringDebug() {
      var items = "\nCONNECTION MANAGER CACHE ITEMS (DEBUG):\n" ;
      synchronized (cache) {
        items += cache.stream()
            .map(item -> String.format("%s %s %s %s",
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
