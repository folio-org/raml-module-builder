package org.folio.rest.tools.utils;

public class ConnectionCacheMetrics {
    private int hits;
    private int misses;
    private int active;
    private int poolSize;
    private int recycled;
    private int recycleErrors;
    private int newConnections;
    private int newConnectionErrors;

    public int getHits() {
        return hits;
    }

    public int getMisses() {
        return misses;
    }

    public int getActive() {
        return active;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public int getRecycled() {
        return recycled;
    }

    public int getRecycleErrors() {
        return recycleErrors;
    }

    public int getNewConnections() {
        return newConnections;
    }

    public int getNewConnectionErrors() {
        return newConnectionErrors;
    }

    public void incrementActive() {
        active++;
    }

    public void decrementActive() {
        active--;
    }

    public void clear() {
      hits = 0;
      misses = 0;
      active = 0;
      recycled = 0;
      recycleErrors = 0;
      newConnections = 0;
      newConnectionErrors = 0;
    }

    public void incrementHits() {
      hits = (hits == Integer.MAX_VALUE) ? 0 : (hits + 1);
    }

    public void incrementMisses() {
      misses = (misses == Integer.MAX_VALUE) ? 0 : (misses + 1);
    }

    public void incrementNewConnections() {
      newConnections = (newConnections == Integer.MAX_VALUE) ? 0 : (newConnections + 1);
    }

    public void incrementNewConnectionErrors() {
      newConnectionErrors = (newConnectionErrors == Integer.MAX_VALUE) ? 0 : (newConnectionErrors + 1);
    }

    public void incrementRecycled() {
      recycled = (recycled == Integer.MAX_VALUE) ? 0 : (recycled + 1);
    }

    public void incrementRecycleErrors() {
      recycleErrors = (recycleErrors == Integer.MAX_VALUE) ? 0 : (recycleErrors + 1);
    }

    public String toString(String msg, int cacheSize) {
        return msg + String.format(
                ":: %s hits, %s misses, %s recycled, %s recycleErrors, %s newConnections, "
                + "%s newConnectionErrors, %s size, %s active, %s pool",
                hits, misses, recycled, recycleErrors, newConnections, newConnectionErrors,
                cacheSize, active, poolSize);
    }
}
