package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ConnectionCacheMetricsTest {

  @Test
  public void incrementsAndClearWork() {
    ConnectionCacheMetrics metrics = new ConnectionCacheMetrics();

    metrics.incrementHits();
    metrics.incrementMisses();
    metrics.incrementNewConnections();
    metrics.incrementNewConnectionErrors();
    metrics.incrementRecycled();
    metrics.incrementRecycleErrors();
    metrics.incrementActive();
    metrics.incrementActive();
    metrics.decrementActive();
    metrics.setPoolSize(5);

    assertEquals(1, metrics.getHits());
    assertEquals(1, metrics.getMisses());
    assertEquals(1, metrics.getNewConnections());
    assertEquals(1, metrics.getNewConnectionErrors());
    assertEquals(1, metrics.getRecycled());
    assertEquals(1, metrics.getRecycleErrors());
    assertEquals(1, metrics.getActive());
    assertEquals(5, metrics.getPoolSize());

    metrics.clear();
    assertEquals(0, metrics.getHits());
    assertEquals(0, metrics.getMisses());
    assertEquals(0, metrics.getNewConnections());
    assertEquals(0, metrics.getNewConnectionErrors());
    assertEquals(0, metrics.getRecycled());
    assertEquals(0, metrics.getRecycleErrors());
    assertEquals(0, metrics.getActive());
  }

  @Test
  public void countersWrapOnMaxValue() {
    ConnectionCacheMetrics metrics = new ConnectionCacheMetrics();

    setField(metrics, "hits", Integer.MAX_VALUE);
    setField(metrics, "misses", Integer.MAX_VALUE);
    setField(metrics, "newConnections", Integer.MAX_VALUE);
    setField(metrics, "newConnectionErrors", Integer.MAX_VALUE);
    setField(metrics, "recycled", Integer.MAX_VALUE);
    setField(metrics, "recycleErrors", Integer.MAX_VALUE);

    metrics.incrementHits();
    metrics.incrementMisses();
    metrics.incrementNewConnections();
    metrics.incrementNewConnectionErrors();
    metrics.incrementRecycled();
    metrics.incrementRecycleErrors();

    assertEquals(0, metrics.getHits());
    assertEquals(0, metrics.getMisses());
    assertEquals(0, metrics.getNewConnections());
    assertEquals(0, metrics.getNewConnectionErrors());
    assertEquals(0, metrics.getRecycled());
    assertEquals(0, metrics.getRecycleErrors());
  }

  @Test
  public void toStringContainsMessageAndNumbers() {
    ConnectionCacheMetrics metrics = new ConnectionCacheMetrics();
    metrics.incrementHits();
    metrics.incrementMisses();
    metrics.incrementRecycled();
    metrics.incrementRecycleErrors();
    metrics.incrementNewConnections();
    metrics.incrementNewConnectionErrors();
    metrics.incrementActive();
    metrics.setPoolSize(3);

    String s = metrics.toString("msg", 2);

    assertTrue(s.contains("msg"));
    assertTrue(s.contains("hits"));
    assertTrue(s.contains("misses"));
    assertTrue(s.contains("recycled"));
    assertTrue(s.contains("recycleErrors"));
    assertTrue(s.contains("newConnections"));
    assertTrue(s.contains("newConnectionErrors"));
  }

  private static void setField(Object target, String fieldName, int value) {
    try {
      var field = ConnectionCacheMetrics.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.setInt(target, value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
