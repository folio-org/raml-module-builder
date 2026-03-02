package org.folio.rest.tools.utils;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.Test;

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

    assertThat(metrics.getHits(), is(1));
    assertThat(metrics.getMisses(), is(1));
    assertThat(metrics.getNewConnections(), is(1));
    assertThat(metrics.getNewConnectionErrors(), is(1));
    assertThat(metrics.getRecycled(), is(1));
    assertThat(metrics.getRecycleErrors(), is(1));
    assertThat(metrics.getActive(), is(1));
    assertThat(metrics.getPoolSize(), is(5));

    metrics.clear();
    assertThat(metrics.getHits(), is(0));
    assertThat(metrics.getMisses(), is(0));
    assertThat(metrics.getNewConnections(), is(0));
    assertThat(metrics.getNewConnectionErrors(), is(0));
    assertThat(metrics.getRecycled(), is(0));
    assertThat(metrics.getRecycleErrors(), is(0));
    assertThat(metrics.getActive(), is(0));
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

    assertThat(metrics.getHits(), is(0));
    assertThat(metrics.getMisses(), is(0));
    assertThat(metrics.getNewConnections(), is(0));
    assertThat(metrics.getNewConnectionErrors(), is(0));
    assertThat(metrics.getRecycled(), is(0));
    assertThat(metrics.getRecycleErrors(), is(0));
  }

  @Test
  public void toStringContainsMessageAndNumbers() {
    ConnectionCacheMetrics metrics = new ConnectionCacheMetrics();

    setField(metrics, "hits", 2);
    setField(metrics, "misses", 3);
    setField(metrics, "recycled", 4);
    setField(metrics, "recycleErrors", 5);
    setField(metrics, "newConnections", 6);
    setField(metrics, "newConnectionErrors", 7);
    setField(metrics, "active", 8);
    setField(metrics, "poolSize", 9);

    var result = metrics.toString("msg", 10);

    assertThat(result, is("msg:: 2 hits, 3 misses, 4 recycled, 5 recycleErrors, 6 newConnections, "
        + "7 newConnectionErrors, 10 size, 8 active, 9 pool"));
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
