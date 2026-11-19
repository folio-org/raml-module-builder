package org.folio.rest.persist.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.folio.rest.persist.PostgresClient;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class CachedConnectionLifecycleTest {
  private final CachedConnectionManager manager = new CachedConnectionManager();
  private final Vertx vertx = mock(Vertx.class);
  private final Pool pool = mock(Pool.class);
  private final PgConnection raw = mock(PgConnection.class);

  @SuppressWarnings("unchecked")
  private CachedPgConnection acquire() {
    Query<RowSet<Row>> query = mock(Query.class);
    when(raw.query(anyString())).thenReturn(query);
    when(query.execute()).thenReturn(Future.succeededFuture());
    when(raw.close()).thenReturn(Future.succeededFuture());
    when(pool.getConnection()).thenReturn(Future.succeededFuture(raw));
    return (CachedPgConnection) manager.getConnection(vertx, pool, "schema1", "tenant1").result();
  }

  private CachedPgConnection borrow() {
    return (CachedPgConnection) manager.getConnection(vertx, pool, "schema1", "tenant1").result();
  }

  @SuppressWarnings("unchecked")
  private Handler<Long> releaseTimer() {
    ArgumentCaptor<Handler<Long>> timer = ArgumentCaptor.forClass(Handler.class);
    verify(vertx).setTimer(anyLong(), timer.capture());
    return timer.getValue();
  }

  @Test
  public void staleCloseCannotReleaseAnotherBorrowersConnection() {
    var first = acquire();
    first.close();
    var second = borrow();
    var waiting = Promise.<SqlConnection>promise();
    when(pool.getConnection()).thenReturn(waiting.future());
    manager.getConnection(vertx, pool, "schema2", "tenant2");
    first.close();
    verify(raw, times(0)).close();
    assertNotSame(first, second);
    assertSame(raw, second.getWrappedConnection());
    assertFalse(second.isAvailable());
    second.close();
    verify(raw, times(1)).close();
    waiting.fail("cleanup");
  }

  @Test
  public void dispatchedTimerCannotCloseCheckedOutConnection() {
    var first = acquire();
    first.close();
    var timer = releaseTimer();
    var second = borrow();
    timer.handle(1L); // Simulate a timer already dispatched before cancellation.
    verify(raw, times(0)).close();
    assertFalse(second.isAvailable());
    second.close();
    manager.closeAllConnectionsInCache();
  }

  @Test
  public void expiredConnectionCannotBeCachedAgain() {
    var connection = acquire();
    connection.close();
    var timer = releaseTimer();
    timer.handle(1L);
    connection.close();
    timer.handle(1L);
    verify(raw, times(1)).close();
    assertEquals(0, manager.getCacheSize());
    assertFalse(connection.isAvailable());
  }

  @Test
  public void evictionCancelsTimerAndCannotBeUndoneByLateClose() {
    var connection = acquire();
    connection.close();
    var timer = releaseTimer();
    for (int i = 1; i < PostgresClient.DEFAULT_MAX_POOL_SIZE; i++) {
      manager.tryAddToCache(new CachedPgConnection("tenant2", "schema2", mock(PgConnection.class),
          manager, vertx, 1, pool));
    }
    timer.handle(1L);
    connection.close();
    verify(raw, times(1)).close();
    assertEquals(PostgresClient.DEFAULT_MAX_POOL_SIZE - 1, manager.getCacheSize());
    assertFalse(connection.isAvailable());
    verify(vertx).cancelTimer(anyLong());
  }

  @Test
  public void shutdownCannotBeUndoneByLateCloseOrTimer() {
    var connection = acquire();
    connection.close();
    var timer = releaseTimer();
    manager.closeAllConnectionsInCache();
    connection.close();
    timer.handle(1L);
    verify(raw, times(1)).close();
    assertEquals(0, manager.getCacheSize());
    assertFalse(connection.isAvailable());
  }

  @Test
  public void closeWaitsForPoolReturnAndReportsFailure() {
    var connection = acquire();
    var waiting = Promise.<SqlConnection>promise();
    when(pool.getConnection()).thenReturn(waiting.future());
    manager.getConnection(vertx, pool, "schema2", "tenant2");
    var release = Promise.<Void>promise();
    when(raw.close()).thenReturn(release.future());
    var closed = connection.close();
    assertFalse(closed.isComplete());
    assertSame(closed, connection.close());
    release.fail("release failed");
    assertTrue(closed.failed());
    assertEquals("release failed", closed.cause().getMessage());
    verify(raw, times(1)).close();
    assertEquals(0, manager.getCacheSize());
    waiting.fail("cleanup");
  }

  @Test
  public void thrownPoolReturnIsReportedAndNotRetried() {
    var connection = acquire();
    var waiting = Promise.<SqlConnection>promise();
    when(pool.getConnection()).thenReturn(waiting.future());
    manager.getConnection(vertx, pool, "schema2", "tenant2");
    when(raw.close()).thenThrow(new IllegalStateException("pool closed"));
    var closed = connection.close();
    assertTrue(closed.failed());
    assertEquals("pool closed", closed.cause().getMessage());
    assertSame(closed, connection.close());
    verify(raw, times(1)).close();
    assertEquals(0, manager.getCacheSize());
    waiting.fail("cleanup");
  }

  @Test
  public void closeHandlerBelongsToOneCheckoutAndRunsOnce() {
    var first = acquire();
    var calls = new AtomicInteger();
    first.closeHandler(ignored -> calls.incrementAndGet());
    first.close();
    var second = borrow();
    first.close();
    second.close();
    assertEquals(1, calls.get());
    manager.closeAllConnectionsInCache();
  }

  @Test
  public void releasePreservesExistingIdleTimestampPolicy() {
    var connection = acquire();
    long created = connection.getIdleSince();
    Awaitility.await().until(() -> System.currentTimeMillis() > created);
    connection.close();
    assertEquals(created, connection.getIdleSince());
    manager.closeAllConnectionsInCache();
  }
}
