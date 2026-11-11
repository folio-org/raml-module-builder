package org.folio.rest.persist.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlConnectOptions;
import java.util.concurrent.atomic.AtomicBoolean;
import org.folio.rest.persist.PgConnectionMock;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientHelper;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class CachedConnectionManagerTest {
  @AfterClass
  public static void afterClass() {
    PostgresClientHelper.setSharedPgPool(false);
  }

  @Test
  public void failedAcquisitionDoesNotDisableCacheReuse() {
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    var pool = mock(Pool.class);
    var waiting = Promise.<SqlConnection>promise();
    when(pool.getConnection()).thenReturn(waiting.future());
    try {
      var acquisition = manager.getConnection(vertx, pool, "schema1", "tenant1");
      waiting.fail("unavailable");
      assertEquals("unavailable", acquisition.cause().getMessage());
      var connection = new CachedPgConnection("tenant1", "schema1", new PgConnectionMock(), manager, vertx, 1, pool);
      connection.close();
      assertTrue(connection.isAvailable());
    } finally {
      waiting.tryFail("test cleanup");
      manager.closeAllConnectionsInCache();
      vertx.close();
    }
  }

  @Test
  public void thrownAcquisitionDoesNotDisableCacheReuse() {
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    var pool = mock(Pool.class);
    when(pool.getConnection()).thenThrow(new IllegalStateException("closed"));
    try {
      assertThrows(IllegalStateException.class, () -> manager.getConnection(vertx, pool, "schema1", "tenant1"));
      var connection = new CachedPgConnection("tenant1", "schema1", new PgConnectionMock(), manager, vertx, 1, pool);
      connection.close();
      assertTrue(connection.isAvailable());
    } finally {
      manager.closeAllConnectionsInCache();
      vertx.close();
    }
  }

  @Test
  public void tenantSetupDoesNotDisableCacheReuse() {
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    var pool = mock(Pool.class);
    var heldRaw = connectionWithSetup(Future.succeededFuture());
    when(pool.getConnection()).thenReturn(Future.succeededFuture(heldRaw));
    var held = (CachedPgConnection) manager.getConnection(vertx, pool, "schema1", "tenant1").result();
    var setup = Promise.<RowSet<Row>>promise();
    var settingUpRaw = connectionWithSetup(setup.future());
    when(pool.getConnection()).thenReturn(Future.succeededFuture(settingUpRaw));
    try {
      var settingUp = manager.getConnection(vertx, pool, "schema2", "tenant2");
      assertFalse(settingUp.isComplete());
      held.close();
      assertTrue(held.isAvailable());
      verify(heldRaw, times(0)).close();
    } finally {
      setup.tryFail("test cleanup");
      manager.closeAllConnectionsInCache();
      vertx.close();
    }
  }

  @Test
  public void readerWaitDoesNotDisableWriterCacheReuse() {
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    var writer = mock(Pool.class);
    var reader = mock(Pool.class);
    var writerRaw = connectionWithSetup(Future.succeededFuture());
    when(writer.getConnection()).thenReturn(Future.succeededFuture(writerRaw));
    var held = (CachedPgConnection) manager.getConnection(vertx, writer, "schema1", "tenant1").result();
    var waiting = Promise.<SqlConnection>promise();
    when(reader.getConnection()).thenReturn(waiting.future());
    try {
      manager.getConnection(vertx, reader, "schema2", "tenant2");
      held.close();
      assertTrue(held.isAvailable());
      verify(writerRaw, times(0)).close();
    } finally {
      waiting.tryFail("test cleanup");
      manager.closeAllConnectionsInCache();
      vertx.close();
    }
  }

  @Test
  public void cachedReaderAndWriterSessionsStayInTheirSourcePools() {
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    var writer = mock(Pool.class);
    var reader = mock(Pool.class);
    var writerRaw = connectionWithSetup(Future.succeededFuture());
    var readerRaw = connectionWithSetup(Future.succeededFuture());
    when(writer.getConnection()).thenReturn(Future.succeededFuture(writerRaw));
    when(reader.getConnection()).thenReturn(Future.succeededFuture(readerRaw));
    try {
      var writerSession = manager.getConnection(vertx, writer, "schema1", "tenant1").result();
      writerSession.close();
      var readerSession = (CachedPgConnection) manager.getConnection(vertx, reader, "schema1", "tenant1").result();
      assertSame(readerRaw, readerSession.getWrappedConnection());
      readerSession.close();
      var reusedWriter = manager.getConnection(vertx, writer, "schema1", "tenant1").result();
      assertSame(writerRaw, ((CachedPgConnection) reusedWriter).getWrappedConnection());
      reusedWriter.close();
    } finally {
      manager.closeAllConnectionsInCache();
      vertx.close();
    }
  }

  @Test
  public void pendingAcquisitionReturnsLeaseExactlyOnce() {
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    var pool = mock(Pool.class);
    var raw = connectionWithSetup(Future.succeededFuture());
    when(pool.getConnection()).thenReturn(Future.succeededFuture(raw));
    var held = (CachedPgConnection) manager.getConnection(vertx, pool, "schema1", "tenant1").result();
    var waiting = Promise.<SqlConnection>promise();
    var secondWaiter = Promise.<SqlConnection>promise();
    when(pool.getConnection()).thenReturn(waiting.future(), secondWaiter.future());
    try {
      manager.getConnection(vertx, pool, "schema2", "tenant2");
      manager.getConnection(vertx, pool, "schema3", "tenant3");
      waiting.fail("first waiter cancelled");
      held.close();
      held.close();
      verify(raw, times(1)).close();
      assertFalse(held.isAvailable());
      assertEquals(0, manager.getCacheSize());
    } finally {
      waiting.tryFail("test cleanup");
      secondWaiter.tryFail("test cleanup");
      vertx.close();
    }
  }

  @Test
  public void failedTenantSwitchReturnsLeaseToPendingAcquisition() {
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    var pool = mock(Pool.class);
    var raw = connectionWithSetup(Future.succeededFuture());
    when(pool.getConnection()).thenReturn(Future.succeededFuture(raw));
    var held = (CachedPgConnection) manager.getConnection(vertx, pool, "schema1", "tenant1").result();
    held.close();
    var setup = Promise.<RowSet<Row>>promise();
    var query = raw.query("unused");
    when(query.execute()).thenReturn(setup.future());
    var switching = manager.getConnection(vertx, pool, "schema2", "tenant2");
    var waiting = Promise.<SqlConnection>promise();
    when(pool.getConnection()).thenReturn(waiting.future());
    try {
      manager.getConnection(vertx, pool, "schema3", "tenant3");
      setup.fail("role unavailable");
      assertTrue(switching.failed());
      verify(raw, times(1)).close();
      assertEquals(0, manager.getCacheSize());
      assertFalse(held.isAvailable());
    } finally {
      setup.tryFail("test cleanup");
      waiting.tryFail("test cleanup");
      vertx.close();
    }
  }

  @SuppressWarnings("unchecked")
  private static PgConnection connectionWithSetup(Future<RowSet<Row>> setup) {
    var raw = mock(PgConnection.class);
    Query<RowSet<Row>> query = mock(Query.class);
    when(raw.query(anyString())).thenReturn(query);
    when(query.execute()).thenReturn(setup);
    when(raw.close()).thenReturn(Future.succeededFuture());
    return raw;
  }

  @Test
  public void getCachedConnectionTest(TestContext context) {
    PostgresClientHelper.setSharedPgPool(true);
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    var pool = Pool.pool(new SqlConnectOptions());
    var connectionToGet = new CachedPgConnection("tenant1", "schema1", new PgConnectionMock(), manager, vertx, 1, pool);
    connectionToGet.close();
    manager.tryAddToCache(connectionToGet);
    manager.getConnection(vertx, pool, "schema1", "tenant1").onComplete(context.asyncAssertSuccess(pgConnection -> {
      var gotConnection = (CachedPgConnection) pgConnection;
      assertEquals(connectionToGet.getSessionId(), gotConnection.getSessionId());
      assertFalse(gotConnection.isAvailable());
    }));
  }

  @Test
  public void removeOldestAvailableConnectionWhenCacheIsFullTest() {
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    // Need to max out the cache.
    int bound = PostgresClient.DEFAULT_MAX_POOL_SIZE;
    for (int i = 0; i < bound; i++) {
      if (i == 1) { // Set one to be available.
        var availableConn = new CachedPgConnection("tenant1", "schema1", new PgConnectionMock(), manager, vertx, 1);
        availableConn.setAvailable();
        continue;
      }
      manager.tryAddToCache(new CachedPgConnection("tenant2", "schema2", new PgConnectionMock(), manager, vertx, 1));
    }
    // One should be removed.
    assertEquals(PostgresClient.DEFAULT_MAX_POOL_SIZE - 1, manager.getCacheSize());
  }

  @Test
  public void closeTest() {
    var manager = new CachedConnectionManager();
    var connection = new CachedPgConnection("tenant1", "schema1", new PgConnectionMock(), manager, Vertx.vertx(), 1);
    assertFalse(connection.isAvailable());
    connection.close();
    assertTrue(connection.isAvailable());
  }

  @Test
  public void closeWithHandlerTest() {
    var manager = new CachedConnectionManager();
    var connection = new CachedPgConnection("tenant1", "schema1", new PgConnectionMock(), manager, Vertx.vertx(), 1);
    var handled = new AtomicBoolean(false);
    Handler<Void> handler = event -> handled.set(true);
    connection.closeHandler(handler);
    connection.close();
    assertTrue(handled.get());
  }

  @Test
  public void closeWithHandlerAsyncResultTest(TestContext context) {
    var async = context.async();
    var manager = new CachedConnectionManager();
    var connection = new CachedPgConnection("tenant1", "schema1", new PgConnectionMock(), manager, Vertx.vertx(), 1);
    assertFalse(connection.isAvailable());
    connection.close()
    .onComplete(x -> {
      assertTrue(connection.isAvailable());
      async.complete();
    });
  }
}
