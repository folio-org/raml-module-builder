package org.folio.rest.persist.cache;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgPool;
import org.folio.rest.persist.PgConnectionMock;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientHelper;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.runner.RunWith;
import org.awaitility.Awaitility;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class CachedConnectionManagerTest {
  @AfterClass
  public static void afterClass() {
    PostgresClientHelper.setSharedPgPool(false);
  }

  @Test
  public void getCachedConnectionTest(TestContext context) {
    PostgresClientHelper.setSharedPgPool(true);
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    var connectionToGet = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1);
    connectionToGet.close();
    manager.tryAddToCache(connectionToGet);
    manager.getConnection(vertx, PgPool.pool(), "tenant1", "tenant1").onComplete(context.asyncAssertSuccess(pgConnection -> {
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
        var availableConn = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1);
        availableConn.setAvailable();
        continue;
      }
      manager.tryAddToCache(new CachedPgConnection("tenant2", new PgConnectionMock(), manager, vertx, 1));
    }
    // One should be removed.
    assertEquals(PostgresClient.DEFAULT_MAX_POOL_SIZE - 1, manager.getCacheSize());
  }

  @Test
  public void closeTest() {
    var manager = new CachedConnectionManager();
    var connection = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, Vertx.vertx(), 1);
    assertFalse(connection.isAvailable());
    connection.close();
    assertTrue(connection.isAvailable());
  }

  @Test
  public void closeWithHandlerTest() {
    var manager = new CachedConnectionManager();
    var connection = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, Vertx.vertx(), 1);
    var handled = new AtomicBoolean(false);
    Handler<Void> handler = event -> handled.set(true);
    connection.closeHandler(handler);
    connection.close();
    assertTrue(handled.get());
  }

  @Test
  public void closeWithHandlerAsyncResultTest() {
    var manager = new CachedConnectionManager();
    var connection = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, Vertx.vertx(), 1);
    assertFalse(connection.isAvailable());
    connection.close(event -> {
      assertTrue(connection.isAvailable());
    });
  }

  @ParameterizedTest
  @CsvSource({
      "500, 2",
      "1500, 1",
      "2500, 0"
  })
  void releaseDelayObserverTest(int delay, int expectedCacheSize) {
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    var connection1 = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1);
    var connection2 = new CachedPgConnection("tenant2", new PgConnectionMock(), manager, vertx, 2);
    connection1.close();
    connection2.close();
    Awaitility.await().atMost(delay, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      assertEquals(expectedCacheSize, manager.getCacheSize());
    });
  }
}

