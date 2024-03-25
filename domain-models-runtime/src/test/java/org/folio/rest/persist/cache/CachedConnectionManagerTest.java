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
import static org.junit.jupiter.api.Assertions.assertThrows;

@RunWith(VertxUnitRunner.class)
public class CachedConnectionManagerTest {
  @AfterClass
  public static void afterClass() {
    PostgresClientHelper.setSharedPgPool(false);
  }

  @Test
  public void getCachedConnection(TestContext context) {
    PostgresClientHelper.setSharedPgPool(true);
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    var connectionToGet = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1000);
    connectionToGet.close();
    manager.tryClose(connectionToGet);
    manager.getConnection(vertx, PgPool.pool(), "tenant1", "tenant1").onComplete(context.asyncAssertSuccess(pgConnection -> {
      var gotConnection = (CachedPgConnection) pgConnection;
      assertEquals(connectionToGet.getSessionId(), gotConnection.getSessionId());
      assertFalse(gotConnection.isAvailable());
    }));
  }

  @Test
  public void getCachedConnectionThrowsConnectionReleaseDelayNotSet(TestContext context) {
    PostgresClientHelper.setSharedPgPool(true);
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    var conn = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1000);
    conn.close();
    var pool = PgPool.pool();
    Throwable throwable = assertThrows(IllegalArgumentException.class, () ->
        manager.getConnection(vertx, pool, "tenant1", "tenant1"));
    assertEquals("Connection release delay has not been set", throwable.getMessage());
  }

  @Test
  public void removeOldestAvailableConnectionTest() {
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    // Need to max out the cache.
    int bound = PostgresClient.DEFAULT_MAX_POOL_SIZE;
    for (int i = 0; i < bound; i++) {
      if (i == 1) { // Set one to be available.
        var availableConn = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1000);
        availableConn.setAvailable();
        manager.tryClose(availableConn);
        continue;
      }
      manager.tryClose(new CachedPgConnection("tenant2", new PgConnectionMock(), manager, vertx, 1000));
    }
    assertEquals(PostgresClient.DEFAULT_MAX_POOL_SIZE - 1, manager.getCacheSize());
  }

  @Test
  public void close() {
    var manager = new CachedConnectionManager();
    var connection = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, Vertx.vertx(), 1000);
    assertFalse(connection.isAvailable());
    connection.close();
    assertTrue(connection.isAvailable());
  }

  @Test
  public void closeWithHandler() {
    var manager = new CachedConnectionManager();
    var connection = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, Vertx.vertx(), 1000);
    var handled = new AtomicBoolean(false);
    Handler<Void> handler = event -> handled.set(true);
    connection.closeHandler(handler);
    connection.close();
    assertTrue(handled.get());
  }

  @Test
  public void closeWithHandlerAsyncResult() {
    var manager = new CachedConnectionManager();
    var connection = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, Vertx.vertx(), 1000);
    assertFalse(connection.isAvailable());
    connection.close(event -> {
      assertTrue(connection.isAvailable());
    });
  }

  @ParameterizedTest
  @CsvSource({
      "500, 1",
      "1500, 2"
  })
  void releaseDelayWithObserverTest(int delay, int expectedCacheSize) {
    var manager = new CachedConnectionManager();
    var vertx = Vertx.vertx();
    var simulatedReleaseDelay = 1000;
    var connection1 = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, simulatedReleaseDelay);
    var connection2 = new CachedPgConnection("tenant2", new PgConnectionMock(), manager, vertx, simulatedReleaseDelay);
    connection2.setAvailable();
    manager.tryClose(connection1);
    manager.tryClose(connection2);
    Awaitility.await().atMost(2000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      assertEquals(expectedCacheSize, manager.getCacheSize());
    });
  }
}

