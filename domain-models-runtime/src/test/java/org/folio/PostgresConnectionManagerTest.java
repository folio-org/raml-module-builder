package org.folio;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.persist.CachedPgConnection;
import org.folio.rest.persist.PgConnectionMock;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresConnectionManager;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.runner.RunWith;
import org.awaitility.Awaitility;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
class PostgresConnectionManagerTest {

  @Test
  public void removeOldestAvailableConnectionTest() {
    var manager = new PostgresConnectionManager();
    // Need to max out the cache.
    int bound = PostgresClient.DEFAULT_MAX_POOL_SIZE;
    for (int i = 0; i < bound; i++) {
      if (i == 1) { // Set one to be available.
        var availableConn = new CachedPgConnection("tenant1", new PgConnectionMock(), manager);
        availableConn.setAvailable();
        manager.tryClose(availableConn);
        continue;
      }
      manager.tryClose(new CachedPgConnection("tenant2", new PgConnectionMock(), manager));
    }
    assertEquals(PostgresClient.DEFAULT_MAX_POOL_SIZE - 1, manager.getCacheSize());
  }

  @ParameterizedTest
  @CsvSource({
      "500, 1",
      "1500, 2"
  })
  void releaseDelayWithObserverTest(int delay, int expected) {
    var vertx = Vertx.vertx();
    var manager = new PostgresConnectionManager();
    manager.setObserver(vertx, delay, 1000);
    var connection1 = new CachedPgConnection("tenant1", new PgConnectionMock(), manager);
    var connection2 = new CachedPgConnection("tenant2", new PgConnectionMock(), manager);
    connection2.setAvailable();
    manager.tryClose(connection1);
    manager.tryClose(connection2);
    Awaitility.await().atMost(3000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      assertEquals(expected, manager.getCacheSize());
    });
  }
}

