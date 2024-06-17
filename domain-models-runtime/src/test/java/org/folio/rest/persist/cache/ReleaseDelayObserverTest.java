package org.folio.rest.persist.cache;

import java.util.concurrent.TimeUnit;
import io.vertx.core.Vertx;
import org.folio.rest.persist.PgConnectionMock;
import org.folio.rest.persist.PostgresClientHelper;
import org.junit.AfterClass;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.awaitility.Awaitility;

public class ReleaseDelayObserverTest {
  @AfterClass
  public static void afterClass() {
    PostgresClientHelper.setSharedPgPool(false);
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
      Assertions.assertEquals(expectedCacheSize, manager.getCacheSize());
    });
  }
}

