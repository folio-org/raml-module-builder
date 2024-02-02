package org.folio;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.persist.CachedPgConnection;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresConnectionManager;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.stream.IntStream;

import static org.junit.Assert.*;

@RunWith(VertxUnitRunner.class)
public class PostgresConnectionManagerTest {

  @Test
  public void tryRemoveOldestAvailableConnection() {
    var manager = new PostgresConnectionManager();
    // Need to max out the cache.
    IntStream.range(0, PostgresClient.DEFAULT_MAX_POOL_SIZE).forEach(i -> {
      if (i == 1) { // Set one to be available.
        var availableConn = new CachedPgConnection("tenant1", null, null);
        availableConn.setAvailable();
        manager.tryClose(availableConn);
      } else {
        manager.tryClose(new CachedPgConnection("tenant2", null, null));
      }
    });
    assertEquals(PostgresClient.DEFAULT_MAX_POOL_SIZE - 1, manager.getCacheSize());
  }
}

