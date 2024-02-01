package org.folio;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.persist.CachedPgConnection;
import org.folio.rest.persist.PostgresConnectionManager;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;

import static org.junit.Assert.*;

@RunWith(VertxUnitRunner.class)
public class PostgresConnectionManagerTest {

  @Test
  public void tryRemoveOldestAvailableConnection() {
    var cache = new ArrayList<CachedPgConnection>();
    var manager = new PostgresConnectionManager(cache);
    cache.add(new CachedPgConnection("tenant1", null, null));
    var oldestAvailable = new CachedPgConnection("tenant1", null, null);
    oldestAvailable.setAvailable();
    cache.add(oldestAvailable);
    cache.add(new CachedPgConnection("tenant1", null, null));
    cache.add(new CachedPgConnection("tenant1", null, null));
    cache.add(new CachedPgConnection("tenant1", null, null));
    var originalSize = cache.size();
    manager.tryRemoveOldestAvailableConnection();
    assertEquals(originalSize - 1, cache.size());
    assertFalse(cache.contains(oldestAvailable));
  }
}

