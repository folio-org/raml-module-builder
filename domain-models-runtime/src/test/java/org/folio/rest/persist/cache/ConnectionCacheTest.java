package org.folio.rest.persist.cache;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.persist.PgConnectionMock;
import org.folio.rest.persist.PostgresClientHelper;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class ConnectionCacheTest {
  @AfterClass
  public static void afterClass() {
    PostgresClientHelper.setSharedPgPool(false);
  }

  @Test
  public void recycleConnectionTest() {
    var manager = new CachedConnectionManager();
    var cache = new ConnectionCache();
    var vertx = Vertx.vertx();
    var conn1 = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1);
    var conn2 = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1);
    var conn3 = new CachedPgConnection("tenant2", new PgConnectionMock(), manager, vertx, 1);
    conn1.close(); // Set it to available. This will make it available to be recycled.
    conn2.close(); // Use this to see if we can get it and see that it is not recycled.
    cache.tryAdd(conn1);
    cache.tryAdd(conn2);
    cache.tryAdd(conn3);

    var recycledConnOptional = cache.getOrRecycleConnection("tenant2");
    assertTrue(recycledConnOptional.isPresent());
    var recycledConn = recycledConnOptional.get();
    assertEquals("tenant2", recycledConn.getTenantId());
    assertEquals(conn1.getSessionId(), recycledConn.getSessionId());
    assertTrue(recycledConn.isRecycled());

    var nonRecycledConnOptional = cache.getOrRecycleConnection("tenant1");
    assertTrue(nonRecycledConnOptional.isPresent());
    var nonRecycledConn = nonRecycledConnOptional.get();
    assertEquals(conn2.getSessionId(), nonRecycledConn.getSessionId());
    assertFalse(nonRecycledConn.isRecycled());
    assertEquals("tenant1", nonRecycledConn.getTenantId());
  }

  @Test
  public void tryAddAndRemoveOldestAvailableTest() {
    var manager = new CachedConnectionManager();
    var cache = new ConnectionCache();
    var vertx = Vertx.vertx();
    var conn1 = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1);
    var conn2 = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1);
    var conn3 = new CachedPgConnection("tenant2", new PgConnectionMock(), manager, vertx, 1);
    conn2.close(); // Set it to available. This will make it available to be recycled.
    cache.tryAdd(conn1);
    cache.tryAdd(conn2);
    cache.tryAdd(conn3);
    cache.removeOldestAvailableAndClose();
    assertEquals(2, cache.size());
    cache.tryAdd(conn1);
    cache.tryAdd(conn3);
    assertEquals(2, cache.size()); // Same size because the connections already exist in the cache.
  }
}
