package org.folio.rest.persist.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.persist.PgConnectionMock;
import org.folio.rest.persist.PostgresClientHelper;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ConnectionCacheTest {
  @AfterClass
  public static void afterClass() {
    PostgresClientHelper.setSharedPgPool(false);
  }

  @Test
  public void getConnectionAvailableOfCurrentTenant() {
    var manager = new CachedConnectionManager();
    var cache = new ConnectionCache();
    var vertx = Vertx.vertx();
    var conn1 = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1);
    var conn2 = new CachedPgConnection("tenant2", new PgConnectionMock(), manager, vertx, 1);
    var conn3 = new CachedPgConnection("tenant3", new PgConnectionMock(), manager, vertx, 1);
    cache.tryAdd(conn1);
    cache.tryAdd(conn2);
    cache.tryAdd(conn3);
    conn2.close(); // Set it to available.

    var connOptional = cache.getAvailableConnection("tenant2");
    assertTrue(connOptional.isPresent());
    var conn = connOptional.get();
    assertEquals("tenant2", conn.getTenantId());
    assertFalse(conn.isAvailable());
  }

  @Test
  public void getConnectionOldestAvailableOfAnotherTenant() {
    var manager = new CachedConnectionManager();
    var cache = new ConnectionCache();
    var vertx = Vertx.vertx();
    var conn1 = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1);
    var conn2 = new CachedPgConnection("tenant3", new PgConnectionMock(), manager, vertx, 1);
    var conn3 = new CachedPgConnection("tenant4", new PgConnectionMock(), manager, vertx, 1);
    cache.tryAdd(conn1);
    cache.tryAdd(conn2);
    cache.tryAdd(conn3);
    conn1.close(); // Set it to available.

    var connOptional = cache.getAvailableConnection("tenant2");
    assertTrue(connOptional.isPresent());
    var conn = connOptional.get();
    assertEquals("tenant1", conn.getTenantId());
    assertFalse(conn.isAvailable());
  }

  @Test
  public void getConnectionWhenNoneAreAvailable() {
    var manager = new CachedConnectionManager();
    var cache = new ConnectionCache();
    var vertx = Vertx.vertx();
    var conn1 = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1);
    var conn2 = new CachedPgConnection("tenant2", new PgConnectionMock(), manager, vertx, 1);
    var conn3 = new CachedPgConnection("tenant3", new PgConnectionMock(), manager, vertx, 1);
    cache.tryAdd(conn1);
    cache.tryAdd(conn2);
    cache.tryAdd(conn3);

    var connOptional1 = cache.getAvailableConnection("tenant1");
    assertFalse(connOptional1.isPresent());
    var connOptional2 = cache.getAvailableConnection("tenant2");
    assertFalse(connOptional2.isPresent());
    var connOptional3 = cache.getAvailableConnection("tenant3");
    assertFalse(connOptional3.isPresent());
    var connOptional4 = cache.getAvailableConnection("tenant4"); //
    assertFalse(connOptional4.isPresent());
  }

  @Test
  public void tryAddAndRemoveOldestAvailableTest() {
    var manager = new CachedConnectionManager();
    var cache = new ConnectionCache();
    var vertx = Vertx.vertx();
    var conn1 = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1);
    var conn2 = new CachedPgConnection("tenant1", new PgConnectionMock(), manager, vertx, 1);
    var conn3 = new CachedPgConnection("tenant2", new PgConnectionMock(), manager, vertx, 1);
    conn2.close(); // Set it to available.
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
