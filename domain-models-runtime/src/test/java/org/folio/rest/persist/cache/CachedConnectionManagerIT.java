package org.folio.rest.persist.cache;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.impl.PgConnectionImpl;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.impl.TenantAPI;
import org.folio.rest.impl.TenantHelper;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientHelper;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(VertxUnitRunner.class)
public class CachedConnectionManagerIT extends TenantHelper {

  @BeforeClass
  public static void setUpClass() {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
  }

  @AfterClass
  public static void afterClass() {
    PostgresClientHelper.setSharedPgPool(false);
  }

  @Before
  public void beforeEach() {
    PostgresClientHelper.setSharedPgPool(true);
    PostgresClient.clearConnectionCache();
  }

  @Test
  public void shouldNotCreateNestedWrappers(TestContext context) {
    tenantPost(new TenantAPI(), context, null, "tenant1");
    tenantPost(new TenantAPI(), context, null, "tenant2");

    var async = context.async();
    var pgClient1 = PostgresClient.getInstance(vertx, "tenant1");
    pgClient1.getConnection().onComplete(context.asyncAssertSuccess(conn1 -> {
      var originalSessionId = ((CachedPgConnection) conn1).getSessionId();
      // Closing the connection returns it to the cache so it can be recycled.
      conn1.close();

      var pgClient2 = PostgresClient.getInstance(vertx, "tenant2");
      pgClient2.getConnection().onComplete(context.asyncAssertSuccess(conn2 -> {
        var recycledCached = (CachedPgConnection) conn2;

        assertEquals(originalSessionId, recycledCached.getSessionId());
        // Should still wrap the REAL connection, not the cached one
        assertEquals(PgConnectionImpl.class, recycledCached.getWrappedConnection().getClass());
        assertNotEquals(CachedPgConnection.class, recycledCached.getWrappedConnection().getClass());

        conn2.close().onComplete(context.asyncAssertSuccess(v -> async.complete()));
      }));
    }));
  }

  @Test
  public void shouldUpdateConnectionMetadataAfterConnectionSucceeds(TestContext context) {
    tenantPost(new TenantAPI(), context, null, "tenant1");
    tenantPost(new TenantAPI(), context, null, "tenant2");

    var async = context.async();
    var pgClient1 = PostgresClient.getInstance(vertx, "tenant1");
    pgClient1.getConnection().onComplete(context.asyncAssertSuccess(conn1 -> {
      var recycledCached1 = (CachedPgConnection) conn1;
      var originalSessionId = recycledCached1.getSessionId();

      assertEquals("tenant1", recycledCached1.getTenantId());
      assertEquals("tenant1_raml_module_builder", recycledCached1.getSchemaName());

      // Closing the connection returns it to the cache so it can be recycled.
      conn1.close();

      // But before it is recycled, it should still have the previous metadata.
      assertEquals("tenant1", recycledCached1.getTenantId());
      assertEquals("tenant1_raml_module_builder", recycledCached1.getSchemaName());

      var pgClient2 = PostgresClient.getInstance(vertx, "tenant2");
      pgClient2.getConnection().onComplete(context.asyncAssertSuccess(conn2 -> {
        var recycledCached2 = (CachedPgConnection) conn2;

        assertEquals(originalSessionId, recycledCached2.getSessionId());
        assertEquals("tenant2", recycledCached2.getTenantId());
        assertEquals("tenant2_raml_module_builder", recycledCached2.getSchemaName());

        conn2.close().onComplete(context.asyncAssertSuccess(v -> async.complete()));
      }));
    }));
  }

  @Test
  public void shouldRecoverOnSetRoleFailure(TestContext context) {
    tenantPost(new TenantAPI(), context, null, "tenant1");

    var async = context.async();
    var pgClient1 = PostgresClient.getInstance(vertx, "tenant1");
    pgClient1.getConnection().onComplete(context.asyncAssertSuccess(conn1 -> {
      var cachedConn = (CachedPgConnection) conn1;
      var originalSessionId = cachedConn.getSessionId();

      // Closing the connection returns it to the cache so it can be recycled.
      conn1.close();

      // Try to get a connection for a tenant that does not exist. This will cause the SET ROLE to fail.
      var pgClient2 = PostgresClient.getInstance(vertx, "nonexistent");
      pgClient2.getConnection().onComplete(context.asyncAssertFailure(failure -> {
        // After the failure, the connection should have been returned to the cache with its original tenant.
        // Let's try to get it again for the original tenant.
        pgClient1.getConnection().onComplete(context.asyncAssertSuccess(conn3 -> {
          var cachedConn3 = (CachedPgConnection) conn3;

          // The connection should be the same one we got before.
          assertEquals(originalSessionId, cachedConn3.getSessionId());
          assertEquals("tenant1", cachedConn3.getTenantId());
          assertEquals("tenant1_raml_module_builder", cachedConn3.getSchemaName());

          conn3.close().onComplete(context.asyncAssertSuccess(v -> async.complete()));
        }));
      }));
    }));
  }

  @Test
  public void shouldRecoverOnSetRoleFailureForNewConnection(TestContext context) {
    tenantPost(new TenantAPI(), context, null, "diku");

    var async = context.async();
    // Try to get a connection for a tenant that does not exist. This will cause the SET ROLE to fail.
    var pgClient1 = PostgresClient.getInstance(vertx, "nonexistent");
    pgClient1.getConnection().onComplete(context.asyncAssertFailure(failure -> {
      // After the failure, the underlying connection should have been closed and returned to the pool.
      // Let's try to get a connection for a valid tenant to prove the pool is healthy.
      var pgClient2 = PostgresClient.getInstance(vertx, "diku");
      pgClient2.getConnection().onComplete(context.asyncAssertSuccess(conn -> {
        // The connection should be a valid one.
        assertEquals("diku", ((CachedPgConnection) conn).getTenantId());
        conn.close().onComplete(context.asyncAssertSuccess(v -> async.complete()));
      }));
    }));
  }
}
