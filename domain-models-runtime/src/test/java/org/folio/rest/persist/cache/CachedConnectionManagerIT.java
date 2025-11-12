package org.folio.rest.persist.cache;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.impl.PgConnectionImpl;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.impl.TenantAPI;
import org.folio.rest.impl.TenantHelper;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientHelper;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
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

  @Test
  public void shouldNotCreateNestedWrappers(TestContext context) {
    PostgresClientHelper.setSharedPgPool(true);
    tenantPost(new TenantAPI(), context, null, "tenant1");
    tenantPost(new TenantAPI(), context, null, "tenant2");

    var async = context.async();
    var pgClient1 = PostgresClient.getInstance(vertx, "tenant1");
    pgClient1.getConnection().onComplete(context.asyncAssertSuccess(conn1 -> {
      // Closing the connection returns it to the cache
      conn1.close();

      var pgClient2 = PostgresClient.getInstance(vertx, "tenant2");
      pgClient2.getConnection().onComplete(context.asyncAssertSuccess(conn2 -> {
        var recycledCached = (CachedPgConnection) conn2;

        // Should still wrap the REAL connection, not the cached one
        assertEquals(PgConnectionImpl.class, recycledCached.getWrappedConnection().getClass());
        assertNotEquals(CachedPgConnection.class, recycledCached.getWrappedConnection().getClass());

        async.complete();
      }));
    }));
  }
}
