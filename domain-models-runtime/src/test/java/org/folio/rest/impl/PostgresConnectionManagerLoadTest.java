package org.folio.rest.impl;

import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientHelper;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.*;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class PostgresConnectionManagerLoadTest extends TenantHelper {
  @Rule
  public Timeout rule = Timeout.seconds(20);

  @BeforeClass
  public static void setUpClass() {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    PostgresClientHelper.setSharedPgPool(false);
  }

  @Test
  public void multipleTenantsSharedPoolLoadTest(TestContext context) {
    PostgresClientHelper.setSharedPgPool(true);
    tenantPost(new TenantAPI(), context, null, "tenant1");
    tenantPost(new TenantAPI(), context, null, "tenant2");
    tenantPost(new TenantAPI(), context, null, "tenant3");
    tenantPost(new TenantAPI(), context, null, "tenant4");
    putLoadOnTenant("tenant1", context);
    putLoadOnTenant("tenant2", context);
    putLoadOnTenant("tenant3", context);
    putLoadOnTenant("tenant4", context);
    // Add more tenants to increase demand on cache.
  }

  private void putLoadOnTenant(String tenant, TestContext context) {
    PostgresClient.getInstance(vertx, tenant).execute("INSERT INTO test_tenantapi VALUES ('27f0857b-3165-4d5a-af77-229e4ad7921d', '{}')")
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        // Add more compose calls like above to increase load.
        .compose(x -> purgeTenant(tenant))
        .onComplete(context.asyncAssertSuccess(x -> {})); // Have to wrap the Handler in the context for it to wait.
  }
}
