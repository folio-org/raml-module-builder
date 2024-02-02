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
public class TenantSharedPoolAPIIT extends TenantITHelper {

  @Rule
  public Timeout rule = Timeout.seconds(1000);

  @BeforeClass
  public static void setUpClass() {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
      purgeTenant("tenant1")
        .compose(x -> purgeTenant("tenant2"))
        .compose(x -> purgeTenant("tenant3"))
        .compose(x -> purgeTenant("tenant4"))
        .compose(x -> vertx.close())
        .onComplete(context.asyncAssertSuccess())
        .onComplete(x -> PostgresClientHelper.setSharedPgPool(false));
  }

  @After
  public void after(TestContext context) {
    purgeTenant(null).onComplete(context.asyncAssertSuccess());
  }

  private void assertTenantPurge(TestContext context, String tenant1, String tenant2, boolean sharedPool) {
    PostgresClient.closeAllClients();
    tenantPost(new TenantAPI(), context, null, tenant1);
    tenantPost(new TenantAPI(), context, null, tenant2);
    PostgresClient.getInstance(vertx, tenant1).execute("INSERT INTO test_tenantapi VALUES ('27f0857b-3165-4d5a-af77-229e4ad7921d', '{}')")
        .compose(x -> PostgresClient.getInstance(vertx, tenant2).execute("INSERT INTO test_tenantapi VALUES ('27f0857b-3165-4d5a-af77-229e4ad7921d', '{}')"))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> assertCountFour(context, tenant2, 1))
        .compose(x -> tenantPurge(context, tenant2))
        .compose(x -> assertCountFour(context, tenant1, 1))
        .onComplete(x -> {
          if (sharedPool) {
            PostgresClientHelper.setSharedPgPool(false);
          }
          context.asyncAssertSuccess();
        });
  }

  @Test
  public void postTenantPurgeSharedPool(TestContext context) {
    PostgresClientHelper.setSharedPgPool(true);
    assertTenantPurge(context, "tenant1", "tenant2", true);
  }

  @Test
  public void postTenantPoolShared(TestContext context) {
    PostgresClientHelper.setSharedPgPool(true);

    var tenant = "tenant6";
    tenantPost(new TenantAPI(), context, null, tenant);
    PostgresClient.getInstance(vertx, tenant).execute("INSERT INTO test_tenantapi VALUES ('27f0857b-3165-4d5a-af77-229e4ad7921d', '{}')")
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .compose(x -> assertCountFour(context, tenant, 1))
        .onComplete(x -> {
          PostgresClientHelper.setSharedPgPool(false);
          context.asyncAssertSuccess();
        });
  }
}
