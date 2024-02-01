package org.folio.rest.impl;

import java.util.HashMap;
import java.util.Map;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientHelper;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.*;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class TenantSharedPoolAPIIT {
  private static final String tenantId = "folio_shared";
  protected static Vertx vertx;
  private static Map<String,String> okapiHeaders = new HashMap<>();
  private static final int TIMER_WAIT = 10000;

  @Rule
  public Timeout rule = Timeout.seconds(1000);

  @BeforeClass
  public static void setUpClass() {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    TenantITHelper.purgeTenant(vertx, "tenant1")
        .compose(x -> TenantITHelper.purgeTenant(vertx, "tenant2"))
        .compose(x -> TenantITHelper.purgeTenant(vertx, "tenant3"))
        .compose(x -> TenantITHelper.purgeTenant(vertx, "tenant4"))
        .compose(x -> vertx.close())
        .onComplete(context.asyncAssertSuccess())
        .onComplete(x -> PostgresClientHelper.setSharedPgPool(false));
  }

  @After
  public void after(TestContext context) {
    TenantITHelper.purgeTenant(vertx, null).onComplete(context.asyncAssertSuccess());
  }

  private void assertTenantPurge(TestContext context, String tenant1, String tenant2, boolean sharedPool) {
    PostgresClient.closeAllClients();
    TenantITHelper.tenantPost(vertx, new TenantAPI(), context, null, tenant1);
    TenantITHelper.tenantPost(vertx, new TenantAPI(), context, null, tenant2);
    PostgresClient.getInstance(vertx, tenant1).execute("INSERT INTO test_tenantapi VALUES ('27f0857b-3165-4d5a-af77-229e4ad7921d', '{}')")
        .compose(x -> PostgresClient.getInstance(vertx, tenant2).execute("INSERT INTO test_tenantapi VALUES ('27f0857b-3165-4d5a-af77-229e4ad7921d', '{}')"))
        .compose(x -> TenantITHelper.assertCountFour(vertx, context, tenant1, 1))
        .compose(x -> TenantITHelper.assertCountFour(vertx, context, tenant2, 1))
        .compose(x -> TenantITHelper.tenantPurge(vertx, context, tenant2))
        .compose(x -> TenantITHelper.assertCountFour(vertx, context, tenant1, 1))
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
    TenantITHelper.tenantPost(vertx, new TenantAPI(), context, null, tenant);
    PostgresClient.getInstance(vertx, tenant).execute("INSERT INTO test_tenantapi VALUES ('27f0857b-3165-4d5a-af77-229e4ad7921d', '{}')")
        .compose(x -> TenantITHelper.assertCountFour(vertx, context, tenant, 1))
        .compose(x -> TenantITHelper.assertCountFour(vertx, context, tenant, 1))
        .compose(x -> TenantITHelper.assertCountFour(vertx, context, tenant, 1))
        .compose(x -> TenantITHelper.assertCountFour(vertx, context, tenant, 1))
        .onComplete(x -> {
          PostgresClientHelper.setSharedPgPool(false);
          context.asyncAssertSuccess();
        });
  }
}
