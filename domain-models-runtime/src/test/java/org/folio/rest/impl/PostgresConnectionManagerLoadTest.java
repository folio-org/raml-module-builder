package org.folio.rest.impl;

import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientHelper;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@RunWith(VertxUnitRunner.class)
public class PostgresConnectionManagerLoadTest extends TenantHelper {
  private final Logger LOG = LogManager.getLogger(PostgresConnectionManagerLoadTest.class);

  @Rule
  public Timeout rule = Timeout.seconds(60);

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

  @Test
  public void randomTenantsWithLongDBCallDuration(TestContext context) {
    Async async = context.async();
    int numTenants = 15;
    int numAssertions = 50;
    Random rand = new Random();
    Function<Integer, String> tenantNameFunc = (index) -> "tenant" + index;

    PostgresClientHelper.setSharedPgPool(true);
    Future.succeededFuture()
        .compose(ar -> {
          // Setup
          Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.DEBUG);

          for (int i = 0; i < numTenants; i++) {
            tenantPost(new TenantAPI(), context, null, tenantNameFunc.apply(i));
          }
          Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.DEBUG);
          return Future.succeededFuture();
        })
        .compose(ar -> {
          // Load testing
          List<Future<Row>> futures = new ArrayList<>();
          for (int i = 0; i < numAssertions; i++) {
            int tenantIndex = rand.nextInt(numTenants);
            futures.add(exerciseTenant(tenantNameFunc.apply(tenantIndex), context));
          }
          return Future.all(futures);
        })
        .compose(notUsed -> {
          // Clean up
          Future<Void> future = Future.succeededFuture();
          Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.DEBUG);
          for (int i = 0; i < numTenants; i++) {
            String tenantName = tenantNameFunc.apply(i);
            future = future.compose(ar -> purgeTenant(tenantName).mapEmpty());
          }
          return future;
        }).onComplete(ar -> {
          if (ar.failed()) {
            LOG.error(ar.cause());
            context.fail(ar.cause());
          }
          Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.DEBUG);
          PostgresClient.closeAllClients();
          async.complete();
        });
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

  protected Future<Row> assertGreaterThan(TestContext context, String tenant, int expectedCount, String query) {
    return PostgresClient.getInstance(vertx, tenant).selectSingle(query)
        .onComplete(context.asyncAssertSuccess(row ->
            assertThat(row.size(), greaterThan(expectedCount))));
  }

  private Future<Row> exerciseTenant(String tenant, TestContext context) {
    //var query = "select current_user; select pg_sleep(.2);"; // This works too, just slower.
    var query = "select current_user;";
    return assertGreaterThan(context, tenant, 0, query);
  }
}
