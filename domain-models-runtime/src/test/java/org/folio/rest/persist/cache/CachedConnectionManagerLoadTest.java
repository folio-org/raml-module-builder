package org.folio.rest.persist.cache;

import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.impl.TenantAPI;
import org.folio.rest.impl.TenantHelper;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@RunWith(VertxUnitRunner.class)
public class CachedConnectionManagerLoadTest extends TenantHelper {
  private final Logger LOG = LogManager.getLogger(CachedConnectionManagerLoadTest.class);

  @Rule
  public Timeout rule = Timeout.seconds(20);

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
  public void randomTenantsLoadTest(TestContext context) {
    Async async = context.async();
    int numTenants = 15;
    int numAssertions = 50;
    Random rand = new Random();
    Function<Integer, String> tenantNameFunc = (index) -> "tenant" + index;

    PostgresClientHelper.setSharedPgPool(true);
    Future.succeededFuture()
        .compose(ar -> {
          // Setup
          for (int i = 0; i < numTenants; i++) {
            tenantPost(new TenantAPI(), context, null, tenantNameFunc.apply(i));
          }
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
          PostgresClient.closeAllClients();
          async.complete();
        });
  }

  protected Future<Row> assertGreaterThan(TestContext context, String tenant, String query) {
    return PostgresClient.getInstance(vertx, tenant).selectSingle(query)
        .onComplete(context.asyncAssertSuccess(row ->
            assertThat(row.size(), greaterThan(0))));
  }

  private Future<Row> exerciseTenant(String tenant, TestContext context) {
    //var query = "select current_user; select pg_sleep(.2);"; // This works too, just slower.
    var query = "select current_user;";
    return assertGreaterThan(context, tenant, query);
  }
}
