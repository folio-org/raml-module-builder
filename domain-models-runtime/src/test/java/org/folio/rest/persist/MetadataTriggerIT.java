package org.folio.rest.persist;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

@RunWith(VertxUnitRunner.class)
public class MetadataTriggerIT {

  private static final String TENANT_ID = "test_tenant";
  private static final String TABLE = "test_md_trigger_table";

  private static Vertx vertx;

  @BeforeClass
  public static void setUp(TestContext context) {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    PostgresClient.getInstance(vertx);

    Async async = context.async();
    int port = NetworkUtils.nextFreePort();

    DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class, options, deploy -> {
      TenantClient tenantClient = new TenantClient("http://localhost:" + port, TENANT_ID, "token");
      try {
        tenantClient.postTenant(null, post -> async.complete());
      } catch (Exception e) {
        context.fail(e);
      }
    });
  }

  @AfterClass
  public static void tearDown() {
    PostgresClient.stopEmbeddedPostgres();
  }

  @Test
  public void isSuccessWhenUpdateEmptyMetadataWithEmptyMetadata(TestContext context) {

    String id = "e2a5133a-0e16-48ea-9226-01fc047625bb";

    JsonObject cr = new JsonObject()
      .put("id", id)
      .put("value", 1);

    JsonObject upd = new JsonObject()
      .put("id", id)
      .put("value", 2);

    save(id, cr)
      .compose(s -> update(id, upd))
      .compose(ur -> ur.getUpdated() == 0 ? failedFuture("Update failed") : succeededFuture())
      .setHandler(context.asyncAssertSuccess());

  }

  @Test
  public void isSuccessWhenUpdateMetadataWithEmptyMetadata(TestContext context) {

    String id = "b82eea1b-aaaa-4658-84c7-f872d713072e";
    String userId = "a218fbec-eaed-4c47-8aa1-d8bdc24c50d1";

    JsonObject crMd = new JsonObject()
      .put("createdDate", "2019-01-01T00:00:00.000+0000")
      .put("createdByUserId", userId);

    JsonObject cr = new JsonObject()
      .put("id", id)
      .put("value", 1)
      .put("metadata", crMd);

    JsonObject upd = new JsonObject()
      .put("id", id)
      .put("value", 2);

    save(id, cr)
      .compose(s -> update(id, upd))
      .compose(ur -> ur.getUpdated() == 0 ? failedFuture("Update failed") : succeededFuture())
      .setHandler(context.asyncAssertSuccess());

  }

  @Test
  public void isSuccessWhenUpdateEmptyMetadataWithMetadata(TestContext context) {

    String id = "072dbc6c-7c68-4992-8e23-6df6f26a9c5b";
    String userId = "84c759de-dd8e-4f84-9b19-4c4775dd4c99";

    JsonObject cr = new JsonObject()
      .put("id", id)
      .put("value", 1);

    JsonObject updMd = new JsonObject()
      .put("updatedDate", "2019-01-02T00:00:00.000+0000")
      .put("updatedByUserId", userId);

    JsonObject upd = new JsonObject()
      .put("id", id)
      .put("value", 2)
      .put("metadata", updMd);

    save(id, cr)
      .compose(s -> update(id, upd))
      .compose(ur -> ur.getUpdated() == 0 ? failedFuture("Update failed") : succeededFuture())
      .setHandler(context.asyncAssertSuccess());

  }

  @Test
  public void isSuccessWhenUpdateMetadataWithMetadata(TestContext context) {

    String id = "8c2fbb4c-d845-4607-b840-adbdc53fe3b5";
    String userId = "a218fbec-eaed-4c47-8aa1-d8bdc24c50d1";

    JsonObject crMd = new JsonObject()
      .put("createdDate", "2019-01-01T00:00:00.000+0000")
      .put("createdByUserId", userId);

    JsonObject cr = new JsonObject()
      .put("id", id)
      .put("value", 1)
      .put("metadata", crMd);

    JsonObject updMd = new JsonObject()
      .put("updatedDate", "2019-01-02T00:00:00.000+0000")
      .put("updatedByUserId", userId);

    JsonObject upd = new JsonObject()
      .put("id", id)
      .put("value", 2)
      .put("metadata", updMd);

    save(id, cr)
      .compose(s -> update(id, upd))
      .compose(ur -> ur.getUpdated() == 0 ? failedFuture("Update failed") : succeededFuture())
      .setHandler(context.asyncAssertSuccess());

  }

  private Future<String> save(String id, JsonObject entity) {

    Future<String> future = Future.future();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    pgClient.save(TABLE, id, entity, future);

    return future;
  }

  private Future<UpdateResult> update(String id, JsonObject entity) {

    Future<UpdateResult> future = Future.future();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    pgClient.update(TABLE, entity, id, future);

    return future;
  }


}
