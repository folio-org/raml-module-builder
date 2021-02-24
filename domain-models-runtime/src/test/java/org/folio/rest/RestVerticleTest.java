package org.folio.rest;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.rest.resource.interfaces.PostDeployVerticle;
import org.folio.rest.resource.interfaces.ShutdownAPI;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class RestVerticleTest implements InitAPI, PostDeployVerticle, ShutdownAPI {
  private static final Logger LOGGER = LogManager.getLogger(RestVerticleTest.class);

  private static Vertx vertx;
  private static int port;
  private static Boolean initResult = null;
  private static int initCalls;
  private static int shutdownCalls;

  @BeforeClass
  public static void setUp(TestContext context) {
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> resultHandler) {
    LOGGER.info("Init handler called");
    initCalls++;
    if (initResult == null) {
      throw new RuntimeException("init exception");
    } else if (initResult == false) {
      resultHandler.handle(Future.failedFuture("init failed"));
    } else {
      resultHandler.handle(Future.succeededFuture(initResult));
    }
  }

  @Override
  public void shutdown(Vertx vertx, Context context, Handler<AsyncResult<Void>> handler) {
    LOGGER.info("shutdown handler called");
    shutdownCalls++;
    handler.handle(Future.succeededFuture());
  }

  @Test
  public void initHookTrue(TestContext context) {
    shutdownCalls = 0;
    initCalls = 0;
    initResult = true;
    JsonObject config = new JsonObject();
    config.put("packageOfImplementations", "org.folio.rest");
    config.put("http.port", port);
    vertx.deployVerticle(RestVerticle.class, new DeploymentOptions().setConfig(config))
        .onComplete(context.asyncAssertSuccess(res -> {
          context.assertEquals(2, initCalls);
          context.assertEquals(0, shutdownCalls);
          vertx.undeploy(res).onComplete(context.asyncAssertSuccess(x -> {
            context.assertEquals(2, initCalls);
            context.assertEquals(1, shutdownCalls);
          }));
        }));
  }

  @Test
  public void initHookFail(TestContext context) {
    shutdownCalls = 0;
    initCalls = 0;
    initResult = false;
    JsonObject config = new JsonObject();
    config.put("packageOfImplementations", "org.folio.rest");
    config.put("http.port", port);
    vertx.deployVerticle(RestVerticle.class, new DeploymentOptions().setConfig(config))
        .onComplete(context.asyncAssertFailure(cause -> {
          context.assertEquals("init failed", cause.getMessage());
          context.assertEquals(1, initCalls);
          context.assertEquals(0, shutdownCalls);
        }));
  }

  @Test
  public void initHookException(TestContext context) {
    shutdownCalls = 0;
    initCalls = 0;
    initResult = null;
    JsonObject config = new JsonObject();
    config.put("packageOfImplementations", "org.folio.rest");
    config.put("http.port", port);
    vertx.deployVerticle(RestVerticle.class, new DeploymentOptions().setConfig(config))
        .onComplete(context.asyncAssertFailure(cause -> {
          context.assertNull(cause.getMessage());
          context.assertEquals(1, initCalls);
          context.assertEquals(0, shutdownCalls);
        }));
  }


}
