package org.folio.rest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
@TestMethodOrder(OrderAnnotation.class)
public class RestLauncherTest {

  private static final String JAVA_TEST_VERTICLE =
      "java:" + TestVerticle.class.getCanonicalName();
  private static final String DISABLE_METRICS = "-Dvertx.metrics.options.enabled=false";
  private static final String ENABLE_METRICS = "-Dvertx.metrics.options.enabled=true";
  private static final String ENABLE_JMX = "-DjmxMetricsOptions={\"domain\":\"org.folio\"}";

  private Vertx vertx;

  @AfterEach
  void closeVertx(VertxTestContext vtc) {
    vertx.close(vtc.succeedingThenComplete());
  }

  @Test
  @Order(1)
  public void isMetricsDisabledByDefault() {
    DummyLauncher launcher = new DummyLauncher();
    launcher.dispatch(new String[] { "run", JAVA_TEST_VERTICLE });
    assertFalse(launcher.enabled);
  }

  @Test
  @Order(2)
  public void canEnableMetrics() {
    DummyLauncher launcher = new DummyLauncher();
    launcher.dispatch(new String[] { "run", JAVA_TEST_VERTICLE, ENABLE_METRICS, ENABLE_JMX});
    assertTrue(launcher.enabled);
  }

  @Test
  @Order(3)
  public void canDisableMetrics() {
    DummyLauncher launcher = new DummyLauncher();
    launcher.dispatch(new String[] { "run", JAVA_TEST_VERTICLE, DISABLE_METRICS });
    assertFalse(launcher.enabled);
  }

  private class DummyLauncher extends RestLauncher {

    private boolean enabled = false;

    @Override
    public void beforeStartingVertx(VertxOptions options) {
      super.beforeStartingVertx(options);
      enabled = options.getMetricsOptions().isEnabled();
    }

    @Override
    public void afterStartingVertx(Vertx vertx) {
      RestLauncherTest.this.vertx = vertx;
    }
  }

  public static class TestVerticle extends AbstractVerticle {
  }

}
