package org.folio.rest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

@TestMethodOrder(OrderAnnotation.class)
public class RestLauncherTest {

  private static final String DISABLE_METRICS = "-Dvertx.metrics.options.enabled=false";
  private static final String ENABLE_METRICS = "-Dvertx.metrics.options.enabled=true";

  @Test
  @Order(1)
  public void isMetricsDisabledByDefault() {
    DummyLauncher launcher = new DummyLauncher();
    launcher.dispatch(new String[] { "run", TestVerticle.class.getName() });
    assertFalse(launcher.enabled);
  }

  @Test
  @Order(2)
  public void canEnableMetrics() {
    DummyLauncher launcher = new DummyLauncher();
    launcher.dispatch(new String[] { "run", TestVerticle.class.getName(), ENABLE_METRICS });
    assertTrue(launcher.enabled);
  }

  @Test
  @Order(3)
  public void canDisableMetrics() {
    DummyLauncher launcher = new DummyLauncher();
    launcher.dispatch(new String[] { "run", TestVerticle.class.getName(), DISABLE_METRICS });
    assertFalse(launcher.enabled);
  }

  private static class DummyLauncher extends RestLauncher {

    private boolean enabled = false;

    @Override
    public void beforeStartingVertx(VertxOptions options) {
      super.beforeStartingVertx(options);
      enabled = options.getMetricsOptions().isEnabled();
    }

    @Override
    public void afterStartingVertx(Vertx vertx) {
      vertx.close();
    }
  }

  public static class TestVerticle extends AbstractVerticle {
  }

}
