package org.folio.rest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

public class RestLauncherTest {

  private static final String DISABLE_METRICS = "-Dvertx.metrics.options.enabled=false";
  private static final String ENABLE_METRICS = "-Dvertx.metrics.options.enabled=true";

  @Test
  public void testMetricsOptions01() {
    DummyLauncher launcher = new DummyLauncher();
    launcher.dispatch(new String[] { "run", TestVerticle.class.getName() });
    assertFalse(launcher.enabled);
  }

  @Test
  public void testMetricsOptions02() {
    DummyLauncher launcher = new DummyLauncher();
    launcher.dispatch(new String[] { "run", TestVerticle.class.getName(), ENABLE_METRICS });
    assertTrue(launcher.enabled);
  }

  @Test
  public void testMetricsOptions03() {
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
