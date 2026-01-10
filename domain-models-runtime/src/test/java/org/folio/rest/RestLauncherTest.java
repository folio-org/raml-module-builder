package org.folio.rest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.launcher.application.HookContext;
import io.vertx.launcher.application.VertxApplication;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

@ExtendWith(VertxExtension.class)
class RestLauncherTest {
  private Vertx vertx;

  @AfterEach
  void closeVertx(VertxTestContext vtc) {
    vertx.close()
    .onComplete(vtc.succeedingThenComplete());
  }

  @BeforeEach
  void beforeEach() {
    unsetProperties();
  }

  @AfterAll
  static void unsetProperties() {
    System.clearProperty("vertx.metrics.options.enabled");
    System.clearProperty("jmxMetricsOptions");
  }

  void assertLaunch(boolean expectedMetricsEnabled) {
    String[] args = { RestVerticle.class.getName() };
    var dummyLauncherHooks = new DummyLauncherHooks();
    VertxApplication vertxApplication = new RestLauncher(args, dummyLauncherHooks);
    assertThat(vertxApplication.launch(), is(0));
    assertThat(dummyLauncherHooks.enabled, is(expectedMetricsEnabled));
  }

  @Test
  void canDisableMetrics() {
    assertLaunch(false);
  }

  @Test
  void canEnableMetrics() {
    System.setProperty("vertx.metrics.options.enabled", "true");
    System.setProperty("jmxMetricsOptions", "{}");
    assertLaunch(true);
  }

  private class DummyLauncherHooks extends RestLauchnerHooks {

    private boolean enabled = false;

    @Override
    public void beforeStartingVertx(HookContext context) {
      enabled = context.vertxOptions().getMetricsOptions().isEnabled();
      super.beforeStartingVertx(context);
    }

    @Override
    public void afterVertxStarted(HookContext context) {
      RestLauncherTest.this.vertx = context.vertx();
    }
  }

  public static class TestVerticle extends AbstractVerticle {
  }

}
