package org.folio.rest;

import io.vertx.core.Vertx;
import io.vertx.core.VertxBuilder;
import io.vertx.core.VertxOptions;
import io.vertx.launcher.application.HookContext;
import io.vertx.launcher.application.VertxApplication;
import io.vertx.launcher.application.VertxApplicationHooks;
import org.folio.okapi.common.MetricsUtil;

public class RestLauncher extends VertxApplication {

  public static void main(String[] args) {
    System.setProperty("vertx.logger-delegate-factory-class-name",
        "io.vertx.core.logging.Log4jLogDelegateFactory");
    VertxApplicationHooks hooks = new VertxApplicationHooks() {
      @Override
      public void beforeStartingVertx(HookContext context) {
        System.out.println("starting rest verticle service..........");
        VertxOptions options = context.vertxOptions();
        options.setBlockedThreadCheckInterval(1500000);
        options.setWarningExceptionTime(1500000);
        VertxBuilder vb = Vertx.builder();
        MetricsUtil.init(vb);
      }
    };

    VertxApplication vertxApplication = new RestLauncher(args, hooks);
    vertxApplication.launch();
  }

  public RestLauncher(String[] args, VertxApplicationHooks hooks) {
    super(args, hooks);
  }
}
