package org.folio.rest;

import io.vertx.launcher.application.VertxApplication;
import io.vertx.launcher.application.VertxApplicationHooks;

public class RestLauncher extends VertxApplication implements VertxApplicationHooks {
  private static final RestLauchnerHooks REST_LAUCHNER_HOOKS = new RestLauchnerHooks();

  public RestLauncher(String[] args, VertxApplicationHooks hooks) {
    super(args, hooks);
  }

  public static void main(String[] args) {
    System.setProperty("vertx.logger-delegate-factory-class-name",
        "io.vertx.core.logging.Log4jLogDelegateFactory");

    VertxApplication vertxApplication = new RestLauncher(args, REST_LAUCHNER_HOOKS);
    vertxApplication.launch();
  }
}
