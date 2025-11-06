package org.folio.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.MetricsUtil;

import io.vertx.core.Vertx;
import io.vertx.launcher.application.HookContext;
import io.vertx.launcher.application.VertxApplicationHooks;

public class RestLauchnerHooks implements VertxApplicationHooks {
  private static final Logger LOGGER = LogManager.getLogger(RestLauchnerHooks.class);

  @Override
  public void beforeStartingVertx(HookContext context) {
    LOGGER.info("starting rest verticle service..........");
    var options = context.vertxOptions();
    options.setBlockedThreadCheckInterval(1_500_000);
    options.setWarningExceptionTime(1_500_000);
    MetricsUtil.init(Vertx.builder());
  }
}
