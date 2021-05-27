package org.folio.rest;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Launcher;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.folio.okapi.common.MetricsUtil;

public class RestLauncher extends Launcher {

  public static void main(String[] args) {
    new RestLauncher().dispatch(args);
  }

  @Override
  public void beforeStartingVertx(VertxOptions options) {
    super.beforeStartingVertx(options);

    System.out.println("starting rest verticle service..........");
    options.setBlockedThreadCheckInterval(1500000);
    options.setWarningExceptionTime(1500000);
    MetricsUtil.init(options);
  }

  @Override
  public void afterStartingVertx(Vertx vertx) {
    super.afterStartingVertx(vertx);
  }

  @Override
  public void beforeDeployingVerticle(DeploymentOptions deploymentOptions) {
    super.beforeDeployingVerticle(deploymentOptions);
  }

  @Override
  public void handleDeployFailed(Vertx vertx, String mainVerticle, DeploymentOptions deploymentOptions, Throwable cause) {
    super.handleDeployFailed(vertx, mainVerticle, deploymentOptions, cause);
  }

}
