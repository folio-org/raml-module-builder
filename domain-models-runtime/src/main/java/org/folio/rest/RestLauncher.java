package org.folio.rest;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Launcher;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.ext.dropwizard.Match;
import io.vertx.ext.dropwizard.MatchType;

public class RestLauncher extends Launcher {

  public static void main(String[] args) {
    // System.setProperty("vertx.logger-delegate-factory-class-name",
    // "io.vertx.core.logging.Log4jLogDelegateFactory");
    new RestLauncher().dispatch(args);
  }

  @Override
  public void beforeStartingVertx(VertxOptions options) {
    // TODO Auto-generated method stub
    super.beforeStartingVertx(options);

    System.out.println("starting rest verticle service..........");
    options.setBlockedThreadCheckInterval(150000);
    options.setWarningExceptionTime(150000);
    options.setMetricsOptions(new DropwizardMetricsOptions().setEnabled(true).addMonitoredHttpServerUri(
        new Match().setValue("/.*").setType(MatchType.REGEX)));
  }

  @Override
  public void afterStartingVertx(Vertx vertx) {
    // TODO Auto-generated method stub
    super.afterStartingVertx(vertx);
  }

  @Override
  public void beforeDeployingVerticle(DeploymentOptions deploymentOptions) {
    // TODO Auto-generated method stub
    super.beforeDeployingVerticle(deploymentOptions);
  }

  @Override
  public void handleDeployFailed(Vertx vertx, String mainVerticle, DeploymentOptions deploymentOptions, Throwable cause) {
    // TODO Auto-generated method stub
    super.handleDeployFailed(vertx, mainVerticle, deploymentOptions, cause);
  }

}