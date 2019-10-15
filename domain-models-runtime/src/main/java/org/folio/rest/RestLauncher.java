package org.folio.rest;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Launcher;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.ext.dropwizard.Match;
import io.vertx.ext.dropwizard.MatchType;

public class RestLauncher extends Launcher {

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4j2LogDelegateFactory");
  }

  public static void main(String[] args) {
    new RestLauncher().dispatch(args);
  }

  @Override
  public void beforeStartingVertx(VertxOptions options) {
    // TODO Auto-generated method stub
    super.beforeStartingVertx(options);

    System.out.println("starting rest verticle service..........");
    options.setBlockedThreadCheckInterval(1500000);
    options.setWarningExceptionTime(1500000);
    boolean enabled = options.getMetricsOptions().isEnabled();
    options.setMetricsOptions(new DropwizardMetricsOptions().setEnabled(enabled).addMonitoredHttpServerUri(
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
