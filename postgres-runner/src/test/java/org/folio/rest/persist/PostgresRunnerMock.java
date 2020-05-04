package org.folio.rest.persist;

import io.vertx.core.Promise;
import java.io.IOException;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;

public class PostgresRunnerMock extends PostgresRunner {
  private static final Logger log = LoggerFactory.getLogger(PostgresRunnerMock.class);

  static boolean startInvoked;
  static boolean stopInvoked;
  static boolean getInvoked;
  static boolean postInvoked;
  static PostgresConfig postgresConfig;

  public PostgresRunnerMock() {
    super();
    startInvoked = false;
    stopInvoked = false;
    getInvoked = false;
    postInvoked = false;
  }

  @Override
  PostgresProcess startPostgres(PostgresConfig postgresConfig) throws IOException {
    log.debug("startPostgres(PostgresConfig)");
    PostgresRunnerMock.postgresConfig = postgresConfig;
    startInvoked = true;
    return null;
  }

  @Override
  public void stop(Promise<Void> startPromise) throws Exception {
    log.debug("stop(Future)");
    super.stop(startPromise);
    stopInvoked = true;
  }

  @Override
  void get(RoutingContext request) {
    log.debug("get(RoutingContext)");
    if (request != null) {
      super.get(request);
    }
    getInvoked = true;
  }

  @Override
  void post(RoutingContext request) {
    log.debug("post(RoutingContext)");
    if (request != null) {
      super.post(request);
    }
    postInvoked = true;
  }

  @Override
  void undeploy() {
    log.debug("undeploy()");
  }

  @Override
  void closeVertx() {
    log.debug("closeVertx()");
    // do not close because the test needs it
  }
}
