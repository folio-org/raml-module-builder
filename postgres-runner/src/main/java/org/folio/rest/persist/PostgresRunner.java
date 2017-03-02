package org.folio.rest.persist;

import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;
import ru.yandex.qatools.embed.postgresql.distribution.Version;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

/**
 * Starts embedded Postgres if the desired PostgresPort if free, and provide a port that allows to stop it.
 *
 * Arguments: PostgresRunnerPort PostgresPort UserName UserPassword
 *
 * Example usage:
 *
 * Start Postgres at port 5433, in addition open port 5434 for PostgresWaiter and PostgresStopper:
 *
 * java -cp target/postgres-runner-fat.jar org.folio.rest.persist.PostgresRunner 5434 5433 postgres postgres &
 *
 * This runs in the background.
 *
 * Wait until Postgres is available:
 *
 * java -cp target/postgres-runner-fat.jar org.folio.rest.persist.PostgresWaiter 5434
 *
 * Now use Postgres at port 5433.
 *
 * Afterwards stop Postgres and PostgresRunner:
 *
 * java -cp target/postgres-runner-fat.jar org.folio.rest.persist.PostgresStopper 5434
 */
public class PostgresRunner extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(PostgresRunner.class);
  private static Vertx vertxForDeploy;
  private boolean postgresRuns = false;
  private PostgresProcess postgresProcess;
  HttpServer runnerServer;
  List<RoutingContext> getRequests = new ArrayList<>();
  List<RoutingContext> postRequests = new ArrayList<>();

  /**
   * Set the Vertx that all following calls to the static main methods use for deploying this Verticle.
   * @param vertx the Vertx
   */
  public static void setVertxForDeploy(Vertx vertx) {
    vertxForDeploy = vertx;
  }

  public static void main(String [] args) {
    if (args.length != 4) {
      throw new IllegalArgumentException("required arguments:\n"
          + "PostgresRunnerPort PostgresPort UserName UserPassword\n");
    }

    main(Integer.parseUnsignedInt(args[0]), Integer.parseUnsignedInt(args[1]), args[2], args[3]);
  }

  public static void main(int runnerPort, int postgresPort, String username, String password) {
    main(new PostgresRunner(), runnerPort, postgresPort, username, password, null);
  }

  public static void main(PostgresRunner postgresRunner, int runnerPort, int postgresPort, String username, String password) {
    main(postgresRunner, runnerPort, postgresPort, username, password, null);
  }

  public static void main(PostgresRunner postgresRunner, int runnerPort, int postgresPort, String username, String password,
      Handler<AsyncResult<String>> asyncHandler) {
    JsonObject config = new JsonObject()
        .put("runnerPort", runnerPort)
        .put("postgresPort", postgresPort)
        .put("userName", username)
        .put("password", password);
    DeploymentOptions options = new DeploymentOptions().setConfig(config).setWorker(true);
    if (vertxForDeploy == null) {
      vertxForDeploy = Vertx.vertx();
    }
    vertxForDeploy.deployVerticle(postgresRunner, options, result -> {
      if (asyncHandler != null) {
        asyncHandler.handle(result);
      }
    });
  }

  @Override
  public void start(Future<Void> startFuture) {
    log.debug("start(Future)");

    int runnerPort   = config().getInteger("runnerPort");
    int postgresPort = config().getInteger("postgresPort");

    if (isPortInUse(runnerPort)) {
      startFuture.fail(new IOException("Quitting because PostgresRunnerPort is already in use: " + runnerPort));
      return;
    }

    postgresRuns = isPortInUse(postgresPort);

    listen(runnerPort, h -> {
      log.debug("listenHandler");
      if (! postgresRuns) {
        vertx.executeBlocking(future -> {
          postgresProcess = startPostgres(postgresPort,
              config().getString("username"),
              config().getString("password"));
          future.complete();
        }, result -> whenPostgresRuns(startFuture));
      } else {
        whenPostgresRuns(startFuture);
      }
    });
  }

  public void whenPostgresRuns(Future<Void> startFuture) {
    log.debug("whenPostgresRuns(Future)");

    postgresRuns = true;

    for (RoutingContext request : getRequests) {
      get(request);
    }
    for (RoutingContext request : postRequests) {
      post(request);
    }
    if (! postRequests.isEmpty()) {
      undeploy();
    }
    startFuture.complete();
  }

  @Override
  public void stop(Future<Void> stopFuture) throws InterruptedException {
    log.debug("stop(Future)");

    Future<Void> serverFuture = Future.future();
    Future<Void> postgresFuture = Future.future();

    if (runnerServer == null) {
      serverFuture.complete();
    } else {
      runnerServer.close(serverFuture.completer());
      runnerServer = null;
    }

    if (postgresProcess == null) {
      postgresFuture.complete();
    } else {
      PostgresProcess oldPostgresProcess = postgresProcess;
      postgresProcess = null;
      vertx.executeBlocking(future -> oldPostgresProcess.stop(), h -> postgresFuture.complete());
    }

    Handler<AsyncResult<Void>> handler = h -> {
      if (serverFuture.isComplete() && postgresFuture.isComplete()) {
        log.debug("stop(Future) complete");
        stopFuture.complete();
      }
    };
    serverFuture.setHandler(handler);
    postgresFuture.setHandler(handler);
  }

  @SuppressWarnings("squid:S1166")  // "Exception handlers should preserve the original exceptions"
  static boolean isPortInUse(int port) {
    try {
      new Socket("localhost", port).close();
      // creating socket was successful because someone has answered
      return true;
    } catch (IOException e) {
      // no endpoint at that port because port is free
      return false;
    }
  }

  void listen(int port, Handler<AsyncResult<HttpServer>> listenHandler) {
    log.debug("listen(port, listenHandler)");

    runnerServer = vertx.createHttpServer();
    Router router = Router.router(vertx);

    router.get().handler(request -> {
      if (postgresRuns) {
        get(request);
      } else {
        getRequests.add(request);
      }
    });
    router.post().handler(request -> {
      if (postgresRuns) {
        post(request);
        undeploy();
      } else {
        postRequests.add(request);
      }
    });

    runnerServer.requestHandler(router::accept).listen(port, listenHandler);
  }

  PostgresProcess startPostgres(PostgresConfig postgresConfig) throws IOException {
    log.debug("startPostgres(PostgresConfig)");
    return PostgresStarter.getDefaultInstance().prepare(postgresConfig).start();
  }

  PostgresProcess startPostgres(int postgresPort, String username, String password) {
    log.debug("startPostgres(postgresPort, username, password)");
    try {
      final PostgresConfig config = new PostgresConfig(
          Version.Main.PRODUCTION,
          new PostgresConfig.Net("localhost", postgresPort),
          new PostgresConfig.Storage("database"),
          new PostgresConfig.Timeout(),
          new PostgresConfig.Credentials(username, password));

      String locale = "en_US.UTF-8";
      if (System.getProperty("os.name").toLowerCase().contains("win")) {
        locale = "american_usa";
      }
      config.getAdditionalInitDbParams().addAll(Arrays.asList(
        "-E", "UTF-8",
        "--locale", locale
      ));

      return startPostgres(config);
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  void get(RoutingContext request) {
    log.debug("get(RountingContext)");
    HttpServerResponse response = request.response();
    response.putHeader("content-type", "text/plain");
    response.end("running");
  }

  void post(RoutingContext request) {
    log.debug("post(RountingContext)");
    HttpServerResponse response = request.response();
    response.putHeader("content-type", "text/plain");
    response.end("stopped");
  }

  void undeploy() {
    log.debug("undeploy()");
    vertx.undeploy(deploymentID(), undeployed -> closeVertx());
  }

  /**
   * This closes the Vertx and is invoked after undeploying this Verticle.
   */
  void closeVertx() {
    log.debug("closeVertx()");
    vertx.close();
  }
}
