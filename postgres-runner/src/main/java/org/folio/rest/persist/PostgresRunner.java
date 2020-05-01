package org.folio.rest.persist;

import io.vertx.core.Promise;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;
import ru.yandex.qatools.embed.postgresql.distribution.Version;

/**
 * Starts embedded Postgres if the desired PostgresPort if free, and provide a port that allows to stop it.
 *
 * <p>Arguments: PostgresRunnerPort PostgresPort UserName UserPassword
 *
 * <p>If no arguments are provided use these environment variables: DB_RUNNER_PORT, DB_PORT, DB_USERNAME, DB_PASSWORD
 *
 * <p>As last resort use the default configuration: 6001, 6000, username, password.
 *
 * <p>Example usage:
 *
 * <p>Start Postgres at port 5433, in addition open port 5434 for PostgresWaiter and PostgresStopper:
 *
 * <p>java -cp target/postgres-runner-fat.jar org.folio.rest.persist.PostgresRunner 5434 5433 postgres postgres &
 *
 * <p>This runs in the background.
 *
 * <p>Wait until Postgres is available:
 *
 * <p>java -cp target/postgres-runner-fat.jar org.folio.rest.persist.PostgresWaiter 5434
 *
 * <p>Now use Postgres at port 5433.
 *
 * <p>Afterwards stop Postgres and PostgresRunner:
 *
 * <p>java -cp target/postgres-runner-fat.jar org.folio.rest.persist.PostgresStopper 5434
 */
public class PostgresRunner extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(PostgresRunner.class);
  private static final String USERNAME = "username";
  @SuppressWarnings("squid:S2068")  // suppress "Credentials should not be hard-coded"
  // This is safe because PostgresRunner is used for unit tests only, a
  // non-default password can be set, but setting it in the pom.xml of users
  // of PostgresRunner is not an improvement.
  private static final String PASSWORD = "password";
  private static Vertx vertxForDeploy;
  HttpServer runnerServer;
  List<RoutingContext> getRequests = new ArrayList<>();
  List<RoutingContext> postRequests = new ArrayList<>();
  PostgresProcess postgresProcess;
  private boolean postgresRuns = false;

  /**
   * Set the Vertx that all following calls to the static main methods use for deploying this Verticle.
   * @param vertx the Vertx
   */
  public static void setVertxForDeploy(Vertx vertx) {
    vertxForDeploy = vertx;
  }

  public static void main(String [] args) {
    main(new PostgresRunner(), config(args, System.getenv()), null);
  }

  static JsonObject config(String [] args, Map<String,String> env) {
    if (args.length == 4) {
      return config(args[0], args[1], args[2], args[3]);
    }

    if (args.length != 0) {
      throw new IllegalArgumentException("Found " + args.length + " arguments, expected 0 or 4:\n"
          + "[PostgresRunnerPort PostgresPort UserName UserPassword]\n");
    }

    String runnerPort = getenv(env, "DB_RUNNER_PORT");
    String postgresPort = getenv(env, "DB_PORT");
    String username = getenv(env, "DB_USERNAME");
    String password = getenv(env, "DB_PASSWORD");
    if (runnerPort == null && postgresPort == null && username == null && password == null) {
      // default configuration
      return config(6001, 6000, USERNAME, PASSWORD);
    }

    if (runnerPort == null || postgresPort == null || username == null || password == null) {
      throw new IllegalArgumentException("All 4 environment variables must be defined: "
          + "DB_RUNNER_PORT, DB_PORT, DB_USERNAME, DB_PASSWORD");
    }

    return config(runnerPort, postgresPort, username, password);
  }

  /**
   * Return env(name). If not found also try deprecated lower case dot separated name.
   * getenv("DB_PORT") checks "DB_PORT" and "db.port".
   * @return the found value, or null if both names do not exist.
   */
  static String getenv(Map<String,String> env, String name) {
    String value = env.get(name);
    if (value != null) {
      return value;
    }
    return env.get(name.toLowerCase(Locale.ROOT).replace('_', '.'));
  }

  private static JsonObject config(int runnerPort, int postgresPort, String username, String password) {
    return new JsonObject()
        .put("runnerPort", runnerPort)
        .put("postgresPort", postgresPort)
        .put(USERNAME, username)
        .put(PASSWORD, password);
  }

  private static JsonObject config(String runnerPort, String postgresPort, String username, String password) {
    return config(Integer.parseInt(runnerPort), Integer.parseInt(postgresPort), username, password);
  }

  public static void main(int runnerPort, int postgresPort, String username, String password) {
    main(new PostgresRunner(), runnerPort, postgresPort, username, password, null);
  }

  public static void main(PostgresRunner postgresRunner, int runnerPort, int postgresPort, String username, String password) {
    main(postgresRunner, runnerPort, postgresPort, username, password, null);
  }

  public static void main(PostgresRunner postgresRunner, int runnerPort, int postgresPort, String username, String password,
      Handler<AsyncResult<String>> asyncHandler) {
    main(postgresRunner, config(runnerPort, postgresPort, username, password), asyncHandler);
  }

  static void main(PostgresRunner postgresRunner, JsonObject config, Handler<AsyncResult<String>> asyncHandler) {
    DeploymentOptions options = new DeploymentOptions().setConfig(config).setWorker(true);
    if (vertxForDeploy == null) {
      vertxForDeploy = Vertx.vertx();
      vertxForDeploy.exceptionHandler(ex -> log.error("Unhandled exception caught by vertx", ex));
    }
    vertxForDeploy.deployVerticle(postgresRunner, options, result -> {
      if (result.failed()) {
        log.error(result.cause().getMessage(), result.cause());
      }
      if (asyncHandler != null) {
        asyncHandler.handle(result);
      }
    });
  }

  @Override
  public void start(Promise<Void> startPromise) {
    log.debug("start(Future)");

    int runnerPort   = config().getInteger("runnerPort");
    int postgresPort = config().getInteger("postgresPort");

    if (isPortInUse(runnerPort)) {
      undeploy();
      startPromise.fail(new IOException("Quitting because PostgresRunnerPort " + runnerPort + " is already in use."));
      return;
    }

    postgresRuns = isPortInUse(postgresPort);

    listen(runnerPort, h -> {
      log.debug("listenHandler");
      if (! postgresRuns) {
        vertx.executeBlocking(future -> {
          postgresProcess = startPostgres(postgresPort,
              config().getString(USERNAME),
              config().getString(PASSWORD));
          future.complete();
        }, result -> whenPostgresRuns(startPromise));
      } else {
        whenPostgresRuns(startPromise);
      }
    });
  }

  public void whenPostgresRuns(Promise<Void> startPromise) {
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
    startPromise.complete();
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    log.debug("stop(Future)");

    Promise<Void> serverPromise = Promise.promise();
    Promise<Void> postgresPromise = Promise.promise();

    if (runnerServer == null) {
      serverPromise.complete();
    } else {
      runnerServer.close(serverPromise.future());
      runnerServer = null;
    }

    if (postgresProcess == null) {
      postgresPromise.complete();
    } else {
      PostgresProcess oldPostgresProcess = postgresProcess;
      postgresProcess = null;
      vertx.executeBlocking(future -> {
        oldPostgresProcess.stop();
        future.complete();
      }, h -> postgresPromise.complete());
    }

    Handler<AsyncResult<Void>> handler = h -> {
      if (serverPromise.future().isComplete() && postgresPromise.future().isComplete()) {
        log.debug("stop(Future) complete");
        stopPromise.complete();
      }
    };
    serverPromise.future().onComplete(handler);
    postgresPromise.future().onComplete(handler);
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
    log.debug("listen(port={0}, listenHandler)", port);

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

    runnerServer.requestHandler(router).listen(port, listenHandler);
  }

  PostgresProcess startPostgres(PostgresConfig postgresConfig) throws IOException {
    log.debug("startPostgres(PostgresConfig)");
    return PostgresStarter.getDefaultInstance().prepare(postgresConfig).start();
  }

  PostgresProcess startPostgres(int postgresPort, String username, String password) {
    log.debug("startPostgres(postgresPort={0}, username={1}, password [not shown])", postgresPort, username);
    try {
      final PostgresConfig config = new PostgresConfig(
          Version.Main.V10,
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
    } catch (IOException e) {
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
