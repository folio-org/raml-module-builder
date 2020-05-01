package org.folio.rest.persist;

import static org.hamcrest.collection.IsMapWithSize.aMapWithSize;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

import io.vertx.core.Promise;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;

@RunWith(VertxUnitRunner.class)
public class PostgresRunnerTest {
  private static final Logger log = LoggerFactory.getLogger(PostgresRunnerTest.class);
  private Level oldLoggerLevel = null;

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  @Rule
  public Timeout timeout = Timeout.seconds(10);

  class Ports {
    int port1;
    int port2;
    public Ports(int port1, int port2) {
      this.port1 = port1;
      this.port2 = port2;
    }
  }

  private java.util.logging.Level setLoggerLevel(Level newLevel) {
    java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
    Level oldLevel = rootLogger.getLevel();
    rootLogger.setLevel(newLevel);
    for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
      if (handler instanceof FileHandler) {
        handler.setLevel(newLevel);
      }
    }
    return oldLevel;
  }

  @Before
  public void setUp() {
    PostgresRunner.setVertxForDeploy(rule.vertx());
    oldLoggerLevel = LogManager.getLogManager().getLogger("").getLevel();
  }

  @After
  public void tearDown() {
    setLoggerLevel(oldLoggerLevel);
  }

  /**
   * Return 2 free ports.
   * @param context where to fail on error
   * @return the 2 free ports, or null on error
   */
  private Ports getPorts() {
    try {
      ServerSocket socket1 = new ServerSocket(0);
      int port1 = socket1.getLocalPort();
      ServerSocket socket2 = new ServerSocket(0);
      int port2 = socket2.getLocalPort();
      socket1.close();
      socket2.close();
      return new Ports(port1, port2);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void mainSuccess(TestContext context, Handler<AsyncResult<Integer>> asyncHandler) {
    mainSuccess(getPorts(), context, asyncHandler);
  }

  private void mainSuccess(Ports ports, TestContext context, Handler<AsyncResult<Integer>> asyncHandler) {
    main(ports, h -> {
      if (h.failed()) {
        context.fail(h.cause());
      }
      asyncHandler.handle(h);
    });
  }

  private void main(Ports ports, Handler<AsyncResult<Integer>> asyncHandler) {
    PostgresRunnerMock.main(new PostgresRunnerMock(), ports.port1, ports.port2, "u", "p", h -> {
      log.debug("main(...) succeeded=" + h.succeeded());
      if (h.succeeded()) {
        asyncHandler.handle(Future.succeededFuture(ports.port1));
      } else {
        asyncHandler.handle(Future.failedFuture(h.cause()));
      }
    });
  }

  private void answers200(int port, String method, Handler<AsyncResult<Void>> asyncHandler) {
    try {
      URL url = new URL("http://localhost:" + port);
      log.debug("{0} {1}", method, url);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod(method);
      if (conn.getResponseCode() != 200) {
        asyncHandler.handle(Future.failedFuture(new IllegalStateException("responseCode = " + conn.getResponseCode())));
      } else {
        asyncHandler.handle(Future.succeededFuture());
      }
      log.debug("{0} {1} response={2}", method, url, conn.getResponseCode());
      conn.disconnect();
    } catch (Exception e) {
      asyncHandler.handle(Future.failedFuture(e));
    }
  }

  private void assertEquals(JsonObject config, int runnerPort, int postgresPort, String username, String password) {
    assertThat(config.getMap(), allOf(
        is(aMapWithSize(4)),
        hasEntry("runnerPort", (Object) runnerPort),
        hasEntry("postgresPort", (Object) postgresPort),
        hasEntry("username", username),
        hasEntry("password", password)
        ));
  }

  @Test(expected = IllegalArgumentException.class)
  public void configOneArgument() {
    PostgresRunner.config(new String [] { "5555" }, Collections.emptyMap());
  }

  Map<String, String> envNine = new HashMap<>(4);
  {
    envNine.put("DB_RUNNER_PORT", "9");
    envNine.put("DB_PORT", "99");
    envNine.put("DB_USERNAME", "nine");
    envNine.put("DB_PASSWORD", "ninetynine");
  }

  @Test
  public void configCommandLine() {
    assertEquals(PostgresRunner.config(new String [] {"8", "88", "eight", "aftereight"}, envNine),
        8, 88, "eight", "aftereight");
  }

  @Test(expected = NumberFormatException.class)
  public void configCommandLineInvalidPort() {
    PostgresRunner.config(new String [] {"8", "8 8", "eight", "aftereight"}, envNine);
  }

  @Test
  public void configEnv() {
    assertEquals(PostgresRunner.config(new String [] {}, envNine),
        9, 99, "nine", "ninetynine");
  }

  @Test(expected = IllegalArgumentException.class)
  public void configEnvMissingUsername() {
    Map<String,String> env = new HashMap<>(envNine);
    env.remove("DB_USERNAME");
    assertEquals(PostgresRunner.config(new String [] {}, env),
        9, 99, "nine", "ninetynine");
  }

  @Test
  public void configDefault() {
    assertEquals(PostgresRunner.config(new String [] {}, Collections.emptyMap()),
        6001, 6000, "username", "password");
  }

  @Test
  public void runnerPortCommandLine() {
    assertThat(PostgresWaiter.runnerPort(new String [] { "11" }, envNine), is(11));
  }

  @Test
  public void runnerPortEnv() {
    assertThat(PostgresWaiter.runnerPort(new String [] { }, envNine), is(9));
  }

  @Test
  public void runnerPortDefault() {
    assertThat(PostgresWaiter.runnerPort(new String [] { }, Collections.emptyMap()), is(6001));
  }

  @Test(expected = IllegalArgumentException.class)
  public void runnerPort2Arguments() {
    PostgresWaiter.runnerPort(new String [] { "1" , "2" }, envNine);
  }

  @Test
  public void testPost(TestContext context) {
    Async async = context.async();
    mainSuccess(context, result -> answers200(result.result(), "POST", h -> async.complete()));
  }

  @Test
  public void testGet(TestContext context) {
    Async async = context.async();
    mainSuccess(context, result -> answers200(result.result(), "GET", h -> async.complete()));
  }

  @Test
  public void runWaiterStopper(TestContext context) throws IOException, InterruptedException {
    Async async = context.async();
    mainSuccess(context, h -> {
      int port = h.result();
      try {
        PostgresWaiter .main(new String [] { Integer.toString(port) });
        PostgresStopper.main(new String [] { Integer.toString(port) });
      } catch (IOException | InterruptedException e) {
        context.fail(e);
      }
      async.complete();
    });
  }

  @Test(timeout=30000)
  public void waiterWithoutRunner() throws Exception {
    String port = ""  + getPorts().port1;
    int oldSecondsToSleep = PostgresWaiter.secondsToSleep;
    try {
      PostgresWaiter.secondsToSleep = 0;
      PostgresWaiter.main(new String [] { port });
    } catch (IOException e) {
      if (! e.getMessage().contains(port)) {
        throw e;
      }
    } finally {
      PostgresWaiter.secondsToSleep = oldSecondsToSleep;
    }
  }

  /**
   * Create a server that listens on the port and returns status code 500.
   * It is created on rule.vertx(), it gets closed automatically after the test.
   */
  private void createHttpServer500(TestContext context, int port, Handler<AsyncResult<Void>> handler) {
    rule.vertx().createHttpServer()
      .requestHandler(request -> request.response().setStatusCode(500).end())
      .listen(port, result -> handler.handle(Future.succeededFuture(null)));
  }

  @Test
  public void wrongResponseCode(TestContext context) throws Exception {
    int port = getPorts().port1;
    createHttpServer500(context, port, context.asyncAssertSuccess(running -> {
      rule.vertx().executeBlocking(future -> {
        try {
          PostgresWaiter.main(new String [] { "" + port });
          context.fail("Exception expected");
        } catch (Exception e) {
          if (e instanceof IllegalStateException && e.getMessage().contains("=500")) {
            future.complete();
          } else {
            context.fail(e);
          }
        }
      }, context.asyncAssertSuccess());
    }));
  }

  @Test(expected = RuntimeException.class)
  public void illegalRunnerPort() throws Exception {
    PostgresStopper.main(new String [] { "999888" });
  }

  @Test
  public void portInUseRunner(TestContext context) throws IOException {
    Ports ports = getPorts();
    createHttpServer500(context, ports.port1, context.asyncAssertSuccess(result -> {
      Level oldLevel = setLoggerLevel(Level.OFF);  // don't log expected failure
      main(ports, context.asyncAssertFailure(exception -> {
        setLoggerLevel(oldLevel);
        String message = exception.getMessage();
        context.assertTrue(message.contains("RunnerPort " + ports.port1 + " is already in use"), message);
      }));
    }));
  }

  @Test
  public void portInUseRunner2(TestContext context) throws IOException {
    Ports ports = getPorts();
    createHttpServer500(context, ports.port1, context.asyncAssertSuccess(result -> {
      Level oldLevel = setLoggerLevel(Level.OFF);  // don't log expected failure
      PostgresRunner.main(new PostgresRunner(), ports.port1, ports.port2, "username", "password",
          context.asyncAssertFailure(exception -> {
            setLoggerLevel(oldLevel);
            String message = exception.getMessage();
            context.assertTrue(message.contains("RunnerPort " + ports.port1 + " is already in use"), message);
          }));
    }));
  }

  @Test
  public void portInUsePostgres(TestContext context) throws IOException {
    Ports ports = getPorts();
    createHttpServer500(context, ports.port2, context.asyncAssertSuccess(pgserver -> {
      main(ports, context.asyncAssertSuccess(runnerserver -> {
        answers200(ports.port1, "GET", context.asyncAssertSuccess(get -> {
          answers200(ports.port1, "POST", context.asyncAssertSuccess());
        }));
      }));
    }));
  }

  @Test
  public void requests(TestContext context) throws IOException {
    PostgresRunner mock = new PostgresRunnerMock() {
      @Override
      PostgresProcess startPostgres(PostgresConfig postgresConfig) throws IOException {
        getRequests.add(null);
        postRequests.add(null);
        return super.startPostgres(postgresConfig);
      }
    };

    Async async = context.async();
    Ports ports = getPorts();
    PostgresRunnerMock.main(mock, ports.port1, ports.port2, "u", "p", h -> {
      context.assertTrue(PostgresRunnerMock.getInvoked);
      context.assertTrue(PostgresRunnerMock.postInvoked);
      context.assertEquals("u", PostgresRunnerMock.postgresConfig.credentials().username());
      context.assertEquals("p", PostgresRunnerMock.postgresConfig.credentials().password());
      async.complete();
    });
  }

  @Test
  public void stopPostgres(TestContext context) {
    PostgresProcess postgresProcessMock = mock(PostgresProcess.class);
    class MyPostgresRunner extends PostgresRunner {
      @Override public void start(Promise<Void> startPromise) {
        postgresProcess = postgresProcessMock;
        startPromise.complete();
      }
    };

    MyPostgresRunner myPostgresRunner = new MyPostgresRunner();
    PostgresRunner.main(myPostgresRunner, new JsonObject(), context.asyncAssertSuccess(deploy -> {
      rule.vertx().undeploy(deploy, context.asyncAssertSuccess(undeploy -> {
        context.verify(block -> verify(postgresProcessMock).stop());
      }));
    }));
  }
}
