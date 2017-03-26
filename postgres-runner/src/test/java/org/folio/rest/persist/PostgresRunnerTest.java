package org.folio.rest.persist;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
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

  @Before
  public void setUp() {
    PostgresRunner.setVertxForDeploy(rule.vertx());
  }

  /**
   * Return 2 free ports.
   * @param context where to fail on error
   * @return the 2 free ports, or null on error
   */
  private Ports getPorts(TestContext context) {
    try {
      ServerSocket socket1 = new ServerSocket(0);
      int port1 = socket1.getLocalPort();
      ServerSocket socket2 = new ServerSocket(0);
      int port2 = socket2.getLocalPort();
      socket1.close();
      socket2.close();
      return new Ports(port1, port2);
    } catch (Exception e) {
      context.fail(e);
    }
    return null;
  }

  private void mainSuccess(TestContext context, Handler<AsyncResult<Integer>> asyncHandler) {
    Ports ports = getPorts(context);
    mainSuccess(ports, context, asyncHandler);
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

  @Test(expected = IllegalArgumentException.class)
  public void runnerNoArguments() {
    PostgresRunner.main(new String [] {});
  }

  @Test(expected = IllegalArgumentException.class)
  public void waiterNoArguments() throws IOException, InterruptedException {
    PostgresWaiter.main(new String [] {});
  }

  @Test(expected = IllegalArgumentException.class)
  public void stopperNoArguments() throws IOException {
    PostgresStopper.main(new String [] {});
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

  @Test
  public void portInUseRunner(TestContext context) throws IOException {
    Async async = context.async();
    Ports ports = getPorts(context);
    ServerSocket socket = new ServerSocket(ports.port1);
    main(ports, h -> {
      try {
        socket.close();
      } catch (IOException e) {
        context.fail(e);
      }
      String message = h.cause().getMessage();
      context.assertTrue(message.contains("RunnerPort " + ports.port1 + " is already in use"), message);
      async.complete();
    });
  }

  @Test
  public void portInUsePostgres(TestContext context) throws IOException {
    Async async = context.async();
    Ports ports = getPorts(context);
    ServerSocket socket = new ServerSocket(ports.port2);
    main(ports, h -> {
      try {
        socket.close();
      } catch (IOException e) {
        context.fail(e);
      }
      answers200(h.result(), "GET", h2 -> {
        if (h2.failed()) {
          context.fail(h2.cause());
        }
        answers200(h.result(), "POST", h3 -> async.complete());
      });
    });
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
    Ports ports = getPorts(context);
    PostgresRunnerMock.main(mock, ports.port1, ports.port2, "u", "p", h -> {
      context.assertTrue(PostgresRunnerMock.getInvoked);
      context.assertTrue(PostgresRunnerMock.postInvoked);
      context.assertEquals("u", PostgresRunnerMock.postgresConfig.credentials().username());
      context.assertEquals("p", PostgresRunnerMock.postgresConfig.credentials().password());
      async.complete();
    });
  }
}
