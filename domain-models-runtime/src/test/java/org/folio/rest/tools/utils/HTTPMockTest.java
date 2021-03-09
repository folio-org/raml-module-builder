package org.folio.rest.tools.utils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.tools.client.HttpClientFactory;
import org.folio.rest.tools.client.Response;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.folio.rest.tools.client.test.HttpClientMock2;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author shale
 *
 */
@RunWith(VertxUnitRunner.class)
public class HTTPMockTest {

  @Test
  public void testWithMock(TestContext context) {
    int port = NetworkUtils.nextFreePort(); // most certainly a failure if mock was not in use
    Vertx vertx = Vertx.vertx();
    Async async = context.async();
    vertx.runOnContext(x -> {
      Vertx.currentContext().config().put(HttpClientMock2.MOCK_MODE, "foo");
      HttpClientInterface client = HttpClientFactory.getHttpClient("localhost", port, "zxc");
      try {
        CompletableFuture<Response> cf = client.request("auth_test2");
        Response response = cf.get(1, TimeUnit.MILLISECONDS);
        context.assertEquals("1", response.getBody().getString("id"));
      } catch (Exception e) {
        context.fail(e);
      }
      async.complete();
    });
    async.await();
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void testWithNoMock(TestContext context) {
    int port = NetworkUtils.nextFreePort(); // no mock so we expect failure
    Vertx vertx = Vertx.vertx();
    Async async = context.async();
    vertx.runOnContext(x -> {
      HttpClientInterface client = HttpClientFactory.getHttpClient("localhost", port, "zxc");
      Class<? extends Exception> aClass = null;
      try {
        CompletableFuture<Response> cf = client.request("auth_test2");
        cf.get(1, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        aClass = e.getClass();
      }
      context.assertEquals(TimeoutException.class, aClass);
      async.complete();
    });
    async.await();
    vertx.close(context.asyncAssertSuccess());
  }

}
