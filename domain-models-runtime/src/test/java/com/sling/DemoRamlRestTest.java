/**
 * DemoRamlRestTest
 * 
 * Aug 18, 2016
 *
 * Apache License Version 2.0
 */

/**
 * @author shale
 *
 */
package com.sling;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.io.ByteStreams;
import com.sling.rest.RestVerticle;
import com.sling.rest.resource.utils.NetworkUtils;

/**
 * This is our JUnit test for our verticle. The test uses vertx-unit, so we declare a custom runner.
 */
@RunWith(VertxUnitRunner.class)
public class DemoRamlRestTest {

  private Vertx             vertx;
  int port;
  
  /**
   * <p/>
   * 
   * @param context
   *          the test context.
   */
  @Before
  public void setUp(TestContext context) throws IOException {
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();
    DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject().put("http.port",
      port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, context.asyncAssertSuccess(id -> {

    }));

  }

  /**
   * This method, called after our test, just cleanup everything by closing the vert.x instance
   *
   * @param context
   *          the test context
   */
  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }
  
  /**
   * just send a get request for books api with and without the required author query param
   * 1. one call should succeed and the other should fail (due to 
   * validation aspect that should block the call and return 400)
   *
   * @param context - the test context
   */
  @Test
  public void test(TestContext context){
    checkURLs(context, "http://localhost:" + port + "/apis/books?author=me", 200);
    checkURLs(context, "http://localhost:" + port + "/apis/books", 400);
  }
  

  
  public void checkURLs(TestContext context, String url, int codeExpected) {
    try {
        Async async = context.async();
        HttpMethod method = HttpMethod.GET;
        HttpClient client = vertx.createHttpClient();
        HttpClientRequest request = client.requestAbs(method, 
          url , new Handler<HttpClientResponse>() {
          @Override
          public void handle(HttpClientResponse httpClientResponse) {

            if (httpClientResponse.statusCode() == codeExpected) {
              context.assertTrue(true);
            }
            async.complete();
          }
        });
        request.headers().add("Authorization", "abcdefg");
        request.headers().add("Accept", "application/json");
        request.setChunked(true);
        request.end();
    } catch (Throwable e) {
      e.printStackTrace();
    } finally {

    }
  }

}
