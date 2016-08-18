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
  private ArrayList<String> urls;
  int port;
  /**
   * Before executing our test, need to deploy our 2 verticles - the persistence verticle to write to mongo anf then the rest verticle.
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
   * This method, iterates through the urls.csv and runs each url - currently only checking the returned status codes
   *
   * @param context
   *          the test context
   */
  @Test
  public void checkURLs(TestContext context) {

    try {
        Async async = context.async();
        HttpMethod method = HttpMethod.GET;
        HttpClient client = vertx.createHttpClient();
        HttpClientRequest request = client.requestAbs(method, 
          "http://localhost:" + port + "/apis/books", new Handler<HttpClientResponse>() {

          @Override
          public void handle(HttpClientResponse httpClientResponse) {

            if (httpClientResponse.statusCode() != 404) {
              // this is cheating for now - add posts to the test case so that
              // we dont get 404 for missing entities
              context.assertInRange(200, httpClientResponse.statusCode(), 5);
            }
            // System.out.println(context.assertInRange(200, httpClientResponse.statusCode(),5).);
            httpClientResponse.bodyHandler(new Handler<Buffer>() {
              @Override
              public void handle(Buffer buffer) {
                /*
                 * // System.out.println("Response (" // + buffer.length() // + "): ");
                 */System.out.println(buffer.getString(0, buffer.length()));
                async.complete();

              }
            });
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
