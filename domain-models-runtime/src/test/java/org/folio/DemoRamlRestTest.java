package org.folio;

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

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.folio.rest.RestVerticle;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * This is our JUnit test for our verticle. The test uses vertx-unit, so we declare a custom runner.
 */
@RunWith(VertxUnitRunner.class)
public class DemoRamlRestTest {

  private Vertx             vertx;
  int port;
  
  /**
   * @param context  the test context.
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
   * @param context  the test context
   */
  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }
  
  /**
   * just send a get request for books api with and without the required author query param
   * 1. one call should succeed and the other should fail (due to 
   * validation aspect that should block the call and return 400)
   * 2. test the built in upload functionality
   * @param context - the test context
   */
  @Test
  public void test(TestContext context) throws Exception {
    checkURLs(context, "http://localhost:" + port + "/apis/books?author=me", 200);
    checkURLs(context, "http://localhost:" + port + "/apis/books", 400);
    postData(context, "http://localhost:" + port + "/apis/admin/upload", getBody("uploadtest.json", true), 400);
    postData(context, "http://localhost:" + port + "/apis/admin/upload?file_name=test.json", getBody("uploadtest.json", true), 204);
    postData(context, "http://localhost:" + port + "/apis/admin/upload?file_name=test2.json", Buffer.buffer(getFile("uploadtest.json")), 204);

  }
  
  public void checkURLs(TestContext context, String url, int codeExpected) {
    try {
      Async async = context.async();
      HttpMethod method = HttpMethod.GET;
      HttpClient client = vertx.createHttpClient();
      HttpClientRequest request = client.requestAbs(method,
              url, new Handler<HttpClientResponse>() {
        @Override
        public void handle(HttpClientResponse httpClientResponse) {

          if (httpClientResponse.statusCode() == codeExpected) {
            context.assertTrue(true);
          }
          async.complete();
        }
      });
      request.exceptionHandler(error -> {
        context.fail(error.getMessage());
        async.complete();
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
  
  /**
   * for POST
   */
  private void postData(TestContext context, String url, Buffer buffer, int errorCode) {
    Async async = context.async();
    HttpClient client = vertx.createHttpClient();
    HttpClientRequest request;
    request = client.postAbs(url);
    request.exceptionHandler(error -> {
      async.complete();
      context.fail(error.getMessage());
    }).handler(response -> {
      int statusCode = response.statusCode();
      // is it 2XX
      System.out.println("Status - " + statusCode + " at " + System.currentTimeMillis() + " for " + url);

      if (statusCode == errorCode) {
        context.assertTrue(true);
      } else {        
        response.bodyHandler(responseData -> {
          context.fail("got non 200 response from bosun, error: " + responseData + " code " + statusCode);
        });
      }
      if(!async.isCompleted()){
        async.complete();
      }
    });
    request.setChunked(true);
    request.putHeader("Authorization", "abcdefg");
    request.putHeader("Accept", "application/json,text/plain");
    request.putHeader("Content-type",
      "multipart/form-data; boundary=MyBoundary");
    request.write(buffer);
    request.end();
  }
  
  private String getFile(String filename) throws IOException {
    return IOUtils.toString(getClass().getClassLoader().getResourceAsStream(filename), "UTF-8");
  }


  private Buffer getBody(String filename, boolean closeBody) {
    Buffer buffer = Buffer.buffer();
    buffer.appendString("--MyBoundary\r\n");
    buffer.appendString("Content-Disposition: form-data; name=\"uploadtest\"; filename=\"uploadtest.json\"\r\n");
    buffer.appendString("Content-Type: application/octet-stream\r\n");
    buffer.appendString("Content-Transfer-Encoding: binary\r\n");
    buffer.appendString("\r\n");
    try {
      buffer.appendString(getFile(filename));
      buffer.appendString("\r\n");
    } catch (IOException e) {
      e.printStackTrace();

    }
    if(closeBody){
      buffer.appendString("--MyBoundary--\r\n");      
    }
    return buffer;
  }

}
