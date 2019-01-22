package org.folio.rest.impl;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.internal.mapper.ObjectMapperType;
import com.jayway.restassured.response.Header;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.User;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class APITests {
  private final int portCodex = 9230;
  private Vertx vertx;
  
  private static Logger logger = LoggerFactory.getLogger("x");
  private final Header tenantHeader = new Header("X-Okapi-Tenant", "testlib");
  
  private void setupMux(TestContext context) {
    JsonObject conf = new JsonObject();
    conf.put("http.port", portCodex);
    DeploymentOptions opt = new DeploymentOptions()
      .setConfig(conf);
    vertx.deployVerticle(RestVerticle.class.getName(), opt, context.asyncAssertSuccess());
  }
  
  
  @Before
  public void setUp(TestContext context) {
    vertx = Vertx.vertx();
    RestAssured.port = portCodex;
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  
    setupMux(context);
  }
  
  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }
  
  @Test
  public void testBadContentType(TestContext context) {
    RestAssured.given()
        .header(tenantHeader)
        .header(new Header("Content-Type", "application/json"))
        .body("<hello/>")
      .when()
        .post("/users/1")
      .then()
        .contentType("")
        .statusCode(400);
  }

  @Test
  public void testEcho(TestContext context) {
    RestAssured.given()
        .header(tenantHeader)
        .header(new Header("Content-Type", "application/xml"))
        .body("<hello/>")
      .when()
        .post("/users/1")
      .then()
        .contentType("application/xml")
        .statusCode(201).content(Matchers.equalTo("<hello/>"));
  }

  @Test
  public void testEchoWithLanguage(TestContext context) {
    User user = RestAssured.given()
        .header(tenantHeader)
      .when()
        .get("/users/1?lang=en")
      .then()
        .contentType("application/json")
        .statusCode(200).extract().as(User.class, ObjectMapperType.JACKSON_1);
    context.assertEquals("John", user.getFirstname());
    context.assertEquals(20, user.getAge());
  }
  @Test
  public void testEchoWithBadLanguage(TestContext context) {
    RestAssured.given()
        .header(tenantHeader)
      .when()
        .get("/users/1?lang=dk")
      .then()
        .contentType("text/plain")
        .statusCode(400).content(Matchers.equalTo("bad language"));
  }

  @Test
  public void testEchoWithBadLanguage2(TestContext context) {
    RestAssured.given()
        .header(tenantHeader)
      .when()
        .get("/users/1?lang=ddd")
      .then()
        .contentType("text/plain")
        .statusCode(400);
  } 

  @Test
  public void testUpload1(TestContext context) {
    Async async = context.async();
    final int sz = 100000;
    final int cnt = 100;
    HttpClient httpClient = vertx.createHttpClient();
    HttpClientRequest req = httpClient.postAbs("http://localhost:" + Integer.toString(portCodex) + "/usersupload", x -> {
      context.assertEquals(201, x.statusCode());
      Buffer b = Buffer.buffer();
      x.handler(b::appendBuffer);
      x.endHandler(y -> {
        int gotTotal = Integer.parseInt(b.toString());
        context.assertEquals(sz * cnt, gotTotal);
        async.complete();
      });
    });
    req.setChunked(true);
    req.putHeader("Content-Type", "application/octet-stream");
    req.putHeader("X-Okapi-Tenant", "testlib");
    req.putHeader("Accept", "text/plain");
    Buffer b = Buffer.buffer(sz);
    for (int i = 0; i < sz; i++) {
      b.appendString("_");
    }
    for (int i = 0; i < cnt; i++) {
      req.write(b);
    }
    req.end();
  }
}
