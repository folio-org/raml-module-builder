package org.folio.rest;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import io.restassured.RestAssured;
import io.restassured.filter.log.ErrorLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.jupiter.api.BeforeAll;

public class ApiTestBase {
  static Vertx vertx;
  /** default request header with "x-okapi-tenant: testlib" and "Content-type: application/json"
   *  and ErrorLoggingFilter (logs to System.out).
   */
  static RequestSpecification r;
  private static final CompletableFuture<String> deploymentFuture = new CompletableFuture<String>();

  @BeforeAll
  static void beforeAll() {
    RestAssured.port = 9230;
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

    vertx = VertxUtils.getVertxWithExceptionHandler();

    // once for all test classes: starting and tenant initialization
    if (deploymentFuture.isDone()) {
      return;
    }

    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    DeploymentOptions deploymentOptions = new DeploymentOptions()
        .setConfig(new JsonObject().put("http.port", RestAssured.port));

    vertx.deployVerticle(RestVerticle.class, deploymentOptions, deploy -> {
      if (deploy.failed()) {
        deploymentFuture.completeExceptionally(deploy.cause());
        return;
      }
      deploymentFuture.complete(deploy.result());
    });
    try {
      deploymentFuture.get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }

    r = given().
        filter(new ErrorLoggingFilter()).
        header("x-okapi-tenant", "testlib").
        contentType(ContentType.JSON);

    // delete tenant (schema, tables, ...) if it exists from previous tests
    TenantAttributes ta = new TenantAttributes().withPurge(true);
    given(r).header("x-okapi-url-to", "http://localhost:" + port).
        contentType(ContentType.JSON)
        .body(Json.encode(ta))
        .when().post("/_/tenant")
        .then().statusCode(204);

    List<Parameter> list = new LinkedList<>();
    list.add(new Parameter().withKey("loadReference").withValue("true"));
    ta = new TenantAttributes().withModuleTo("mod-api-1.0.0").withParameters(list);

    // create tenant (schema, tables, ...)
    String location = given(r)
        .header("x-okapi-url-to", "http://localhost:" + port)
        .contentType(ContentType.JSON)
        .body(Json.encode(ta))
        .when().post("/_/tenant")
        .then().statusCode(201)
        .extract().header("Location");

    given(r)
        .header("x-okapi-url-to", "http://localhost:" + port)
        .when().get(location + "?wait=5000")
        .then()
        .statusCode(200)
        .body("error", is(nullValue()));
  }

  /**
   * @param path API path, for example <code>/bees</code>
   * @param arrayName property name of the result array, for example <code>bees</code>
   */
  static void deleteAll(String path, String arrayName) {
    List<Map<String,String>> array =
    given(r).
    when().get(path + "?limit=100").
    then().
      statusCode(200).
      body("total_records", lessThan(100)).
    extract().path(arrayName);

    for (Map<String,String> item : array) {
      given(r).
      when().delete(path + "/" + item.get("id")).
      then().statusCode(204);
    }

    given(r).
    when().get(path).
    then().
      statusCode(200).
      body("total_records", equalTo(0));
  }

  static String randomUuid() {
    return UUID.randomUUID().toString();
  }
}
