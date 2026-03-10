package org.folio.rest;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import io.restassured.RestAssured;
import io.restassured.filter.log.ErrorLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.awaitility.Awaitility;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.jupiter.api.BeforeAll;

public class ApiTestBase {
  private static final int TENANT_REQUEST_RETRIES = 10;
  private static final long TENANT_REQUEST_RETRY_DELAY_MS = 500;
  static Vertx vertx;
  /** default request header with "x-okapi-tenant: testlib" and "Content-type: application/json"
   *  and ErrorLoggingFilter (logs to System.out).
   */
  static RequestSpecification r;
  private static boolean isDeployed = false;

  @BeforeAll
  static void beforeAll() {
    RestAssured.port = 9230;
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

    vertx = VertxUtils.getVertxWithExceptionHandler();

    // once for all test classes: starting and tenant initialization
    if (isDeployed) {
      return;
    }

    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    DeploymentOptions deploymentOptions = new DeploymentOptions()
        .setConfig(new JsonObject().put("http.port", RestAssured.port));
    try {
      vertx.deployVerticle(RestVerticle.class, deploymentOptions)
      .toCompletionStage()
      .toCompletableFuture()
      .get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }

    isDeployed = true;

    r = given().
        filter(new ErrorLoggingFilter()).
        header("x-okapi-tenant", "testlib").
        contentType(ContentType.JSON);

    // delete tenant (schema, tables, ...) if it exists from previous tests
    TenantAttributes ta = new TenantAttributes().withPurge(true);
    postTenantWithRetry(ta, 204);

    List<Parameter> list = new LinkedList<>();
    list.add(new Parameter().withKey("loadReference").withValue("true"));
    ta = new TenantAttributes().withModuleTo("mod-api-1.0.0").withParameters(list);

    // create tenant (schema, tables, ...)
    String location = postTenantWithRetry(ta, 201).header("Location");

    given(r)
        .header("x-okapi-url-to", "http://localhost:" + port)
        .when().get(location + "?wait=5000")
        .then()
        .statusCode(200)
        .body("complete", is(true))
        .body("error", is(nullValue()));
  }

  private static Response postTenantWithRetry(TenantAttributes tenantAttributes, int expectedStatus) {
    var responseRef = new AtomicReference<Response>();
    Awaitility.await()
        .atMost(TENANT_REQUEST_RETRIES * TENANT_REQUEST_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
        .pollDelay(0, TimeUnit.MILLISECONDS)
        .pollInterval(TENANT_REQUEST_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
        .until(() -> {
          Response response = given(r)
              .header("x-okapi-url-to", "http://localhost:" + port)
              .contentType(ContentType.JSON)
              .body(Json.encode(tenantAttributes))
              .when()
              .post("/_/tenant");
          responseRef.set(response);
          return response.statusCode() == expectedStatus
              || !isTransientConnectionStartupFailure(response);
        });
    Response response = responseRef.get();
    if (response.statusCode() != expectedStatus) {
      throw new AssertionError(String.format(
          "Expected status code <%d> but was <%d>. Response body: %s",
          expectedStatus, response.statusCode(), response.asString()));
    }
    return response;
  }

  private static boolean isTransientConnectionStartupFailure(Response response) {
    if (response.statusCode() != 400 && response.statusCode() != 500 && response.statusCode() != 503) {
      return false;
    }
    String body = response.asString();
    return body != null && body.contains("Connection refused");
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
