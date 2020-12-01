package org.folio.rest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.hasSize;

import java.util.Random;

import io.restassured.response.Response;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import io.restassured.RestAssured;

public class TenantApiTest extends ApiTestBase {
  private Random random = new Random();

  /**
   * @return a new tenant id to ensure that it doesn't exist even if the developer's database hasn't been wiped.
   */
  private String randomTenant() {
    return "tenant" + Math.abs(random.nextLong());
  }

  @Test
  void post() {
    Response response = given(r).
        header("x-okapi-tenant", randomTenant()).
        header("x-okapi-url-to", "http://localhost:" + RestAssured.port).
        body("{\"module_to\":\"mod-api-1.0.0\"}").
        when().post("/_/tenant").
        then().
        statusCode(201).extract().response();
    String location = response.getHeader("Location");
    Assert.assertTrue(location, location.startsWith("/_/tenant/"));

    given(r).
        get(location + "?wait=5000").
        then().
        statusCode(200).
        body("messages", hasSize(0));  // JSON list of commands that have failed
  }

  // https://issues.folio.org/browse/RMB-508 https://issues.folio.org/browse/RMB-511 https://issues.folio.org/browse/MODEVENTC-14
  // mod-event-config /_/tenant with "X-Okapi-Request-Id: 1" fails with 500 Internal Server Error
  @Test
  void posttWithRequestId() {
    Response response = given(r).
        header("x-okapi-tenant", randomTenant()).
        header("x-okapi-url-to", "http://localhost:" + RestAssured.port).
        header("X-Okapi-Request-Id", "1").
        body("{\"module_to\":\"mod-api-1.0.0\"}").
        when().post("/_/tenant").
        then().
        statusCode(201).extract().response();
    String location = response.getHeader("Location");
    Assert.assertTrue(location, location.startsWith("/_/tenant/"));

    given(r).
        get(location + "?wait=5000").
        then().
        statusCode(200).
        body("messages", hasSize(0));  // JSON list of commands that have failed
  }
}
