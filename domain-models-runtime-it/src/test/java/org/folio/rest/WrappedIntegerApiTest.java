package org.folio.rest;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;

import org.junit.jupiter.api.Test;

class WrappedIntegerApiTest extends ApiTestBase {

  @Test
  void getWithValue() {
    given(r).
      queryParam("value", 10).
    when().get("/wrapped-integer").
    then().
      statusCode(200).
      body(equalTo("10"));
  }

  @Test
  void getWithValueZero() {
    given(r).
      queryParam("value", 0).
    when().get("/wrapped-integer").
    then().
      statusCode(200).
      body(equalTo("0"));
  }

  @Test
  void getWithoutValue() {
    given(r).
    when().get("/wrapped-integer").
    then().
      statusCode(200).
      body(equalTo("null"));
  }

}
