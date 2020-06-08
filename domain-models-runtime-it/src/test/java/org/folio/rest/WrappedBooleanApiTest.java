package org.folio.rest;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;

import org.junit.jupiter.api.Test;

public class WrappedBooleanApiTest extends ApiTestBase {

  @Test
  void getWithValueTrue() {
    given(r).
      queryParam("value", true).
    when().get("/wrapped-boolean").
    then().
      statusCode(200).
      body(equalTo("true"));
  }

  @Test
  void getWithValueFalse() {
    given(r).
      queryParam("value", false).
    when().get("/wrapped-boolean").
    then().
      statusCode(200).
      body(equalTo("false"));
  }

  @Test
  void getWithoutValue() {
    given(r).
    when().get("/wrapped-boolean").
    then().
      statusCode(200).
      body(equalTo("null"));
  }

}
