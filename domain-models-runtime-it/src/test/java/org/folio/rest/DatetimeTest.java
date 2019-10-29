package org.folio.rest;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;

import org.junit.jupiter.api.Test;

public class DatetimeTest extends ApiTestBase {
  @Test
  void get() {
    System.out.println(new java.util.Date());
    given(r).
      queryParam("startDate", "2016-11-25T22:00:00.0+00:00").
      queryParam("endDate", "2018-11-26T16:17:18Z").
      queryParam("requestedDate", "2019-11-20").
    when().get("/datetime").
    then().
      statusCode(200).
      body(containsString("2016"),
           containsString("2018"),
           containsString("2019"));
  }
}
