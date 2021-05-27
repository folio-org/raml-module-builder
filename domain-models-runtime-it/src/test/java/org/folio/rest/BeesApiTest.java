package org.folio.rest;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;

public class BeesApiTest extends ApiTestBase {
  @Test
  void getPostGetPutGetDeleteGetAudit() {
    given(r).
    when().get("/bees/bees/").
    then().
      statusCode(200).
      body("total_records", is(1)); // there's already one in reference-data

    String id = UUID.randomUUID().toString();
    JsonObject foo = new JsonObject().put("id", id).put("name", "Willy");
    given(r).body(foo.encode()).
    when().post("/bees/bees").
    then().
      statusCode(201);

    given(r).
    when().get("/bees/bees/" + id).
    then().
      statusCode(200).
      body("id", is(id)).
      body("name", is("Willy"));

    given(r).body(new JsonObject().put("name", "Maya").encode()).
    when().put("/bees/bees/" + id).
    then().
      statusCode(204);

    given(r).
    when().get("/bees/bees/" + id).
    then().
      statusCode(200).
      body("id", is(id)).
      body("name", is("Maya"));

    given(r).
    when().delete("/bees/bees/" + id).
    then()
      .statusCode(204);

    given(r).
    when().get("/bees/bees/" + id).
    then().
      statusCode(404);

    given(r).
    when().get("/bees/history").
    then().log().body().
      statusCode(200).
      body("beeHistories.size()", is(4),
          "beeHistories.beeHistory.id", hasItems(id, id, id),
          "beeHistories.findAll { it.operation == \"I\" }.beeHistory.name", hasItems("Willy"),
          "beeHistories.findAll { it.operation == \"U\" }.beeHistory.name", hasItems("Maya"),
          "beeHistories.findAll { it.operation == \"D\" }.beeHistory.name", hasItems("Maya"));
  }

  @Test
  void ensureHeadersNotEchoed() {
    given(r).
    header("X-foo", "bar").
    header("User-Agent", "browser").
    when().get("/bees/history").
    then().log().body().
      statusCode(200)
      .header("X-foo", is(nullValue()))
      .header("User-Agent", is(nullValue()));
  }
}
