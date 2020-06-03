package org.folio.rest;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.*;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;

public class MyitemsApiTest extends ApiTestBase {
  /**
   * Delete records from previous test method
   */
  @BeforeEach
  void deleteAll() {
    deleteAll("/myitems", "myitems");
  }

  @Test
  void post() {
    JsonObject foo = new JsonObject().put("id", randomUuid()).put("name", "foo");
    JsonObject bar = new JsonObject().put("id", randomUuid()).put("name", "bar");
    JsonObject baz = new JsonObject().put("id", randomUuid()).put("name", "baz'");
    JsonObject [] jsonObjects = { foo, bar, baz };

    for (JsonObject jsonObject : jsonObjects) {
      given(r).body(jsonObject.encode()).
      when().post("/myitems").
      then().
        statusCode(201);
    }

    given(r).
    when().get("/myitems?query=cql.allRecords=1 sortBy name").
    then().
      statusCode(200).
      body("total_records", equalTo(3)).
      body("myitems[0].id", equalTo(bar.getString("id"))).
      body("myitems[0].name", equalTo("bar")).
      body("myitems[1].id", equalTo(baz.getString("id"))).
      body("myitems[1].name", equalTo("baz'")).
      body("myitems[2].id", equalTo(foo.getString("id"))).
      body("myitems[2].name", equalTo("foo"));
  }

  @Test
  void put() {
    String id = randomUuid();
    given(r).body(new JsonObject().put("id", id).put("name", "Puttgarden").encode()).
    when().post("/myitems").
    then()
      .statusCode(201);

    Map<Object,String> metadata1 =
    given(r).
    when().get("/myitems/" + id).
    then().
      statusCode(200).
      body("id", equalTo(id)).
      body("name", equalTo("Puttgarden")).
    extract().path("metadata");

    given(r).body(new JsonObject().put("name", "Putnam").encode()).
    when().put("/myitems/" + id).
    then().
      statusCode(204);

    given(r).
    when().get("/myitems/" + id).
    then().
      statusCode(200).
      body("id", equalTo(id)).
      body("name", equalTo("Putnam")).
      body("metadata.createdDate", is(metadata1.get("createdDate"))).
      body("metadata.updatedDate", is(greaterThan(metadata1.get("createdDate")))).
      body("metadata.createdByUserId", is(nullValue())).
      body("metadata.updatedByUserId", is(nullValue()));

    given(r).
      header(RestVerticle.OKAPI_USERID_HEADER, "hal").
      body(new JsonObject().put("name", "Pusta").encode()).
    when().put("/myitems/" + id).
    then().
      statusCode(204);

    given(r).
    when().get("/myitems/" + id).
    then().
      statusCode(200).
      body("id", equalTo(id)).
      body("name", equalTo("Pusta")).
      body("metadata.createdDate", is(metadata1.get("createdDate"))).
      body("metadata.updatedDate", is(greaterThan(metadata1.get("createdDate")))).
      body("metadata.createdByUserId", is(nullValue())).
      body("metadata.updatedByUserId", is("hal"));
  }

  @Test
  void patch() {
    String id = randomUuid();
    given(r).body(new JsonObject().put("id", id).put("name", "Puttgarden").encode()).
        when().post("/myitems").
        then()
        .statusCode(201);

    Map<Object,String> metadata1 =
        given(r).
            when().get("/myitems/" + id).
            then().
            statusCode(200).
            body("id", equalTo(id)).
            body("name", equalTo("Puttgarden")).
            extract().path("metadata");

    given(r).body(new JsonObject().put("name", "Putnam").encode()).
        when().patch("/myitems/" + id).
        then().
        statusCode(204);

    given(r).
        when().get("/myitems/" + id).
        then().
        statusCode(200).
        body("id", equalTo(id)).
        body("name", equalTo("Putnam")).
        body("metadata.createdDate", is(metadata1.get("createdDate"))).
        body("metadata.updatedDate", is(greaterThan(metadata1.get("createdDate")))).
        body("metadata.createdByUserId", is(nullValue())).
        body("metadata.updatedByUserId", is(nullValue()));

    given(r).
        header(RestVerticle.OKAPI_USERID_HEADER, "hal").
        body(new JsonObject().put("name", "Pusta").encode()).
        when().patch("/myitems/" + id).
        then().
        statusCode(204);

    given(r).
        when().get("/myitems/" + id).
        then().
        statusCode(200).
        body("id", equalTo(id)).
        body("name", equalTo("Pusta")).
        body("metadata.createdDate", is(metadata1.get("createdDate"))).
        body("metadata.updatedDate", is(greaterThan(metadata1.get("createdDate")))).
        body("metadata.createdByUserId", is(nullValue())).
        body("metadata.updatedByUserId", is("hal"));
  }

  @Test
  void delete() {
    String xyzId =
        given(r).body(new JsonObject().put("name", "Xyz").encode()).
        when().post("/myitems").
        then()
          .statusCode(201).
        extract().header("Location").replaceFirst(".*/", "");
    String abcId =
        given(r).body(new JsonObject().put("name", "Abc").encode()).
        when().post("/myitems").
        then()
          .statusCode(201).
        extract().header("Location").replaceFirst(".*/", "");

    given(r).
    when().delete("/myitems/" + abcId).
    then()
      .statusCode(204);

    given(r).
    when().get("/myitems/" + abcId).
    then().
      statusCode(404);

    given(r).
    when().get("/myitems/" + xyzId).
    then().
      statusCode(200).
      body("id", equalTo(xyzId));
  }
}
