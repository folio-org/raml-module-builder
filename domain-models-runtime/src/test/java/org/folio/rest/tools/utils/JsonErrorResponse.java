package org.folio.rest.tools.utils;

import io.vertx.core.json.JsonObject;

import java.util.List;

public class JsonErrorResponse extends Response {
  public JsonErrorResponse(int statusCode, String body, String contentType) {
    super(statusCode, body, contentType);
  }

  public List<JsonObject> getErrors() {
    return JsonArrayHelper.toList(getJson().getJsonArray("errors"));
  }
}
