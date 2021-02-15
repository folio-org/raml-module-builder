package org.folio.rest.tools;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.IOException;

public interface ClientGrabber {
  void generateClassMeta(String interfaceName);
  void generateMethodMeta(String methodName, JsonObject params, String url,
                          String httpVerb, JsonArray contentType, JsonArray accepts);
  void generateClass(JsonObject classSpecificMapping) throws IOException;
}
