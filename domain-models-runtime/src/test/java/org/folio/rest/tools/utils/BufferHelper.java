package org.folio.rest.tools.utils;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class BufferHelper {

  public static JsonObject jsonObjectFromBuffer(Buffer buffer) {
    return new JsonObject(stringFromBuffer(buffer));
  }

  public static String stringFromBuffer(Buffer buffer) {
    return buffer.getString(0, buffer.length());
  }
}
