package org.folio.rest.tools.utils;

import io.vertx.core.json.JsonObject;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * @author shale
 *
 */
public final class JwtUtils {
  private JwtUtils() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  /**
   * @return strEncoded decoded using Base64 and UTF-8.
   */
  public static String getJson(String strEncoded) {
    return new String(Base64.getDecoder().decode(strEncoded), StandardCharsets.UTF_8);
  }

  /**
   * Get the value for key from the payload. It neither validates the
   * token header nor the token signature.
   * @param token  a JWT token
   * @return the value for key as String, or null if the key does not exist or token cannot be decoded
   */
  public static String get(String key, String token) {
    try {
      if (token == null) {
        return null;
      }
      String[] split = token.split("\\.");
      //the split array contains the 3 parts of the token - the payload is the middle part
      String json = getJson(split[1]);
      Object value = new JsonObject(json).getValue(key);
      if (value == null) {
        return null;
      }
      return value.toString();
    } catch (Exception e) {
      // ignore invalid token
      return null;
    }
  }

}
