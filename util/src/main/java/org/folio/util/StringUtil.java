package org.folio.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Utility class for String operations.
 */
public final class StringUtil {
  private StringUtil() {
    throw new UnsupportedOperationException();
  }

  /**
   * Encode source using www-form-urlencoded scheme and charset.
   *
   * @param source  String to encode
   * @param charset  name of the charset to use
   * @return the encoded String, "" if source is null, or null if charset is not supported or null
   */
  public static String urlEncode(String source, String charset) {
    if (source == null) {
      return "";
    }
    try {
      return URLEncoder.encode(source, charset);
    } catch (UnsupportedEncodingException|NullPointerException e) {
      return null;
    }
  }

  /**
   * Encode source using www-form-urlencoded scheme and UTF-8 charset.
   * <p>
   * Note that <a href="https://tools.ietf.org/html/rfc3986#section-2.5">RFC3986 Section 2.5</a>
   * requires UTF-8 encoding and that {@link java.net.URLEncoder#encode(String)} is deprecated
   * because it uses the platform's default encoding. See also
   * <a href="https://en.wikipedia.org/wiki/Percent-encoding">https://en.wikipedia.org/wiki/Percent-encoding</a>.
   *
   * @param source  String to encode
   * @return the encoded String or "" if source is null.
   */
  public static String urlEncode(String source) {
    // Using this standard charset is always supported and therefore will
    // never trigger an UnsupportedEncodingException.
    return urlEncode(source, StandardCharsets.UTF_8.name());
  }
}
