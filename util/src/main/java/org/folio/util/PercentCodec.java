package org.folio.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Percent encoding of UTF-8 strings, conforming to RFC 3986.
 *
 * <p>These are the characters that are never encoded: The letters a-z and A-Z and the digits 0-9, and the
 * {@code -._~} characters. They are the
 * <a href="https://datatracker.ietf.org/doc/html/rfc3986#section-2.3">Unreserved Characters (RFC 3986 Section 2.3)</a>.
 *
 * <p>All other characters are always percent encoded.
 *
 * <p>Space is always encoded as {@code %20} to conform with RFC 3986.
 * <a href="https://datatracker.ietf.org/doc/html/rfc1630#page-7">RFC 1630</a> allows to encode space as {@code +}
 * but RFC 1630's category is "Informational" only, whereas RFC 3986's category is "Standards Track".
 *
 * <p>The only encoding difference between {@link java.net.URLEncoder} and this class is the space character:
 * URLEncoder encodes it as {@code +} and this class encodes it as {@code %20}. In addition this class
 * supports Appendable and CharSequence for ease of use and performance, and it doesn't require a charset
 * parameter because it always uses UTF8 regardless of the locale.
 */
public final class PercentCodec {
  private static final char [] UNRESERVED = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~".toCharArray();
  private static final char [] HEX = "0123456789ABCDEF".toCharArray();
  private static final char [][] ENCODE = getEncodeArray();

  private PercentCodec() {
    throw new UnsupportedOperationException();
  }

  private static char [][] getEncodeArray() {
    char [][] encode = new char [256][];
    for (char c : UNRESERVED) {
      char [] cc = { c };
      encode[c] = cc;
    }
    for (int i = 0; i < 256; i++) {
      if (encode[i] != null) {
        continue;
      }
      char [] cc = { '%', HEX[i >> 4], HEX[i & 0xf] };
      encode[i] = cc;
    }
    return encode;
  }

  /**
   * Apply percent encoding (RFC 3986) to charSequence and append the result to appendable.
   *
   * <p>All characters except these get percent encoded: The letters a-z and A-Z and the digits 0-9, and the
   * {@code -._~} characters.
   *
   * <p>Example usage:
   *
   * <pre>
   * String baseUrl = "https://example.com/users";
   * String query =  "status == open";
   * StringBuilder url = new StringBuilder(baseUrl);
   * url.append("?query=");
   * PercentCodec.encode(url, query);
   * // yields "https://example.com/users?query=status%20%3D%3D%20open"
   * </pre>
   *
   * @param charSequence in UTF8 encoding; if null or empty the appendable is unchanged
   * @return appendable
   */
  public static Appendable encode(Appendable appendable, CharSequence charSequence) {
    if (charSequence == null || charSequence.length() == 0) {
      return appendable;
    }
    ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(CharBuffer.wrap(charSequence));
    while (byteBuffer.hasRemaining()) {
      int i = byteBuffer.get() & 0xff;
      char [] encode = ENCODE[i];
      for (char c : encode) {
        try {
          appendable.append(c);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
    return appendable;
  }

  /**
   * Return charSequence with percent encoding (RFC 3986) applied.
   *
   * <p>All characters except these get percent encoded: The letters a-z and A-Z and the digits 0-9, and the
   * {@code -._~} characters.
   *
   * <p>Example usage:
   *
   * <pre>
   * String baseUrl = "https://example.com/users";
   * String query =  "status == open";
   * String url = baseUrl + "?query=" + PercentCodec.encode(query);
   * // yields "https://example.com/users?query=status%20%3D%3D%20open"
   * </pre>
   *
   * @param charSequence in UTF8 encoding; if null or empty then "" is returned
   */
  public static CharSequence encode(CharSequence charSequence) {
    if (charSequence == null || charSequence.length() == 0) {
      return "";
    }
    ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(CharBuffer.wrap(charSequence));
    CharBuffer out = CharBuffer.allocate(3 * byteBuffer.limit());

    while (byteBuffer.hasRemaining()) {
      int c = byteBuffer.get() & 0xff;
      out.put(ENCODE[c]);
    }

    out.limit(out.position());
    out.position(0);
    return out;
  }

  /**
   * Return charSequence with percent encoding (RFC 3986) applied.
   *
   * <p>All characters except these get percent encoded: The letters a-z and A-Z and the digits 0-9, and the
   * {@code -._~} characters.
   *
   * @param charSequence in UTF8 encoding; if null or empty then "" is returned
   */
  public static String encodeAsString(CharSequence charSequence) {
    return encode(charSequence).toString();
  }
}
