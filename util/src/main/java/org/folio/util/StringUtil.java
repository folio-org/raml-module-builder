package org.folio.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
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
   * Appends s to appendable as a quoted CQL string constant.
   * It masks the five special CQL characters {@code \ * ? ^ "}
   * and puts the result into double quotes.
   *
   * <p>Example usage:
   *
   * <pre>
   * StringBuilder query = new StringBuilder("username==");
   * StringUtil.appendCqlEncoded(query, username).append(" AND x=y";
   * String url = "https://example.com/users?query=" + PercentCodec.encode(query);
   * </pre>
   *
   * <p>query is {@code username=="" AND x=y} if username is null
   * <p>query is {@code username=="" AND x=y} if username is an empty string
   * <p>query is {@code username=="foo" AND x=y} if username is {@code foo}
   * <p>query is {@code username=="foo\* bar\*" AND x=y} if username is {@code foo* bar*}
   * <p>query is {@code username=="\\\*\?\^\"" AND x=y} if username is {@code \*?^"}
   *
   * @return appendable
   */
  public static Appendable appendCqlEncoded(Appendable appendable, CharSequence s) {
    try {
      appendable.append('"');
      if (s != null) {
        for (int i = 0; i < s.length(); i++) {
          char c = s.charAt(i);
          switch (c) {
          case '\\':
          case '*':
          case '?':
          case '^':
          case '"':
            appendable.append('\\').append(c);
            break;
          default:
            appendable.append(c);
          }
        }
      }
      appendable.append('"');
      return appendable;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Returns s as a quoted CQL string constant. It masks the five
   * special CQL characters {@code \ * ? ^ "} and puts the result into
   * double quotes.
   *
   * <p>Example usage:
   *
   * <pre>
   * String query = "username==" + StringUtil.cqlEncode(username);
   * String url = "https://example.com/users?query=" + PercentCodec.encode(query);
   * </pre>
   *
   * <p>query is {@code username==""} if s is null
   * <p>query is {@code username==""} if s is an empty string
   * <p>query is {@code username=="foo"} if s is {@code foo}
   * <p>query is {@code username=="foo\* bar\*"} if s is {@code foo* bar*}
   * <p>query is {@code username=="\\\*\?\^\""} if s is {@code \*?^"}
   *
   * @return appendable
   * @see #appendCqlEncoded(Appendable, CharSequence) appendCqlEncoded for appending to a StringBuilder
   */
  public static String cqlEncode(CharSequence s) {
    if (s == null) {
      return "\"\"";
    }
    return appendCqlEncoded(new StringBuilder(s.length() + 2), s).toString();
  }

  /**
   * Encode source using www-form-urlencoded scheme and charset.
   *
   * <p>This is for web forms only.
   *
   * <p>Otherwise use {@link PercentCodec}, for example for HTTP requests.
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
   *
   * <p>This is for web forms only.
   *
   * <p>Otherwise use {@link PercentCodec}, for example for HTTP requests.
   *
   * <p>Note that <a href="https://tools.ietf.org/html/rfc3986#section-2.5">RFC3986 Section 2.5</a>
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

  /**
   * Decode source using www-form-urlencoded scheme and charset.
   *
   * @param source  String to encode
   * @param charset  name of the charset to use
   * @return the decoded String, "" if source is null, or null if charset is not supported or null
   */
  public static String urlDecode(String source, String charset) {
    if (source == null) {
      return "";
    }
    try {
      return URLDecoder.decode(source, charset);
    } catch (UnsupportedEncodingException|NullPointerException e) {
      return null;
    }
  }

  /**
   * Decode source using www-form-urlencoded scheme and UTF-8 charset.
   *
   * <p>Note that <a href="https://tools.ietf.org/html/rfc3986#section-2.5">RFC3986 Section 2.5</a>
   * requires UTF-8 encoding and that {@link java.net.URLDecoder#decode(String)} is deprecated
   * because it uses the platform's default encoding. See also
   * <a href="https://en.wikipedia.org/wiki/Percent-encoding">https://en.wikipedia.org/wiki/Percent-encoding</a>.
   *
   * @param source  String to decode
   * @return the decoded String or "" if source is null.
   */
  public static String urlDecode(String source) {
    return urlDecode(source, StandardCharsets.UTF_8.name());
  }
}
