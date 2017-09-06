package org.folio.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * InputStream functions.
 */
public final class IOUtil {
  /** private constructor preventing instantiation. */
  private IOUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Read UTF-8 from the inputStream and return it as a String.
   * <p>
   * This replaces org.apache.commons.io.IOUtils.toString(inputStream, "UTF-8")
   * and avoids including the full commons-io package.
   * <p>
   * There is no toString(InputStream) because IOUtils.toString(InputStream) is deprecated
   * as it uses Charset.defaultCharset() what most likely is unintended.
   *
   * @param inputStream where to read from
   * @return the String
   * @throws IOException on i/o error when reading from the inputStream
   * @throws UnsupportedEncodingException if the charsetName is not supported
   * @throws NullPointerException if inputStream is null
   */
  public static String toUTF8String(final InputStream inputStream) throws IOException {
    return toString(inputStream, StandardCharsets.UTF_8.name());
  }

  /**
   * Read from the inputStream using charset encoding and return it as an String.
   * <p>
   * This replaces org.apache.commons.io.IOUtils.toString(inputStream, charset)
   * and avoids including the full commons-io package.
   *
   * @param inputStream where to read from
   * @param charset charset the inputStream is encoded
   * @return the String
   * @throws IOException on i/o error when reading from the inputStream
   * @throws NullPointerException if inputStream is null
   * @throws UnsupportedEncodingException if the charsetName is not supported
   */
  public static String toString(final InputStream inputStream, final Charset charset) throws IOException {
    return toString(inputStream, charset.name());
  }

  /**
   * Read from the inputStream in charsetName encoding and return it as a String.
   * <p>
   * This replaces org.apache.commons.io.IOUtils.toString(inputStream, charsetName)
   * and avoids including the full commons-io package.
   *
   * @param inputStream where to read from
   * @param charsetName name of the encoding of inputStream
   * @return the String
   * @throws IOException on i/o error when reading from the inputStream
   * @throws NullPointerException if inputStream is null
   * @throws UnsupportedEncodingException if the charsetName is not supported
   */
  public static String toString(final InputStream inputStream, final String charsetName) throws IOException {
    // Implementation idea:
    // https://stackoverflow.com/questions/309424/read-convert-an-inputstream-to-a-string#35446009
    // "8. Using ByteArrayOutputStream and inputStream.read (JDK)"

    if (inputStream == null) {
      throw new NullPointerException("inputStream must not be null");
    }
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int length;
    while ((length = inputStream.read(buffer)) != -1) {
      result.write(buffer, 0, length);
    }
    return result.toString(charsetName);
  }
}
