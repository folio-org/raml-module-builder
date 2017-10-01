package org.folio.util;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Reads an InputStream or File into a String.
 */
public final class IoUtil {
  /** private constructor preventing instantiation. */
  private IoUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Read from the UTF8 encoded file <code>path</code> and return it as a String.
   *
   * @param path  file to read
   * @return file content
   * @throws IOException  on file read error
   */
  public static String toStringUtf8(final String path) throws IOException {
    try (FileInputStream fileInputStream = new FileInputStream(path)) {
      return toStringUtf8(fileInputStream);
    }
  }

  /**
   * Read from the UTF-8 encoded inputStream and return it as a String.<p />
   *
   * <p>This replaces org.apache.commons.io.IOUtils.toString(inputStream, "UTF-8")
   * and avoids including the full commons-io package.<p />
   *
   * <p>There is no toString(InputStream) because IOUtils.toString(InputStream) is deprecated
   * as it uses Charset.defaultCharset() what most likely is unintended.
   *
   * @param inputStream where to read from
   * @return the inputStream content
   * @throws IOException on i/o error when reading from the inputStream
   * @throws NullPointerException if inputStream is null
   * @throws UnsupportedEncodingException if the charsetName is not supported
   */
  public static String toStringUtf8(final InputStream inputStream) throws IOException {
    return toString(inputStream, StandardCharsets.UTF_8.name());
  }

  /**
   * Read from the inputStream in <code>charset</code> encoding and return it as a String.
   *
   * <p>This replaces org.apache.commons.io.IOUtils.toString(inputStream, charset)
   * and avoids including the full commons-io package.
   *
   * @param inputStream where to read from
   * @param charset  the encoding of the inputStream
   * @return the inputStream content
   * @throws IOException on i/o error when reading from the inputStream
   * @throws NullPointerException if inputStream is null
   * @throws UnsupportedEncodingException if the charsetName is not supported
   */
  public static String toString(final InputStream inputStream, final Charset charset)
      throws IOException {
    return toString(inputStream, charset.name());
  }

  /**
   * Read from the inputStream in <code>charsetName</code> encoding and return it as a String.
   *
   * <p>This replaces org.apache.commons.io.IOUtils.toString(inputStream, charsetName)
   * and avoids including the full commons-io package.
   *
   * @param inputStream where to read from
   * @param charsetName  encoding of inputStream
   * @return the inputStream content
   * @throws IOException on i/o error when reading from the inputStream
   * @throws NullPointerException if inputStream is null
   * @throws UnsupportedEncodingException if the charsetName is not supported
   */
  public static String toString(final InputStream inputStream, final String charsetName)
      throws IOException {
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
