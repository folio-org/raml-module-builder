package org.folio.util;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public final class ResourceUtil {
  private ResourceUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Return the resource file as an UTF-8 String.
   *
   * @param name resource path of the input file
   * @return UTF-8 String
   * @throws IOException on i/o error when reading the input file
   */
  public static String asString(final String name) throws IOException {
    // Implementation idea:
    // https://stackoverflow.com/questions/309424/read-convert-an-inputstream-to-a-string#35446009
    // "8. Using ByteArrayOutputStream and inputStream.read (JDK)"

    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(name)) {
      if (inputStream == null) {
        throw new FileNotFoundException("Resource not found: " + name);
      }
      ByteArrayOutputStream result = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024];
      int length;
      while ((length = inputStream.read(buffer)) != -1) {
        result.write(buffer, 0, length);
      }
      return result.toString(StandardCharsets.UTF_8.name());
    }
  }
}
