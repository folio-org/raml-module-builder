package org.folio.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public final class ResourceUtil {
  private ResourceUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Return the UTF-8 encoded resource file.
   *
   * @param name  resource path of the input file
   * @return the conent of the resource file
   * @throws IOException on i/o error when reading the input file
   */
  public static String asString(final String name) throws IOException {
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(name)) {
      if (inputStream == null) {
        throw new FileNotFoundException("Resource not found: " + name);
      }
      return IoUtil.toStringUtf8(inputStream);
    }
  }
}
