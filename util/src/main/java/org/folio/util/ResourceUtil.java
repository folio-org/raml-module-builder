package org.folio.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

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
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(name)) {
      if (inputStream == null) {
        throw new FileNotFoundException("Resource not found: " + name);
      }
      return IOUtil.toUTF8String(inputStream);
    }
  }
}
