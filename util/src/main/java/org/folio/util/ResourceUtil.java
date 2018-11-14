package org.folio.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Load a resource file as String.
 */
public final class ResourceUtil {
  private ResourceUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Return the UTF-8 encoded resource file.
   * <p>
   * Both asString("/dir/file.txt") and asString("dir/file.txt") load
   * /dir/file.txt from the jar file or from classes/dir/file.txt or
   * from test-classes/dir/file.txt.
   *
   * @param name  resource path of the input file, with or without leading slash
   * @return the content of the resource file
   * @throws IOException on i/o error when reading the input file
   */
  public static String asString(final String name) throws IOException {
    return asString(name, (ClassLoader) null);
  }

  /**
   * Return the UTF-8 encoded resource file using aClass' class loader
   * to locate it, for example when picking the .jar to search within.
   *
   * @param name  resource path of the input file, with or without leading slash
   * @param aClass  the class that provides the class loader
   * @return the content of the resource file
   * @throws IOException on i/o error when reading the input file
   */
  public static String asString(final String name, final Class<? extends Object> aClass) throws IOException {
    return asString(name, aClass.getClassLoader());
  }

  /**
   * Return the UTF-8 encoded resource file using the classLoader
   * to locate it, for example when picking the .jar to search within.
   *
   * @param name  resource path of the input file, with or without leading slash
   * @param classLoader  the class loader that locates the resource file
   * @return the content of the resource file
   * @throws IOException on i/o error when reading the input file
   */
  public static String asString(final String name, ClassLoader classLoader) throws IOException {
    String finalName;
    if (name.startsWith("/")) {
      finalName = name;
    } else {
      finalName = "/" + name;
    }

    URL url;
    if (classLoader == null) {
      url = ResourceUtil.class.getResource(finalName);
    } else {
      url = classLoader.getResource(finalName);
    }

    if (url == null) {
      throw new FileNotFoundException("Resource not found: " + name);
    }

    try (InputStream inputStream = url.openStream()) {
      return IoUtil.toStringUtf8(inputStream);
    }
  }
}
