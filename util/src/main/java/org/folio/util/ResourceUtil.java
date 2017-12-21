package org.folio.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

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
   * asString("dir/file.txt") loads the resource src/main/resources/dir/file.txt
   *
   * @param name  resource path of the input file, without leading slash
   * @param aClass  the class that provides the class loader
   * @return the content of the resource file
   * @throws IOException on i/o error when reading the input file
   */
  public static String asString(final String name) throws IOException {
    return asString(name, ResourceUtil.class.getClassLoader());
  }

  /**
   * Return the UTF-8 encoded resource file using aClass' class loader
   * to locate it, for example when picking the .jar to search within.
   *
   * @param name  resource path of the input file, without leading slash
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
   * @param name  resource path of the input file, without leading slash
   * @param classLoader  the class loader that locates the resource file
   * @return the content of the resource file
   * @throws IOException on i/o error when reading the input file
   */
  public static String asString(final String name, ClassLoader classLoader) throws IOException {
    ClassLoader finalClassLoader = classLoader;
    if (classLoader == null) {
      finalClassLoader = ResourceUtil.class.getClassLoader();
    }
    try (InputStream inputStream = finalClassLoader.getResourceAsStream(name)) {
      if (inputStream == null) {
        throw new FileNotFoundException("Resource not found: " + name);
      }
      return IoUtil.toStringUtf8(inputStream);
    }
  }
}
