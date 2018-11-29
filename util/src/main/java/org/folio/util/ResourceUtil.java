package org.folio.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * Load a resource file as String from a jar file or from a classes directory.
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
   * @throws UncheckedIOException on i/o error when reading the input file
   */
  public static String asString(final String name) {
    return asString(name, (ClassLoader) null);
  }

  /**
   * Return the UTF-8 encoded resource file.
   *
   * @param name  resource path of the input file, with or without leading slash
   * @param aClass  the class that provides the class loader in case the default class
   *                loader does not work; may be null
   * @return the content of the resource file
   * @throws UncheckedIOException on i/o error when reading the input file
   */
  public static String asString(final String name, final Class<? extends Object> aClass) {
    if (aClass == null) {
      return asString(name, (ClassLoader) null);
    }
    return asString(name, aClass.getClassLoader());
  }

  /**
   * Return the UTF-8 encoded resource file.
   *
   * @param name  resource path of the input file, with or without leading slash
   * @param classLoader  a fall-back class loader that locates the resource file
   * @return the content of the resource file
   * @throws UncheckedIOException on i/o error when reading the input file
   */
  public static String asString(final String name, ClassLoader classLoader) {
    try (InputStream inputStream = inputStream(name, classLoader)) {
      if (inputStream == null) {
        throw new FileNotFoundException("Resource not found");
      }

      return IoUtil.toStringUtf8(inputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(name + ": " + e.getMessage(), e);
    }
  }

  /**
   * Try getResourceAsStream with this path (rawName) and the three class loaders
   * in this order until we get a non-null InputStream:
   *
   * <p>Thread.currentThread().getContextClassLoader()
   * <br>ClassLoaderUtil.class.getClassLoader()
   * <br>classLoader
   *
   * <p>If this was unsuccessful then try the three class loaders again after adding or removing
   * a leading slash from the path, for example "dir/a.txt" and "/dir/a.txt".
   *
   * @return the InputStream, or null
   */
  private static InputStream inputStream(final String rawName, ClassLoader classLoader) {
    // Implementation idea:
    // https://stackoverflow.com/questions/15749192/how-do-i-load-a-file-from-resource-folder#answer-15749281
    // https://github.com/krosenvold/struts2/blob/master/xwork-core/src/main/java/com/opensymphony/xwork2/util/ClassLoaderUtil.java

    String name = rawName;

    InputStream in = inputStream3(name, classLoader);
    if (in != null) {
      return in;
    }

    if (name.startsWith("/")) {
      name = name.substring(1);
    } else {
      name = "/" + name;
    }

    return inputStream3(name, classLoader);
  }

  private static InputStream inputStream3(final String name, ClassLoader classLoader) {
    InputStream in = inputStream1(name, Thread.currentThread().getContextClassLoader());
    if (in != null) {
      return in;
    }

    in = inputStream1(name, ResourceUtil.class.getClassLoader());
    if (in != null) {
      return in;
    }

    return inputStream1(name, classLoader);
  }

  private static InputStream inputStream1(final String name, ClassLoader classLoader) {
    if (classLoader == null) {
      return null;
    }
    return classLoader.getResourceAsStream(name);
  }
}
