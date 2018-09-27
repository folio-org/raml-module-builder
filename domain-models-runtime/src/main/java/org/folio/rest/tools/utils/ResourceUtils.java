package org.folio.rest.tools.utils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Stream;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/**
 * Helper for resource files.
 */
public final class ResourceUtils {
  private ResourceUtils() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  /**
   * Check if the resource is in a jar or on disk. This can be used to check if you
   * are running in an IDE or the code is running from within a jar. Reading the resource
   * varies based on this.
   * @param dir
   * @return
   */
  public static boolean isResourceInJarFile(String dir) {
    URI uri = URI.create(dir);
    if ("jar".equals(uri.getScheme())) {
      return true;
    }
    else{
      return false;
    }
  }

  /**
   * List the files found under /resources
   * @param dir
   * @return
   */
  public static ArrayList<String> ListResources(String dir) {
    try {
      URL url = ResourceUtils.class.getClassLoader().getResource(dir);
      if (url == null) {
        return new ArrayList<>();
      }
      URI uri = URI.create(dir);

      if ("jar".equals(uri.getScheme())) {
        try (FileSystem fileSystem = getFileSystem(uri)) {
          Path path = fileSystem.getPath(dir);
          return listResources(path, true, false);
        }
      }
      else {
        Path path = Paths.get(uri);
        return listResources(path, false, false);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(dir, e);
    }
  }

  /**
   * List the files found under /resources as absolute path
   * @param dir
   * @return
   */
  public static ArrayList<String> ListResourcesAsAbsolutePaths(String dir) {
    try {
      URL url = ResourceUtils.class.getClassLoader().getResource(dir);
      if (url == null) {
        return new ArrayList<>();
      }
      URI uri = url.toURI();

      if ("jar".equals(uri.getScheme())) {
        try (FileSystem fileSystem = getFileSystem(uri)) {
          Path path = fileSystem.getPath(dir);
          return listResources(path, true, true);
        }
      }
      else {
        Path path = Paths.get(uri);
        return listResources(path, false, true);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(dir, e);
    }
  }

  private static FileSystem getFileSystem(URI uri) throws IOException {
    try {
      return FileSystems.newFileSystem(uri, Collections.<String, Object> emptyMap());
    } catch (FileSystemAlreadyExistsException e) {
      return FileSystems.getFileSystem(uri);
    }
  }

  private static ArrayList<String> listResources(Path path, boolean inJar, boolean absolutePath)
      throws Exception {
    ArrayList<String> ret = new ArrayList<>();
    try(Stream<Path> walk = Files.walk(path, 1)){
      int cnt = 0;
      for (Iterator<Path> it = walk.iterator(); it.hasNext();) {
        Path file = it.next();
        if(cnt++ != 0){
          //first entry is the directory itself, so skip it
          String name = file.getFileName().toString();
          String resource = path.getFileName().toString() + "/" + name;
          if(absolutePath){
            String r = resource2AbsolutePath(resource, inJar);
            ret.add(r);
          }
          else{
            ret.add(resource);
          }
        }
      }
    }
    return ret;
  }

  /**
   * Read a resource file as a String with UTF8 encoding.
   * @param resourcePath  the path of the resource file to read
   * @return content of the resource file
   * @throws IllegalArgumentException  if the resource file is not found
   * @throws UncheckedIOException  if an I/O error occurs
   */
  public static String resource2String(String resourcePath) {
    try {
      URL url = Resources.getResource(resourcePath);
      return Resources.toString(url, Charsets.UTF_8);
    } catch (IOException e) {
      // This should not happen:
      // https://docs.oracle.com/javase/8/docs/technotes/guides/lang/resources.html#security
      // This function can be used where only unchecked exceptions are
      // allowed, for example when initializing a final static class variable.
      throw new UncheckedIOException(e);
    }
  }

  public static String resource2AbsolutePath(String resourcePath) throws Exception {
    return resource2AbsolutePath(resourcePath, true);
  }

  public static String resource2AbsolutePath(String resourcePath, boolean inJar) throws Exception {
    if(inJar){
      return ResourceUtils.class.getResource("/"+resourcePath).toExternalForm();
    }
    URL url = Resources.getResource(resourcePath);
    return new File(url.toURI()).getAbsolutePath();
  }

}
