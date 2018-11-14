package org.folio.rest.tools.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class JarUtils {

  private static final String CLASSES_PATH = "classes/";
  private static final String CLASSPATH_JAR_FILE_NAME = "classpath.jar";

  private JarUtils() {

  }

  public static URL archiveClasspath(URL url) throws IOException {
    File file;
    try {
      file = new File(url.toURI());
    } catch(URISyntaxException e) {
      file = new File(url.getPath());
    }
    File archiveFile = new File(file.getParent() + File.separator + CLASSPATH_JAR_FILE_NAME);
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    FileOutputStream fos = new FileOutputStream(archiveFile);
    JarOutputStream jos = new JarOutputStream(fos, manifest);
    archiveClasspath(file, file.getName(), jos);
    jos.close();
    fos.close();
    return archiveFile.toURI().toURL();
  }

  private static void archiveClasspath(File file, String entryName, JarOutputStream jos) throws IOException {
    if (file.isHidden()) {
        return;
    }
    if (entryName.startsWith(CLASSES_PATH)) {
      entryName = entryName.substring(8);
    }
    if (file.isDirectory()) {
      if (!entryName.endsWith(File.separator)) {
        entryName = entryName + File.separator;
      }
      JarEntry entry = new JarEntry(entryName);
      entry.setTime(file.lastModified());
      jos.putNextEntry(entry);
      jos.closeEntry();
      for (File f: file.listFiles()) {
        archiveClasspath(f, entryName + f.getName(), jos);
      }
      return;
    }
    try (FileInputStream fis = new FileInputStream(file)) {
      JarEntry entry = new JarEntry(entryName);
      entry.setTime(file.lastModified());
      jos.putNextEntry(entry);
      byte[] bytes = new byte[1024];
      int length;
      while ((length = fis.read(bytes)) >= 0) {
        jos.write(bytes, 0, length);
      }
      fis.close();
      jos.closeEntry();
    }
  }

}
