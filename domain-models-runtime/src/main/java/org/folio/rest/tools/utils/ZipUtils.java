package org.folio.rest.tools.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipUtils {

  private static final String CLASSES_PATH = "classes/";
  private static final String CLASSPATH_ZIP_FILE_NAME = "classpath.zip";

  public static URL zipClasspath(URL url) throws IOException {
    File file;

    try {
      file = new File(url.toURI());
    } catch(URISyntaxException e) {
      file = new File(url.getPath());
    }
    
    File zipFile = new File(file.getParent() + File.separator + CLASSPATH_ZIP_FILE_NAME);

    FileOutputStream fos = new FileOutputStream(zipFile);
    ZipOutputStream zos = new ZipOutputStream(fos);

    zipClasspath(file, file.getName(), zos);

    zos.close();
    fos.close();

    return zipFile.toURI().toURL();
  }

  private static void zipClasspath(File file, String entryName, ZipOutputStream zos) throws IOException {
    // match jar path structure
    if (entryName.startsWith(CLASSES_PATH)) {
      entryName = entryName.substring(8);
    }
    if (file.isHidden()) {
        return;
    }
    if (file.isDirectory()) {
        if (entryName.endsWith(File.separator)) {
          zos.putNextEntry(new ZipEntry(entryName));
          zos.closeEntry();
        } else {
          zos.putNextEntry(new ZipEntry(entryName + File.separator));
          zos.closeEntry();
        }
        for (File f : file.listFiles()) {
          zipClasspath(f, entryName + File.separator + f.getName(), zos);
        }
        return;
    }
    FileInputStream fis = new FileInputStream(file);
    ZipEntry zipEntry = new ZipEntry(entryName);
    zos.putNextEntry(zipEntry);
    byte[] bytes = new byte[1024];
    int length;
    while ((length = fis.read(bytes)) >= 0) {
      zos.write(bytes, 0, length);
    }
    fis.close();
  }

}
