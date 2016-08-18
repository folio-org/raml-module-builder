package com.sling.rest.tools;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

public class Messages {

  public static final String      MESSAGES_DIR     = "messages";
  public static final String      DEFAULT_LANGUAGE = "en";

  // language + code = text
  private Map<String, Properties> messageMap       = new HashMap<>();

  private Messages() {

    try {
      loadMessages();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // will only be loaded (lazily) when getInstance() is called
  // once called the JVM (lazily) loads serially and returns - so this is thread
  // safe
  private static class SingletonHelper {
    private static final Messages INSTANCE = new Messages();
  }

  public static Messages getInstance() {
    return SingletonHelper.INSTANCE;
  }

  // assume api messages are in English for now!!!
  private void loadMessages() throws Exception {
    System.out.println("Loading messages................................");

    URI uri = Messages.class.getClassLoader().getResource(MESSAGES_DIR).toURI();
    Path messagePath;

    FileSystem fileSystem = null;
    if (uri.getScheme().equals("jar")) {
      
      try {
        fileSystem = FileSystems.newFileSystem(uri, Collections.<String, Object> emptyMap());
      } catch (FileSystemAlreadyExistsException e) {
        fileSystem = FileSystems.getFileSystem(uri);
        //e.printStackTrace();
      }
      messagePath = fileSystem.getPath(MESSAGES_DIR);
    } else {
      messagePath = Paths.get(uri);
    }
    Stream<Path> walk = Files.walk(messagePath, 1);
    for (Iterator<Path> it = walk.iterator(); it.hasNext();) {
      Path file = it.next();
      String name = file.getFileName().toString();
      int sep = name.indexOf("_");
      if (sep == -1) {
        continue;
      }
      String lang = name.substring(0, sep);
      InputStream stream = getClass().getResourceAsStream("/" + MESSAGES_DIR + "/" + name);
      Properties properties = new Properties();
      properties.load(stream);
      messageMap.put(lang, properties);
    }
    walk.close();
    if (fileSystem != null) {
      fileSystem.close();
    }
  }

  public String getMessage(String language, String code) {
    try {
      return messageMap.get(language).getProperty(code, "Error");
    } catch (Exception e) {
      return messageMap.get(DEFAULT_LANGUAGE).getProperty(code, "Error");
    }
  }

}
