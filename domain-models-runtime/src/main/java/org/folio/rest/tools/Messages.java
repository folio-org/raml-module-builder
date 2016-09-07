package org.folio.rest.tools;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Provide language specific messages, caching the language files in memory.
 */
public class Messages {
  private static final Logger log = LoggerFactory.getLogger(Messages.class);

  public static final String      INFRA_MESSAGES_DIR     = "infra-messages";
  public static final String      MESSAGES_DIR           = "messages";
  public static final String      DEFAULT_LANGUAGE       = "en";

  // language + code = text
  private Map<String, Properties> messageMap       = new HashMap<>();

  private Messages() {
    // throws exception on error
    loadAllMessages();
  }

  // will only be loaded (lazily) when getInstance() is called
  // once called the JVM (lazily) loads serially and returns - so this is thread
  // safe
  private static class SingletonHelper {
    private static final Messages INSTANCE = new Messages();
    private SingletonHelper() {
      // prevent instantiation
    }
  }

  public static Messages getInstance() {
    return SingletonHelper.INSTANCE;
  }

  private void loadAllMessages() {
    loadMessages(INFRA_MESSAGES_DIR);
    if (messageMap.isEmpty()) {
      throw new IllegalStateException("Messages not found: " + INFRA_MESSAGES_DIR);
    }
    //load project specific messages - they may not exist
    loadMessages(MESSAGES_DIR);
  }

  private void loadMessages(String dir) {
    try {
      //load messages from the runtime jar
      URL url = Messages.class.getClassLoader().getResource(dir);
      if (url == null) {
        return;
      }
      URI uri = url.toURI();

      if ("jar".equals(uri.getScheme())) {
        try (FileSystem fileSystem = getFileSystem(uri)) {
          Path messagePath = fileSystem.getPath(dir);
          loadMessages(messagePath);
        }
      } else {
        Path messagePath = Paths.get(uri);
        loadMessages(messagePath);
      }
    } catch (IOException|URISyntaxException e) {
      throw new IllegalArgumentException(dir, e);
    }
  }

  private FileSystem getFileSystem(URI uri) throws IOException {
    try {
      return FileSystems.newFileSystem(uri, Collections.<String, Object> emptyMap());
    } catch (FileSystemAlreadyExistsException e) { // NOSONAR
      return FileSystems.getFileSystem(uri);
    }
  }
  
  // assume api messages are in English for now!!!
  private void loadMessages(Path messagePath) throws IOException {

    Stream<Path> walk = Files.walk(messagePath, 1);
    for (Iterator<Path> it = walk.iterator(); it.hasNext();) {
      Path file = it.next();
      String name = file.getFileName().toString();
      int sep = name.indexOf('_');
      if (sep == -1) {
        continue;
      }
      String lang = name.substring(0, sep);
      String resource = "/" + messagePath.getFileName().toString() + "/" + name;
      log.info("Loading messages from " + resource + " ................................");
      InputStream stream = getClass().getResourceAsStream(resource);
      Properties properties = new Properties();
      properties.load(stream);
      Properties existing = messageMap.get(lang);
      if(existing == null){
        messageMap.put(lang, properties);
      }
      else{
        existing.putAll(properties);
        messageMap.put(lang, existing);
      }
    }
    walk.close();

  }

  /**
   * Return the message from the properties file.
   *
   * @param language - the language of the properties file to search in
   * @param code - message code
   * @return the message, or null if not found
   */
  private String getMessageSingle(String language, String code) {
    Properties properties = messageMap.get(language);
    if (properties == null) {
      return null;
    }
    return properties.getProperty(code);
  }

  /**
   * Return the message from the properties file.
   * @param language - the language of the properties file to search in. If not found, also tries
   *                   the default language.
   * @param code - message code
   * @return the message, or null if not found
   */
  public String getMessage(String language, String code) {
    String message = getMessageSingle(language, code);
    if (message != null) {
      return message;
    }
    return getMessageSingle(DEFAULT_LANGUAGE, code);
  }

  /**
   * Return the message from the properties file.
   * @param language  - the language of the properties file to search in. If not found, also tries
   *                   the default language.
   * @param code - message code
   * @param messageArguments - message arguments to insert, see java.text.MessageFormat.format()
   * @return the message with arguments inserted
   */
  public String getMessage(String language, String code, Object... messageArguments) {
    String pattern = getMessage(language, code);
    if (pattern == null) {
      return "Error message not found: " + language + " " + code;
    }
    return MessageFormat.format(pattern, messageArguments);
  }
}