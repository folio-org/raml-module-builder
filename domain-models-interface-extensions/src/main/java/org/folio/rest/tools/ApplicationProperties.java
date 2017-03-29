package org.folio.rest.tools;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Utility to access the values from the resource "application.properties".
 *
 * Statically loads the resource and throws an exception if it does not exist.
 *
 * All methods never return null, they return "" if no value is found.
 */
public final class ApplicationProperties {
  private static final Logger log = LoggerFactory.getLogger(ApplicationProperties.class);
  private static final Properties properties = new Properties();
  static {
    load("application.properties");
    log.info("module name: " + getNameUnderscore()
      + ", version: " + getVersionShort()
      + ", git: " + ApplicationProperties.getGitCommitIdAbbrev());
  }

  private ApplicationProperties() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Load a different resource that replaces the values of the initial loading.
   * @param path the path of the resource
   * @throws InternalError if the resource does not exist.
   */
  static void load(String path) {
    InputStream input = ApplicationProperties.class.getClassLoader().getResourceAsStream(path);
    if (input == null) {
      throw new InternalError("Resource not found: " + path);
    }
    try {
      properties.clear();
      properties.load(input);
      input.close();
    } catch (IOException e) {
      throw new InternalError(e);
    }
  }

  /**
   * @return the value of application.name or "" if undefined
   */
  public static String getName() {
    return properties.getProperty("application.name", "");
  }

  /**
   * @return the value of application.name with minus replaced by underscore, or "" if undefined
   */
  public static String getNameUnderscore() {
    return getName().replace('-', '_');
  }

  /**
   * @return the version as specified in pom.xml
   */
  public static String getVersion() {
    return properties.getProperty("application.version", "");
  }

  /**
   * @return getVersion() with all characters from the first - on removed; this returns 1.2.3 for 1.2.3-SNAPSHOT.
   */
  public static String getVersionShort() {
    return StringUtils.substringBefore(getVersion(), "-");
  }

  /**
   * @return the full git commit id
   */
  public static String getGitCommitId() {
    return properties.getProperty("git.commit.id", "");
  }

  /**
   * @return the abbreviated git commit id
   */
  public static String getGitCommitIdAbbrev() {
    return properties.getProperty("git.commit.id.abbrev", "");
  }

  /**
   * @return the url of the origin of the git clone
   */
  public static String getGitRemoteOriginUrl() {
    return properties.getProperty("git.remote.origin.url", "");
  }
}
