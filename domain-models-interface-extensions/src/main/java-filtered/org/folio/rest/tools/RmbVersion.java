package org.folio.rest.tools;

public class RmbVersion {
  // pom.xml copies this file to target/generated-sources and fills in the value
  private static final String RMB_VERSION = "${project.version}";

  public static String getRmbVersion() {
    return RMB_VERSION;
  }
}
