package org.folio.rest.tools.utils;

public class ModuleName {
  /**
   * The module name with minus replaced by underscore, for example {@code mod_foo_bar}.
   */
  public static String getModuleName() {
    return "raml_module_builder";
  }

  /**
   * The module version taken from pom.xml at compile time.
   */
  public static String getModuleVersion() {
    return "9.8.7-SNAPSHOT";
  }
}
