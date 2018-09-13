package org.folio.rest.persist.ddlgen;

/**
 * @author shale
 *
 */
public class FullText {

  static final String DEFAULT_DICTIONARY = "simple";

  private String defaultDictionary;

  public String getDefaultDictionary() {
    return defaultDictionary;
  }

  public void setDefaultDictionary(String defaultDictionary) {
    this.defaultDictionary = defaultDictionary;
  }

}
