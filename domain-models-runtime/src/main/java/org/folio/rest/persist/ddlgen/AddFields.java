package org.folio.rest.persist.ddlgen;

/**
 * @author shale
 *
 */
public class AddFields extends Field {

  private Object defaultValue;

  public AddFields() {
    super();
    super.tOps = TableOperation.ADD;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(Object defaultValue) {
    this.defaultValue = defaultValue;
  }

}
