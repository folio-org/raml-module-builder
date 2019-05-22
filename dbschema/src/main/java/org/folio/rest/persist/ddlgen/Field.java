package org.folio.rest.persist.ddlgen;

/**
 * @author shale
 *
 */
public class Field {

  //path to field, jsonb->'aaa'->>'bbb'
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected String fieldPath;
  //name of field - used to generate user friendly name, ex. bbb,
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected String fieldName;
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected TableOperation tOps = TableOperation.ADD;

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public TableOperation gettOps() {
    return tOps;
  }

  public void settOps(TableOperation tOps) {
    this.tOps = tOps;
  }

  public String getFieldPath() {
    return fieldPath;
  }

  public void setFieldPath(String fieldPath) {
    this.fieldPath = fieldPath;
  }

}
