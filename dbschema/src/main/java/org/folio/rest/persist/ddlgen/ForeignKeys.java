package org.folio.rest.persist.ddlgen;

/**
 * @author shale
 *
 */
public class ForeignKeys extends Field {

  private String targetTable;

  public ForeignKeys() {
    super();
  }
  public ForeignKeys(String fieldPath, String targetTable) {
    super();
    super.fieldPath = fieldPath;
    this.targetTable = targetTable;
  }

  public ForeignKeys(String fieldPath, String targetTable, TableOperation tOps) {
    super();
    super.fieldPath = fieldPath;
    this.targetTable = targetTable;
    this.tOps = tOps;
  }
  public String getTargetTable() {
    return targetTable;
  }
  public void setTargetTable(String targetTable) {
    this.targetTable = targetTable;
  }

}
