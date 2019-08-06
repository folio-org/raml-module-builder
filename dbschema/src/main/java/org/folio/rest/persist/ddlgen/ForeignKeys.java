package org.folio.rest.persist.ddlgen;

/**
 * @author shale
 *
 */
public class ForeignKeys extends Field {

  private String targetTable;
  private String targetTableAlias;
  private String tableAlias;

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

  /**
   * Return targetTableAlias. If it is null, fall back to targetTable.
   *
   * @return targetTableAlias as a {@link String}
   */
  public String getTargetTableAlias() {
    return targetTableAlias == null ? targetTable : targetTableAlias;
  }

  public void setTargetTableAlias(String targetTableAlias) {
    this.targetTableAlias = targetTableAlias;
  }

  public String getTableAlias() {
    return tableAlias;
  }

  public void setTableAlias(String tableAlias) {
    this.tableAlias = tableAlias;
  }

  @Override
  public String toString() {
    return "ForeignKeys [tableAlias=" + tableAlias + ", targetTable=" + targetTable + ", targetTableAlias="
        + targetTableAlias + "]";
  }

}
