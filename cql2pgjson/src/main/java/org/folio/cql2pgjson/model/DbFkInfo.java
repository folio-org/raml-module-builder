package org.folio.cql2pgjson.model;

public class DbFkInfo {

  private String table;
  private String field;
  private String targetTable;

  public DbFkInfo(String table, String field, String targetTable) {
    super();
    this.table = table;
    this.field = field;
    this.targetTable = targetTable;
  }

  @Override
  public String toString() {
    return "DbFkInfo [table=" + table + ", field=" + field + ", targetTable=" + targetTable + "]";
  }

  public String getTable() {
    return table;
  }

  public String getField() {
    return field;
  }

  public String getTargetTable() {
    return targetTable;
  }

}
