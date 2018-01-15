package org.folio.rest.persist.ddlgen;

/**
 * @author shale
 *
 */
public class Join {

  private ViewTable table;
  private ViewTable joinTable;

  public ViewTable getTable() {
    return table;
  }
  public void setTable(ViewTable table) {
    this.table = table;
  }
  public ViewTable getJoinTable() {
    return joinTable;
  }
  public void setJoinTable(ViewTable joinTable) {
    this.joinTable = joinTable;
  }
}
