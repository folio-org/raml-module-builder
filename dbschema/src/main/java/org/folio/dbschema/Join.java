package org.folio.dbschema;

import java.util.List;

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

  void setup(List<Table> tables) {
    getJoinTable().setup(tables);
    getTable().setup(tables);
  }
}
