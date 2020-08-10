package org.folio.dbschema;

import java.util.List;

/**
 * @author shale
 *
 */
public class View extends Versioned {

  private String viewName;
  private String mode;
  private String joinType = "JOIN";
  private List<Join> join;

  public String getViewName() {
    return viewName;
  }
  public void setViewName(String viewName) {
    this.viewName = viewName;
  }
  public String getMode() {
    return mode;
  }
  public void setMode(String mode) {
    this.mode = mode;
  }
  /**
   * Name of the primary key field of the referenced table. This is no longer configurable and is always "id".
   * A basic table has these two fields: id UUID PRIMARY KEY, jsonb JSONB NOT NULL.
   */
  public String getPkColumnName() {
    return Table.PK_COLUMN_NAME;
  }
  public String getJoinType() {
    return joinType;
  }
  public void setJoinType(String joinType) {
    this.joinType = joinType;
  }
  public List<Join> getJoin() {
    return join;
  }
  public void setJoin(List<Join> join) {
    this.join = join;
  }

  public void setup(List<Table> tables) {
    if (getMode() == null) {
      setMode("new");
    }
    List<Join> joins = getJoin();
    if (joins == null) {
      return;
    }
    joins.forEach(j -> j.setup(tables));
  }
}
