package org.folio.rest.persist.ddlgen;

import java.util.List;

/**
 * @author shale
 *
 */
public class View extends Versioned {

  private String viewName;
  private String mode;
  //pkColumnName should be the id column (not in jsonb) of the table within the view whose results
  //we will be returning from the select on this view, since the postgresClient exposes mapping the id
  //into the jsonb or returning that id for the pojos it maps to from the returned jsons
  private String pkColumnName = "id";
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
  public String getPkColumnName() {
    return pkColumnName;
  }
  public void setPkColumnName(String pkColumnName) {
    this.pkColumnName = pkColumnName;
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

}
