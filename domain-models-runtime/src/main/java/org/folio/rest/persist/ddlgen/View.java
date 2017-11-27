package org.folio.rest.persist.ddlgen;

/**
 * @author shale
 *
 */
public class View {

  private String viewName;
  private ViewTable table;
  private ViewTable joinTable;
  private String mode;
  private double fromModuleVersion;
  private String pkColumnName = "id";

  public String getViewName() {
    return viewName;
  }
  public void setViewName(String viewName) {
    this.viewName = viewName;
  }
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
  public String getMode() {
    return mode;
  }
  public void setMode(String mode) {
    this.mode = mode;
  }
  public double getFromModuleVersion() {
    return fromModuleVersion;
  }
  public void setFromModuleVersion(double fromModuleVersion) {
    this.fromModuleVersion = fromModuleVersion;
  }
  public String getPkColumnName() {
    return pkColumnName;
  }
  public void setPkColumnName(String pkColumnName) {
    this.pkColumnName = pkColumnName;
  }
}
