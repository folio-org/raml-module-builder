package org.folio.rest.persist.ddlgen;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shale
 *
 */
public class Schema {

  private List<Table> tables = new ArrayList<>();
  private List<View> views = new ArrayList<>();
  private List<Script> scripts = new ArrayList<>();

  public List<Table> getTables() {
    return tables;
  }
  public void setTables(List<Table> tables) {
    this.tables = tables;
  }
  public List<View> getViews() {
    return views;
  }
  public void setViews(List<View> views) {
    this.views = views;
  }
  public List<Script> getScripts() {
    return scripts;
  }
  public void setScripts(List<Script> scripts) {
    this.scripts = scripts;
  }
}
