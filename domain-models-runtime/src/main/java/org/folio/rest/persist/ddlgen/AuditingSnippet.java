package org.folio.rest.persist.ddlgen;

/**
 * @author shale
 *
 */
public class AuditingSnippet {

  private String delete;
  private String insert;
  private String update;

  public String getDelete() {
    return delete;
  }
  public void setDelete(String delete) {
    this.delete = delete;
  }
  public String getUpdate() {
    return update;
  }
  public void setUpdate(String update) {
    this.update = update;
  }
  public String getInsert() {
    return insert;
  }
  public void setInsert(String insert) {
    this.insert = insert;
  }

}
