package org.folio.dbschema;

/**
 * @author shale
 *
 */
public class AuditingSnippet {

  private AuditInject delete;
  private AuditInject insert;
  private AuditInject update;

  public AuditInject getDelete() {
    return delete;
  }
  public void setDelete(AuditInject delete) {
    this.delete = delete;
  }
  public AuditInject getInsert() {
    return insert;
  }
  public void setInsert(AuditInject insert) {
    this.insert = insert;
  }
  public AuditInject getUpdate() {
    return update;
  }
  public void setUpdate(AuditInject update) {
    this.update = update;
  }



}
