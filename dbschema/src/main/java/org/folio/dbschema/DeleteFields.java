package org.folio.dbschema;

/**
 * @author shale
 *
 */
public class DeleteFields extends Field {

  public DeleteFields() {
    super();
    super.tOps = TableOperation.DELETE;
  }

}
