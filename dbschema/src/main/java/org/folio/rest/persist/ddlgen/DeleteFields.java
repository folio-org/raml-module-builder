package org.folio.rest.persist.ddlgen;

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
