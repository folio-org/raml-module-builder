package org.folio.rest.persist.ddlgen;

/**
 * @author shale
 *
 */
public class TableIndexes extends Field {

  public TableIndexes() {
    super();
  }
  public TableIndexes(String fieldPath) {
    super();
    super.fieldPath = fieldPath;
  }
  public TableIndexes(String fieldPath, TableOperation tOps) {
    super();
    super.fieldPath = fieldPath;
    this.tOps = tOps;
  }

}
