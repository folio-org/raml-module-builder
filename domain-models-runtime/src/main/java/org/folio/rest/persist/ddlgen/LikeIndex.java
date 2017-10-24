package org.folio.rest.persist.ddlgen;

/**
 * @author shale
 *
 */
public class LikeIndex extends TableIndexes {

  private boolean caseSensitive = true;
  private String whereClause = null;

  public boolean isCaseSensitive() {
    return caseSensitive;
  }
  public void setCaseSensitive(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
  }
  public String getWhereClause() {
    return whereClause;
  }
  public void setWhereClause(String whereClause) {
    this.whereClause = whereClause;
  }

}
