package org.folio.rest.persist.ddlgen;

/**
 * @author shale
 *
 */
public class ViewTable {

  private String tableName;
  private String joinOnField;

  //needed for the join table, since we can not join two tables
  //with two columns named the same, so the join table / or the root table
  //need to alias the jsonb column to another name, so that when the result set comes in
  //it will contain columns - id, jsonb, jsonb_alias
  private String jsonFieldAlias = "jsonb";

  public String getTableName() {
    return tableName;
  }
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  public String getJoinOnField() {
    return joinOnField;
  }
  public void setJoinOnField(String joinOnField) {
    this.joinOnField = joinOnField;
  }
  public String getJsonFieldAlias() {
    return jsonFieldAlias;
  }
  public void setJsonFieldAlias(String jsonFieldAlias) {
    this.jsonFieldAlias = jsonFieldAlias;
  }

}
