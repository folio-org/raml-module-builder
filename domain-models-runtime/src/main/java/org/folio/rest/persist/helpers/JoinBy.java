package org.folio.rest.persist.helpers;

import org.folio.rest.persist.Criteria.Criteria;

/**
 *
 * A JoinBy is one side of a join statement
 *
 * table name, alias to the table, and the column to join on
 * for example:
 * SELECT * FROM test x1 LEFT JOIN test x2 ON x1.id...
 * The table name: test
 * The alias: x1
 * The column to join on: id
 * selectFromThisTable indicates if this is the table that should be selected from
 * FROM 'thistable' */
public class JoinBy {


  public static final String RIGHT_JOIN = "RIGHT JOIN";
  public static final String LEFT_JOIN = "LEFT JOIN";
  public static final String INNER_JOIN = "INNER JOIN";
  public static final String FULL_JOIN = "FULL JOIN";

  private String tableName;
  private Criteria joinColumn;
  private String alias = "";
  private String fields = "";
  //private boolean selectFromThisTable = false;

  public JoinBy(String tableName, String alias, Criteria joinColumn, String []selectedFields) {
    this.tableName = tableName;
    this.joinColumn = joinColumn;
    this.alias = alias;
    this.joinColumn.setAlias(this.alias);
    this.joinColumn.setJoinON(true);
    if(selectedFields != null){
      for (int i = 0; i < selectedFields.length; i++) {
        if(SQLFunctions.contains(selectedFields[i])){
          fields = fields + selectedFields[i];
        }
        else{
          fields = fields + alias+"."+selectedFields[i];
        }
        if(i+1<selectedFields.length){
          fields = fields + ", ";
        }
      }
    }
    else{
      fields = " * ";
    }

  }

  public String getTableName() {
    return tableName;
  }
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  public Criteria getJoinColumn() {
    return joinColumn;
  }
  public void setJoinColumn(Criteria joinColumn) {
    this.joinColumn = joinColumn;
  }
  public String getAlias() {
    return alias;
  }
  public void setAlias(String alias) {
    this.alias = alias;
  }

  public String getSelectFields() {
    return fields;
  }

}
