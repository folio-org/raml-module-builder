package org.folio.rest.persist.ddlgen;

import java.util.List;

/**
 * @author shale
 *
 */
public class Index extends TableIndexes {

  private boolean caseSensitive = false;
  private String whereClause = null;
  private boolean stringType = true;
  private boolean removeAccents = true;
  private List<String> arrayModifiers = null;
  private String arraySubfield = null;
  private String multiFieldNames;
  private String sqlExpression;

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
  public boolean isStringType() {
    return stringType;
  }
  public void setStringType(boolean stringType) {
    this.stringType = stringType;
  }
  public boolean isRemoveAccents() {
    return removeAccents;
  }
  public void setRemoveAccents(boolean removeAccents) {
    this.removeAccents = removeAccents;
  }

  public List<String> getArrayModifiers() {
    return arrayModifiers;
  }

  public void setArrayModifiers(List<String> modifiers) {
    this.arrayModifiers = modifiers;
  }

  public String getArraySubfield() {
    return arraySubfield;
  }

  public void setArraySubfield(String modifiersSubfield) {
    this.arraySubfield = modifiersSubfield;
  }
  public String getMultiFieldNames() {
    return multiFieldNames;
  }
  public void setMultiFieldNames(String queryIndexName) {
    this.multiFieldNames = queryIndexName;
  }
  public String getSqlExpression() {
    return sqlExpression;
  }
  public void setSqlExpression(String sqlExpression) {
    this.sqlExpression = sqlExpression;
  }
  public String getFinalSqlExpression(String tableLoc) {
    if(this.getMultiFieldNames() == null && this.getSqlExpression() == null) {
      return this.fieldPath;
    } else if ( this.getSqlExpression() != null) {
      return this.getSqlExpression();
    } else {
      String [] splitIndex = this.getMultiFieldNames().split(" *, *");

      StringBuilder result = new StringBuilder("concat_space_sql(");
      for(int i = 0;i < splitIndex.length;i++) {
        if(splitIndex[i].contains("[*]")) {



        }
        if(i != 0) {
          result .append(" , ");
        }
        String [] rawExpandedTerm = splitIndex[i].split("\\.");
        StringBuilder expandedTerm = new StringBuilder();

        for(int j = 0; j < rawExpandedTerm.length; j++) {
          String arrowToken = "->";
          if(j == rawExpandedTerm.length - 1) {
            arrowToken = "->>";
          }
          expandedTerm.append(arrowToken).append("'").append(rawExpandedTerm[j]).append("'");
        }
        result.append(tableLoc).append(".jsonb").append(expandedTerm);
      }

      result.append(")");
      return result.toString();
    }

  }
}
