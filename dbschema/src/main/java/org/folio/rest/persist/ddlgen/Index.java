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
        if(i != 0) {
          result .append(" , ");
        }
        String [] rawExpandedTerm = splitIndex[i].split("\\.");
        StringBuilder expandedTerm = formatExpandedTerm(tableLoc + ".jsonb",rawExpandedTerm);
        result.append(expandedTerm);
      }
      result.append(")");
      return result.toString();
    }

  }

  private StringBuilder formatExpandedTerm(String table, String[] rawExpandedTerm) {
    StringBuilder expandedTerm = new StringBuilder();
    StringBuilder result = new StringBuilder();
    boolean wasArrayIndex = false;
    expandedTerm.append(table);
    for(int j = 0; j < rawExpandedTerm.length; j++) {

      final String indexSyntax = "[*]";
      int idx = rawExpandedTerm[j].indexOf(indexSyntax);
      int endOffset = idx > -1 ? -2 : -1;
      String arrowToken = "->";
      if(j == rawExpandedTerm.length + endOffset) {
        arrowToken = "->>";
      }

      if(idx > -1) {

        expandedTerm.append(arrowToken).append("'").append(rawExpandedTerm[j].substring(0,idx)).append("',").append("'").append(rawExpandedTerm[j+1]).append("'");
        wasArrayIndex = true;
        break;
      } else {
        expandedTerm.append(arrowToken).append("'").append(rawExpandedTerm[j]).append("'");
      }

    }
    if(wasArrayIndex) {
      result.append("concat_array_object_values(").append(expandedTerm).append(")");
    } else {
      result.append(expandedTerm);
    }
    return result;
  }
}
