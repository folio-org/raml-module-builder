package org.folio.rest.persist.ddlgen;

import java.util.List;

import org.folio.cql2pgjson.util.Cql2PgUtil;

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
        result.append(formatExpandedTerm(tableLoc, splitIndex[i]));
      }
      result.append(")");
      return result.toString();
    }

  }

  public static StringBuilder formatExpandedTerm(String table, String term) {
    StringBuilder expandedTerm = new StringBuilder();
    StringBuilder result = new StringBuilder();

      int idx = term.indexOf("[*]");
      //case where the syntax is found
      if(idx > -1) {
        expandedTerm.append(table).append(".").append(Cql2PgUtil.cqlNameAsSqlJson("jsonb",term.substring(0,idx)));

        int arrayTermPresent = term.indexOf( "[*].");
        if(arrayTermPresent > -1) {
          formatArrayWithTerm(term, expandedTerm, result, arrayTermPresent);
        } else {
          result.append("concat_array_object(").append(expandedTerm);
        }
        result.append(")");
      } else {
        result.append(table).append(".").append(Cql2PgUtil.cqlNameAsSqlText("jsonb",term));
      }
    return result;
  }
  private static void formatArrayWithTerm(String term, StringBuilder expandedTerm, StringBuilder result,
      int arrayTermPresent) {
    result.append("concat_array_object_values(").append(expandedTerm);
    String arrayTerm = term.substring( arrayTermPresent + "[*].".length(), term.length());
    result.append(",").append("'").append(arrayTerm).append("'");
  }

}
