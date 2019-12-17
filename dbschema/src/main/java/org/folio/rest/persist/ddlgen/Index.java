package org.folio.rest.persist.ddlgen;

import java.util.List;

import org.folio.cql2pgjson.util.Cql2PgUtil;

/**
 * @author shale
 *
 */
public class Index extends TableIndexes {
  private static final String ARRAY_TOKEN = "[*]";
  private static final String ARRAY_TERM_TOKEN = "[*].";
  private static final String JSONB = "jsonb";
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
    if (this.getMultiFieldNames() == null && this.getSqlExpression() == null) {
      return this.fieldPath;
    }
    if (this.getSqlExpression() != null) {
      return this.getSqlExpression();
    }

    String [] splitIndex = this.getMultiFieldNames().split(" *, *");

    StringBuilder result = new StringBuilder("concat_space_sql(");
    for (int i = 0; i < splitIndex.length; i++) {
      if (i != 0) {
        result.append(" , ");
      }
      appendExpandedTerm(tableLoc, splitIndex[i], result);
    }
    result.append(")");
    return Cql2PgUtil.wrapInLowerUnaccent(result.toString(), caseSensitive , removeAccents) ;
  }

  protected static void appendExpandedTerm(String table, String term, StringBuilder result) {
    int idx = term.indexOf(ARRAY_TOKEN);

    //case where [*] is not found
    if (idx == -1) {
      result.append(table).append(".").append(Cql2PgUtil.cqlNameAsSqlText(JSONB, term));
      return;
    }
    //case where [*] is found at the end
    if (idx == term.length() - ARRAY_TOKEN.length()) {
      result.append("concat_array_object(")
            .append(table).append(".").append(Cql2PgUtil.cqlNameAsSqlJson(JSONB, term.substring(0,idx)))
            .append(")");
      return;
    }
    //case with [*].value
    result.append("concat_array_object_values(")
          .append(table).append(".").append(Cql2PgUtil.cqlNameAsSqlJson(JSONB,term.substring(0,idx)))
          .append(",")
          .append("'").append(term.substring(idx + ARRAY_TERM_TOKEN.length(), term.length())).append("'")
          .append(")");
  }
}
