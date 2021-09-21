package org.folio.dbschema;

import java.util.Arrays;
import java.util.List;

import org.folio.dbschema.util.SqlUtil;

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
  private String sqlExpressionQuery;

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

  public String getSqlExpressionQuery() {
    return sqlExpressionQuery;
  }

  public void setSqlExpressionQuery(String sqlExpressionQuery) {
    this.sqlExpressionQuery = sqlExpressionQuery;
  }

  public String getFinalSqlExpression(String tableLoc, boolean lowerIt) {
    if (this.getSqlExpression() != null) {
      return this.getSqlExpression();
    }
    if (this.getMultiFieldNames() == null) {
      return this.fieldPath;
    }

    String [] splitIndex = this.getMultiFieldNames().split(",");
    Arrays.asList(splitIndex).replaceAll(s -> s.strip());

    StringBuilder result = new StringBuilder("concat_space_sql(");
    for (int i = 0; i < splitIndex.length; i++) {
      if (i != 0) {
        result.append(" , ");
      }
      appendExpandedTerm(tableLoc, splitIndex[i], result);
    }
    result.append(")");
    return SqlUtil.Cql2PgUtil.wrapInLowerUnaccent(result.toString(), lowerIt , removeAccents);
  }

  public String getFinalSqlExpression(String tableLoc) {
    return this.getFinalSqlExpression(tableLoc, !caseSensitive);
  }

  /**
   * Like getFinalSqlExpression, but wrap in left(..., 600).
   * <p>
   * Special case: getSqlExpression() is returned unchanged, the sqlExpression developer must take care
   * of the 2712 by limit.
   * Also, stringType=false does not truncate
   * <p>
   * PostgreSQL indexes have a 2712 byte limit, 600 multi-byte characters are within this limit.
   */
  public String getFinalTruncatedSqlExpression(String tableLoc) {
    if (this.getSqlExpression() != null) {
      return this.getSqlExpression();
    }
    if (!this.isStringType()) {
      return this.fieldPath;
    }
    if (this.getMultiFieldNames() != null) {
      return "left(" + getFinalSqlExpression(tableLoc) + ",600)";
    }
    if (this.fieldName.contains(",")) {
      // "left(jsonb->>'a', jsonb->>'b', 600)" fails with "function left(text, text, integer) does not exist"
      // https://issues.folio.org/browse/RMB-539
      throw new UnsupportedOperationException("Index with multi field name not supported: " + this.fieldName);
    }
    return "left(" + this.fieldPath + ",600)";
  }

  protected static void appendExpandedTerm(String table, String term, StringBuilder result) {
    int idx = term.indexOf(ARRAY_TOKEN);

    //case where [*] is not found
    if (idx == -1) {
      result.append(table).append(".").append(SqlUtil.Cql2PgUtil.cqlNameAsSqlText(JSONB, term));
      return;
    }
    //case where [*] is found at the end
    if (idx == term.length() - ARRAY_TOKEN.length()) {
      result.append("concat_array_object(")
            .append(table).append(".").append(SqlUtil.Cql2PgUtil.cqlNameAsSqlJson(JSONB, term.substring(0,idx)))
            .append(")");
      return;
    }
    //case with [*].value
    result.append("concat_array_object_values(")
          .append(table).append(".").append(SqlUtil.Cql2PgUtil.cqlNameAsSqlJson(JSONB,term.substring(0,idx)))
          .append(",")
          .append("'").append(term.substring(idx + ARRAY_TERM_TOKEN.length(), term.length())).append("'")
          .append(")");
  }

  public void setupIndex() {
    if (! isStringType()){
      setCaseSensitive(true);
      setRemoveAccents(false);
    }
    setFieldPath(convertDotPath2PostgresNotation(null, getFieldName(), isStringType(), this, false));
    setFieldName(normalizeFieldName(getFieldName()));
  }

  public void setupLikeIndex() {
    setupIndex();
  }

  public void setupUniqueIndex() {
    setupIndex();
  }

  public void setupGinIndex() {
    setFieldPath(convertDotPath2PostgresNotation(null, getFieldName(), true , this, true));
    setFieldName(normalizeFieldName(getFieldName()));
  }

  public void setupFullTextIndex() {
    if (isCaseSensitive()) {
      throw new IllegalArgumentException("full text index does not support case sensitive: " + getFieldName());
    }
    // this suppresses the lower() in the CREATE INDEX.
    setCaseSensitive(true);
    setFieldPath(convertDotPath2PostgresNotation(null, getFieldName(), true, this, true));
    setFieldName(normalizeFieldName(getFieldName()));
  }
}
