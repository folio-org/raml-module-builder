package org.folio.cql2pgjson.model;

import org.apache.commons.lang3.StringUtils;

/**
 * Container for the WHERE, ORDER BY, LIMIT and OFFSET clause of a SQL SELECT query.
 */
public class SqlSelect {
  private final String select;
  private final String from;
  private final String where;
  private final String orderBy;

  /**
   * Set the values. A null value is converted to an empty String.
   * @param where  the WHERE clause without "WHERE" keyword
   * @param orderBy  the ORDER BY clause without "ORDER BY" keyword
   */
  public SqlSelect(String where, String orderBy) {
    this.select = "";
    this.from = "";
    this.where = StringUtils.defaultString(where);
    this.orderBy = StringUtils.defaultString(orderBy);
  }

  public SqlSelect(String select, String from, String where, String orderBy) {
    this.select = StringUtils.defaultString(select);
    this.from = StringUtils.defaultString(from);
    this.where = StringUtils.defaultString(where);
    this.orderBy = StringUtils.defaultString(orderBy);
  }

  /**
   * @return the WHERE clause without "WHERE" keyword, or empty String if none.
   */
  public String getWhere() {
    return where;
  }

  /**
   * @return the ORDER BY clause without "ORDER BY" keyword, or empty String if none.
   */
  public String getOrderBy() {
    return orderBy;
  }

  /**
   * Construct the full SQL.
   */
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    if (! select.isEmpty()) {
      b.append("SELECT ").append(select);
    }
    if (! from.isEmpty()) {
      b.append(" FROM ").append(from);
    }
    if (! where.isEmpty()) {
      b.append(" WHERE ").append(where);
    }
    if (! orderBy.isEmpty()) {
      b.append(" ORDER BY ").append(orderBy);
    }
    return b.toString().trim();
  }
}
