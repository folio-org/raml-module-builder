package org.folio.cql2pgjson.model;

import org.apache.commons.lang3.StringUtils;

/**
 * Container for the WHERE, ORDER BY, LIMIT and OFFSET clause of a SQL SELECT query.
 */
public class SqlSelect {
  private final String fullQuery;
  private final String where;
  private final String orderBy;

  /**
   * Set the values. A null value is converted to an empty String.
   * @param where  the WHERE clause without "WHERE" keyword
   * @param orderBy  the ORDER BY clause without "ORDER BY" keyword
   */
  public SqlSelect(String where, String orderBy) {
    this(where, orderBy, null);
  }

  /**
   * Set the values. A null value is converted to an empty String.
   * @param where  the WHERE clause without "WHERE" keyword
   * @param orderBy  the ORDER BY clause without "ORDER BY" keyword
   * @param fullQuery  the ORDER BY clause without "ORDER BY" keyword
   */
  public SqlSelect(String where, String orderBy, String fullQuery) {
    this.where = StringUtils.defaultString(where);
    this.orderBy = StringUtils.defaultString(orderBy);
    this.fullQuery = fullQuery;
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
   * @return full SQL including SELECT, WHERE, LIMIT, OFFSET, ORDER BY.
   */
  public String getFullQuery() {
    return fullQuery;
  }

  /**
   * Concatenation of getWhere() and getOrderBy() and including "WHERE" and "ORDER BY" keywords if needed.
   */
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    if (! where.isEmpty()) {
      b.append("WHERE ").append(where);
    }
    if (! orderBy.isEmpty()) {
      if (b.length() > 0) {
        b.append(' ');
      }
      b.append("ORDER BY ").append(orderBy);
    }
    return b.toString();
  }
}
