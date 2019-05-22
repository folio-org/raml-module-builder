package org.folio.rest.persist.facets;

/**
 * @author shale
 *
 */
public class ParsedQuery {

  private String countQuery;
  private String queryWithoutLimOff;
  private String whereClause;
  private String orderByClause;
  private String limitClause;
  private String offsetClause;

  public String getCountQuery() {
    return countQuery;
  }
  public void setCountQuery(String countQuery) {
    this.countQuery = countQuery;
  }
  public String getQueryWithoutLimOff() {
    return queryWithoutLimOff;
  }
  public void setQueryWithoutLimOff(String queryWithoutLimOff) {
    this.queryWithoutLimOff = queryWithoutLimOff;
  }
  public String getWhereClause() {
    return whereClause;
  }
  public void setWhereClause(String whereClause) {
    this.whereClause = whereClause;
  }
  public String getOrderByClause() {
    return orderByClause;
  }
  public void setOrderByClause(String orderByClause) {
    this.orderByClause = orderByClause;
  }
  public String getLimitClause() {
    return limitClause;
  }
  public void setLimitClause(String limitClause) {
    this.limitClause = limitClause;
  }
  public String getOffsetClause() {
    return offsetClause;
  }
  public void setOffsetClause(String offsetClause) {
    this.offsetClause = offsetClause;
  }

}
