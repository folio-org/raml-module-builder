package org.folio.rest.persist.facets;

/**
 * @author shale
 *
 */
public class ParsedQuery {

  private String originalQuery;
  private String queryWithoutOrderBy;
  private String whereClause;
  private String orderByClause;
  private String limitClause;
  private String offsetClause;

  public String getOriginalQuery() {
    return originalQuery;
  }
  public void setOriginalQuery(String originalQuery) {
    this.originalQuery = originalQuery;
  }
  public String getQueryWithoutOrderBy() {
    return queryWithoutOrderBy;
  }
  public void setQueryWithoutOrderBy(String queryWithoutOrderBy) {
    this.queryWithoutOrderBy = queryWithoutOrderBy;
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
