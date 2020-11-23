package org.folio.rest.persist.cql;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;

public class CQLWrapper {

  private static final Logger log = LogManager.getLogger(CQLWrapper.class);
  CQL2PgJSON field;
  Criterion criterion;
  String query;
  String whereClause;
  private Limit  limit = new Limit();
  private Offset offset = new Offset();
  private List<WrapTheWrapper> addedWrappers = new ArrayList<>();

  public CQLWrapper() {
    // nothing to do
  }

  public CQLWrapper(Criterion criterion) {
    this.criterion = criterion;
    this.limit = criterion.getLimit();
    this.offset = criterion.getOffset();
  }

  public CQLWrapper(CQL2PgJSON field, String query) {
    this.field = field;
    this.query = query;
  }

  /**
   * CQLWrapper constructor setting query, limit and offset.
   *
   * <p>Intended usage:
   *
   * <pre>
   * import org.folio.util.ResourceUtils;
   *
   * public class ... {
   *   private static CQL2PgJSON cql2PgJson;
   *   static {
   *     try {
   *       cql2PgJson = new CQL2PgJSON("users.jsonb", ResourceUtils.resource2String("ramls/user.json"));
   *     } catch (Exception e) {
   *       throw new RuntimeException(e);
   *     }
       }
   *
   *   private static CQLWrapper cqlWrapper(String cql, int offset, int limit) {
   *     return new CQLWrapper(cql2PgJson, cql, offset, limit);
   *   }
   * </pre>
   *
   * @param field  JSONB field
   * @param query  CQL query
   * @param limit  maximum number of records to return; use a negative number for no limit
   * @param offset  skip this number of records; use a negative number for no offset
   */
  public CQLWrapper(CQL2PgJSON field, String query, int limit, int offset) {
    super();
    this.field = field;
    this.query = query;
    if (limit >= 0) {
      this.limit = new Limit(limit);
    }
    if (offset >= 0) {
      this.offset = new Offset(offset);
    }
  }

  public CQL2PgJSON getField() {
    return field;
  }

  public CQLWrapper setField(CQL2PgJSON field) {
    this.field = field;
    return this;
  }

  /**
   * Returns input query for the wrapped query. Form is depending on
   * input query type {@link CQLWrapper#getType()}
   * @return input query
   */
  public String getQuery() {
    if (whereClause != null) {
      return whereClause;
    }
    if (criterion != null) {
      return criterion.toString();
    }
    return query; // CQL query
  }

  /**
   * @return type of query that is wrapped. {@code "WHERE"} {@link CQLWrapper#setWhereClause(java.lang.String) };
   * {@code "CRITERION"} {@link CQLWrapper#CQLWrapper(org.folio.rest.persist.Criteria.Criterion) };
   * {@code "CQL"} for wrap of CQL
   * {@link CQLWrapper#CQLWrapper(org.folio.cql2pgjson.CQL2PgJSON, java.lang.String) } ;
   * {@code "NONE"} for no query wrapped.
   */
  String getType() {
    if (whereClause != null) {
      return "WHERE";
    }
    if (criterion != null) {
      return "CRITERION";
    }
    if (field != null && query != null) {
      return "CQL";
    }
    return "NONE";
  }

  /**
   * Sets CQL query (should be used with {@link CQLWrapper#setField(org.folio.cql2pgjson.CQL2PgJSON)}
   * Or this constructor can be used to specify both:
   * {@link CQLWrapper#CQLWrapper(org.folio.cql2pgjson.CQL2PgJSON, java.lang.String)
   * @param query CQL query
   * @return wrapper itself
   */
  public CQLWrapper setQuery(String query) {
    this.query = query;
    return this;
  }
  public Limit getLimit() {
    return limit;
  }
  public CQLWrapper setLimit(Limit limit) {
    this.limit = limit;
    return this;
  }
  public Offset getOffset() {
    return offset;
  }
  public CQLWrapper setOffset(Offset offset) {
    this.offset = offset;
    return this;
  }
  public CQLWrapper addWrapper(CQLWrapper wrapper){
    addWrapper(wrapper, "and");
    return this;
  }

  /**
   *
   * @param wrapper
   * @param operator - and / or
   * @return
   */
  public CQLWrapper addWrapper(CQLWrapper wrapper, String operator){
    addedWrappers.add(new WrapTheWrapper(wrapper, operator));
    return this;
  }

  /**
   * Get where clause (without WHERE prefix) for Criterion/CQL cases
   * @return clause or empty string if none
   */
  private String getWhereThis() {
    if (criterion != null) {
      return criterion.getWhere();
    }
    if (field != null && query != null) {
      try {
        return field.toSql(query).getWhere();
      } catch (QueryValidationException e) {
        throw new CQLQueryValidationException(e);
      }
    }
    return "";
  }

  /**
   * Append text to sb. Do nothing if text is null or empty.
   * Before appending append a space if sb is not empty.
   * @param sb where to append
   * @param text what to append
   */
  private void spaceAppend(StringBuilder sb, String text) {
    if (text.isEmpty()) {
      return;
    }
    if (sb.length() > 0) {
      sb.append(' ');
    }
    sb.append(text);
  }

  /**
   * @return where clause excluding WHERE prefix or empty string if for no where
   */
  private String getWhereOp() {
    StringBuilder sb = new StringBuilder();
    sb.append(getWhereThis());
    for (WrapTheWrapper wrap : addedWrappers) {
      String a = wrap.wrapper.getWhereThis();
      if (!a.isEmpty()) {
        if (sb.length() > 0) {
          sb.insert(0, '(');
          sb.append(") ");
          sb.append(wrap.operator);
          sb.append(' ');
        }
        sb.append(a);
      }
    }
    return sb.toString();
  }

  /**
   * @return where clause including WHERE prefix or empty string if for no where
   */
  public String getWhereClause() {
    if (whereClause != null) {
      return whereClause;
    }
    String s = getWhereOp();
    if (s.isEmpty()) {
      return "";
    }
    return "WHERE " + s;
  }


  /**
   * This function sets a raw WHERE clause - should not include limits, offset, or order
   * It should not be used. Construct with CQL2PgJSON or Criterion instead
   * @param whereClause including WHERE prefix
   * @return itself (fluent)
   */
  public CQLWrapper setWhereClause(String whereClause) {
    this.whereClause = whereClause;
    return this;
  }

  /**
   * @return sort by criteria excluding SORT BY prefix or empty string if no sorting
   */
  private String getOrderByOp() {
    if (query == null || field == null) {
      return "";
    }
    try {
      return field.toSql(query).getOrderBy();
    } catch (QueryValidationException e) {
      throw new CQLQueryValidationException(e);
    }
  }

  /**
   * @return sort by criteria including SORT BY prefix or empty string if no sorting
   */
  String getOrderByClause() {
    if (criterion != null) {
      return criterion.getOrderBy();
    }
    String s = getOrderByOp();
    if (s.isEmpty()) {
      return "";
    }
    return "ORDER BY " + s;
  }

  /**
   * @return query including SQL clauses of WHERE, ORDER BY
   */
  public String getWithoutLimOff() {
    StringBuilder sb = new StringBuilder(getWhereClause());
    spaceAppend(sb, getOrderByClause());
    return sb.toString();
  }

  /**
   * @return full query including SQL clauses of WHERE, ORDER BY, OFFSET and LIMIT.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(getWithoutLimOff());
    spaceAppend(sb, limit.toString());
    spaceAppend(sb, offset.toString());
    String sql = sb.toString();
    if (log.isInfoEnabled()) {
      log.info("{} >>> SQL: {} >>>{}", getType(), getQuery(), sql);
    }
    return sql;
  }

  class WrapTheWrapper {

    CQLWrapper wrapper;
    String operator;

    public WrapTheWrapper(CQLWrapper wrapper, String operator) {
      this.wrapper = wrapper;
      this.operator = operator;
    }

  }

}
