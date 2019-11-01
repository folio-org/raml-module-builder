package org.folio.rest.persist.cql;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;

public class CQLWrapper {

  private static final Logger log = LoggerFactory.getLogger(CQLWrapper.class);
  CQL2PgJSON field;
  Criterion criterion;
  String query;
  String whereClause;
  private Limit  limit = new Limit();
  private Offset offset = new Offset();
  private List<WrapTheWrapper> addedWrappers = new ArrayList<>();

  public CQLWrapper() {
    super();
  }

  public CQLWrapper(Criterion criterion) {
    super();
    this.criterion = criterion;
    this.limit = criterion.getLimit();
    this.offset = criterion.getOffset();
  }

  public CQLWrapper(CQL2PgJSON field, String query) {
    super();
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

  private String getQuery() {
    if (whereClause != null) {
      return whereClause;
    }
    if (criterion != null) {
      return criterion.toString();
    }
    return query; // CQL query
  }

  private String getType() {
    if (whereClause != null) {
      return "WHERE";
    }
    if (criterion != null) {
      return "CRITERION";
    }
    if (field != null) {
      return "CQL";
    }
    return "NONE";
  }

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
    if(wrapper.query != null && wrapper.field != null){
      addedWrappers.add(new WrapTheWrapper(wrapper, operator));
    }
    return this;
  }

  /**
   * Append field.cql2pgJson(query) to sb.
   *
   * @param sb where to append
   * @param query CQL query
   * @param field field the query is based on
   * @throws CQLQueryValidationException when the underlying CQL2PgJSON throws a QueryValidationException
   */
  private void appendWhere(StringBuilder sb, String query, CQL2PgJSON field) {
    if (criterion != null) {
      sb.append(criterion.getWhere());
    }
    if (field != null && query != null) {
      try {
        sb.append(field.toSql(query).getWhere());
      } catch (QueryValidationException e) {
        throw new CQLQueryValidationException(e);
      }
    }
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
    appendWhere(sb, query, field);
    for (WrapTheWrapper wrap : addedWrappers) {
      if (sb.length() > 0) {
        // (q1) operator q2 .. left-associative
        sb.insert(0, '(');
        sb.append(") ").append(wrap.operator).append(' ');
      }
      appendWhere(sb, wrap.wrapper.getQuery(), wrap.wrapper.getField());
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
  String getOrderByOp() {
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
  private String getOrderByClause() {
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
      log.info(getType() + " >>> SQL: " + getQuery() + " >>>" + sql);
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
