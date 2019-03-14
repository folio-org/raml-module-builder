package org.folio.rest.persist.cql;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;
import org.z3950.zing.cql.cql2pgjson.QueryValidationException;

public class CQLWrapper {

  private static final Logger log = LoggerFactory.getLogger(CQLWrapper.class);
  CQL2PgJSON field;
  String query;
  private Limit  limit = new Limit();
  private Offset offset = new Offset();
  private List<WrapTheWrapper> addedWrappers = new ArrayList<>();

  public CQLWrapper() {
    super();
  }

  public CQLWrapper(CQL2PgJSON field, String query) {
    super();
    this.field = field;
    this.query = query;
  }

  public CQL2PgJSON getField() {
    return field;
  }
  public CQLWrapper setField(CQL2PgJSON field) {
    this.field = field;
    return this;
  }
  public String getQuery() {
    return query;
  }
  public CQLWrapper setQuery(String query) {
    this.query = query;
    return this;
  }
  public CQLWrapper setLimit(Limit limit) {
    this.limit = limit;
    return this;
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
  private void append(StringBuilder sb, String query, CQL2PgJSON field) {
    if(query == null || field == null) {
      return;
    }
    try {
      sb.append(field.cql2pgJson(query));
    } catch (QueryValidationException e) {
      throw new CQLQueryValidationException(e);
    }
  }

  /**
   * Append text to sb. Do nothing if sb is null or empty.
   * Before appending append a space if sb is not empty.
   * @param sb where to append
   * @param text what to append
   */
  private void spaceAppend(StringBuilder sb, String text) {
    if (text == null || text.isEmpty()) {
      return;
    }
    if (sb.length() > 0) {
      sb.append(' ');
    }
    sb.append(text);
  }

  /**
   * @return a space followed by the SQL clauses of WHERE, OFFSET and LIMIT
   * @throws CQLQueryValidationException when the underlying CQL2PgJSON throws a QueryValidationException
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    append(sb, query, field);
    for (WrapTheWrapper wrap : addedWrappers) {
      if (sb.length() > 0) {
        sb.append(' ').append(wrap.operator).append(' ');
      }
      append(sb, wrap.wrapper.getQuery(), wrap.wrapper.getField());
    }
    if (sb.length() > 0) {
      sb.insert(0, " WHERE ");
    }
    spaceAppend(sb, limit.toString());
    spaceAppend(sb, offset.toString());
    String sql = sb.toString();
    if (log.isInfoEnabled()) {
      log.info("CQL >>> SQL: " + this.query + " >>>" + sql);
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
