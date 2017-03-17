package org.folio.rest.persist.cql;

import java.util.ArrayList;
import java.util.List;

import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;
import org.z3950.zing.cql.cql2pgjson.QueryValidationException;


public class CQLWrapper {

  CQL2PgJSON field;
  String query;
  private Limit  limit = new Limit();
  private Offset offset = new Offset();
  private List<WrapTheWrapper> addedWrappers = new ArrayList<WrapTheWrapper>();


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
   * @return a space followed by the SQL clauses of WHERE, OFFSET and LIMIT
   * @throws IllegalStateException when the underlying CQL2PgJSON throws a QueryValidationException
   */
  @Override
  public String toString() {
    String q = "";
    StringBuffer sb = new StringBuffer();
    if(query != null && field != null){
      try {
        sb.append(field.cql2pgJson(query));
      } catch (QueryValidationException e) {
        throw new IllegalStateException(e);
      }
    }
    int addons = addedWrappers.size();
    for (int i = 0; i < addons; i++) {
      CQL2PgJSON field = addedWrappers.get(i).wrapper.getField();
      String query = addedWrappers.get(i).wrapper.getQuery();
      if(sb.length() > 0){
        sb.append(" ").append(addedWrappers.get(i).operator).append(" ");
      }
      try {
        sb.append(field.cql2pgJson(query));
      } catch (QueryValidationException e) {
        throw new IllegalStateException(e);
      }
    }
    if(sb.length() > 0){
      q = " WHERE " + sb.toString();
    }
    return  q + " " + offset.toString() + " " + limit.toString();
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
