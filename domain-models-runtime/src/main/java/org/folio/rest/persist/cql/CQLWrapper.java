package org.folio.rest.persist.cql;

import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;


public class CQLWrapper {

  CQL2PgJSON field;
  String query;
  private Limit  limit = new Limit();
  private Offset offset = new Offset();

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
  @Override
  public String toString() {
    return field.cql2pgJson(query) + " " + offset.toString() + " " + limit.toString();
  }

}
