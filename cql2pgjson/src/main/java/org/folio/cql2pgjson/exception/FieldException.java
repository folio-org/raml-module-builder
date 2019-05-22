package org.folio.cql2pgjson.exception;

public class FieldException extends CQL2PgJSONException {

  private static final long serialVersionUID = -6349785544488093395L;

  /**
  * JSON Field (tableName) for search must be provided
  */
  public FieldException( String message ) {
    super(message);
  }

}
