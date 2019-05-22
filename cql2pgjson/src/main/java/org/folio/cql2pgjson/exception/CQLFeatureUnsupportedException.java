package org.folio.cql2pgjson.exception;

public class CQLFeatureUnsupportedException extends QueryValidationException {

  private static final long serialVersionUID = -7664905159736223646L;

  /**
  * Feature or features of the CQL query are currently unsupported by CQL2PgJSON
  */
  public CQLFeatureUnsupportedException( String message ) {
    super(message);
  }

}
