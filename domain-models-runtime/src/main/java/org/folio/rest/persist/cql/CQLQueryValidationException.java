package org.folio.rest.persist.cql;

public class CQLQueryValidationException extends IllegalStateException {
  private static final long serialVersionUID = 5440468893364845491L;

  public CQLQueryValidationException(Throwable e) {
    super(e);
  }
}

