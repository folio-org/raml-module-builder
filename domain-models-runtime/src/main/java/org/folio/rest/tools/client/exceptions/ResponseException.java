package org.folio.rest.tools.client.exceptions;

import javax.ws.rs.core.Response;

/**
 * Exception with a {@link javax.ws.rs.core.Response}.
 *
 * <p>The Response can contain an HTTP body with error details and
 * an HTTP status.
 */
public class ResponseException extends RuntimeException {
  private final Response response;

  public ResponseException(Response response) {
    super(response == null ? "null response" : response.getStatus() + " " + response.getStatusInfo());
    this.response = response;
  }

  public Response getResponse() {
    return response;
  }
}
