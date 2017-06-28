package org.folio;

/**
 * HTTP status codes of FOLIO's APIs.
 */
public enum HttpStatus {
  /**
   * 200, the request has succeeded.  The information returned with the
   * response is dependent on the method used in the request, as follows:
   *
   * <ul><li>GET - an entity corresponding to the requested resource is sent
   *         in the response;</li>
   *     <li>HEAD - the response must only contain the header information and
             no Entity-Body;</li>
   *     <li>POST - an entity describing or containing the result of the action</li>
   * </ul>
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.2">RFC 1945 Section 9.2</a>.
   */
  HTTP_ACCEPTED(200),

  /**
   * 201, the request has succeeded. The information returned with the
   * response is dependent on the method used in the request, as follows:
   *
   * <ul><li>GET - an entity corresponding to the requested resource is sent
   *         in the response;</li>
   *
   *     <li>HEAD - the response must only contain the header information and
   *         no Entity-Body;</li>
   *
   *     <li>POST - an entity describing or containing the result of the action.</li>
   * </ul>
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.2">RFC 1945 Section 9.2</a>.
   */
  HTTP_CREATED(201),

  /**
   * 204, the request was successful but there is no new information to send back.
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.2">RFC 1945 Section 9.2</a>.
   * To be returned on successful POST, PUT and DELETE requests.
   */
  HTTP_NO_CONTENT(204),

  /**
   * 400, the request has malformed syntax and was rejected. Do not repeat without modifications.
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.4">RFC 1945 Section 9.4</a>.
   */
  HTTP_BAD_REQUEST(400),

  /**
   * 401, authentication is missing or invalid.
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.4">RFC 1945 Section 9.4</a>.
   */
  HTTP_UNAUTHORIZED(401),

  /**
   * 403, the access is denied because of insufficient privileges.
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.4">RFC 1945 Section 9.4</a>.
   */
  HTTP_FORBIDDEN(403),

  /**
   * 404, nothing has been found matching the request URI.
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.4">RFC 1945 Section 9.4</a>.
   */
  HTTP_NOT_FOUND(404),

  /**
   * 422, the validation of the request failed.  The body, the URI parameters or the HTTP
   * headers do not comply with the requirements published with the FOLIO API.
   */
  HTTP_VALIDATION_ERROR(422),

  /**
   * 500, internal server error. The server encountered an unexpected condition which prevented it
   * from fulfilling the request.
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.5">RFC 1945 Section 9.5</a>.
   */
  HTTP_INTERNAL_SERVER_ERROR(500),

  /**
   * 501, the functionality required to fulfill the request is not supported. This is the
   * appropriate response when the server does not recognize the request method and is not
   * capable of supporting it for any resource.
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.5">RFC 1945 Section 9.5</a>.
   */
  HTTP_NOT_IMPLEMENTED(501);

  private final int value;

  HttpStatus(int value) {
    this.value = value;
  }

  /**
   * @return status code as int value
   */
  public int toInt() {
    return value;
  }

  /**
   * Convert int to HttpStatus.
   *
   * @param value int value of the HttpStatus to return
   * @return HttpStatus
   * @throws IllegalArgumentException if there isn't an HttpStatus for value
   */
  public static HttpStatus get(int value) {
    for (HttpStatus status : HttpStatus.values()) {
      if (status.value == value) {
        return status;
      }
    }
    throw new IllegalArgumentException("FOLIO does not specify a name for HTTP status value " + value);
  }
}
