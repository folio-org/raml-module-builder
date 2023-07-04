package org.folio;

/**
 * HTTP status codes of FOLIO's APIs.
 * <p>
 * See the <a href="https://www.iana.org/assignments/http-status-codes/http-status-codes.xhtml">IANA
 * Hypertext Transfer Protocol (HTTP) Status Code Registry</a>.
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
  HTTP_OK(200),

  /**
   * 201, the request has been fulfilled and resulted in a new resource being
   * created. The newly created resource can be referenced by the URI(s)
   * returned in the entity of the response.
   * <p>
   * See 202 (accepted) for cases when the resource has not been created but
   * will be asynchronously.
   * <p>
   * Only POST can create a resource.
   * <p>
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.2">RFC 1945 Section 9.2</a>.
   */
  HTTP_CREATED(201),

  /**
   * 202, the request has been accepted for processing, but the processing
   * has not been completed.
   * <p>
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.2">RFC 1945 Section 9.2</a>.
   */
  HTTP_ACCEPTED(202),

  /**
   * 204, the request was successful but there is no new information to send back.
   * <p>
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.2">RFC 1945 Section 9.2</a>.
   * <p>
   * To be returned on successful POST, PUT and DELETE requests.
   */
  HTTP_NO_CONTENT(204),

  /**
   * 400, the request has malformed syntax and was rejected. Do not repeat without modifications.
   * <p>
   * See <a href="https://tools.ietf.org/html/rfc7231#section-6.5.1">RFC 7231 Section 6.5.1</a>.
   * <p>
   * FOLIO usually returns 400 with a text/plain response body, examples are the frequently used
   * <a href="https://github.com/folio-org/raml/blob/raml1.0/rtypes/collection.raml">collection</a>
   * and
   * <a href="https://github.com/folio-org/raml/blob/raml1.0/rtypes/item-collection.raml">item-collection</a>
   * resource types.
   * <p>
   * For an application/json response body see {@link #HTTP_UNPROCESSABLE_ENTITY} (422).
   */
  HTTP_BAD_REQUEST(400),

  /**
   * 401, authentication is missing or invalid.
   * <p>
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.4">RFC 1945 Section 9.4</a>.
   */
  HTTP_UNAUTHORIZED(401),

  /**
   * 403, the access is denied because of insufficient privileges.
   * <p>
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.4">RFC 1945 Section 9.4</a>.
   */
  HTTP_FORBIDDEN(403),

  /**
   * 404, nothing has been found matching the request URI.
   * <p>
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.4">RFC 1945 Section 9.4</a>.
   */
  HTTP_NOT_FOUND(404),

  /**
   * 422, the validation of the request failed (unprocessable entity).  The body, the URI parameters or the HTTP
   * headers do not comply with the requirements published with the FOLIO API.
   * <p>
   * See <a href="https://tools.ietf.org/html/rfc4918#section-11.2">RFC 4918 Section 11.2</a>.
   * <p>
   * FOLIO usually returns 422 with an application/json response body as specified by the
   * <a href="https://github.com/folio-org/raml/blob/raml1.0/traits/validation.raml">validation</a>
   * trait with the
   * <a href="https://github.com/folio-org/raml/blob/raml1.0/schemas/errors.schema">errors</a> and
   * <a href="https://github.com/folio-org/raml/blob/raml1.0/schemas/error.schema">error</a> schemas.
   * <p>
   * For a text/plain response body see {@link #HTTP_BAD_REQUEST} (400).
   */
  HTTP_UNPROCESSABLE_ENTITY(422),

  /**
   * 500, internal server error. The server encountered an unexpected condition which prevented it
   * from fulfilling the request.
   * <p>
   * See <a href="https://tools.ietf.org/html/rfc1945#section-9.5">RFC 1945 Section 9.5</a>.
   */
  HTTP_INTERNAL_SERVER_ERROR(500),

  /**
   * 501, the functionality required to fulfill the request is not supported. This is the
   * appropriate response when the server does not recognize the request method and is not
   * capable of supporting it for any resource.
   * <p>
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
