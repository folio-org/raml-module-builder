package org.folio;

/**
 * HTTP status codes of FOLIO's APIs as enum value (name HTTP_...),
 * and all Apache httpcomponents HTTP status codes as int constants (name SC_...).
 * <p>
 * See the <a href="https://www.iana.org/assignments/http-status-codes/http-status-codes.xhtml">IANA
 * Hypertext Transfer Protocol (HTTP) Status Code Registry</a>.
 * <p>
 * The SC_... names make {@code org.folio.HttpStatus} a drop-in replacement for
 * {@code org.apache.hc.core5.http.HttpStatus} and {@code org.apache.http.HttpStatus}.
 * However, deprecated {@code SC_UNPROCESSABLE_ENTITY} is not provided, use
 * {@code SC_UNPROCESSABLE_CONTENT} instead.
 *
 * @see <a href="https://github.com/apache/httpcomponents-core/blob/5.4.x/httpcore5/src/main/java/org/apache/hc/core5/http/HttpStatus.java">
 *      org.apache.hc.core5.http.HttpStatus</a>
 * @see <a href="https://github.com/apache/httpcomponents-core/blob/4.4.x/httpcore/src/main/java/org/apache/http/HttpStatus.java">
 *      org.apache.http.HttpStatus</a>
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

  //--- 1xx Informational ---
  /** {@code 100 1xx Informational} (HTTP Semantics) */
  public static final int SC_INFORMATIONAL = 100;
  /** {@code 100 Continue} (HTTP Semantics) */
  public static final int SC_CONTINUE = 100;
  /** {@code 101 Switching Protocols} (HTTP Semantics)*/
  public static final int SC_SWITCHING_PROTOCOLS = 101;
  /** {@code 102 Processing} (WebDAV - RFC 2518) */
  public static final int SC_PROCESSING = 102;
  /** {@code 103 Early Hints (Early Hints - RFC 8297)}*/
  public static final int SC_EARLY_HINTS = 103;
  // --- 2xx Success ---
  /** {@code 2xx Success} (HTTP Semantics) */
  public static final int SC_SUCCESS = 200;
  /** {@code 200 OK} (HTTP Semantics) */
  public static final int SC_OK = 200;
  /** {@code 201 Created} (HTTP Semantics) */
  public static final int SC_CREATED = 201;
  /** {@code 202 Accepted} (HTTP Semantics) */
  public static final int SC_ACCEPTED = 202;
  /** {@code 203 Non Authoritative Information} (HTTP Semantics) */
  public static final int SC_NON_AUTHORITATIVE_INFORMATION = 203;
  /** {@code 204 No Content} (HTTP Semantics) */
  public static final int SC_NO_CONTENT = 204;
  /** {@code 205 Reset Content} (HTTP Semantics) */
  public static final int SC_RESET_CONTENT = 205;
  /** {@code 206 Partial Content} (HTTP Semantics) */
  public static final int SC_PARTIAL_CONTENT = 206;
  /**
   * {@code 207 Multi-Status} (WebDAV - RFC 2518)
   * or
   * {@code 207 Partial Update OK} (HTTP/1.1 - draft-ietf-http-v11-spec-rev-01?)
   */
  public static final int SC_MULTI_STATUS = 207;
  /**
   * {@code 208 Already Reported} (WebDAV - RFC 5842, p.30, section 7.1)
   */
  public static final int SC_ALREADY_REPORTED = 208;
  /**
   * {@code 226 IM Used} (Delta encoding in HTTP - RFC 3229, p. 30, section 10.4.1)
   */
  public static final int SC_IM_USED = 226;
  // --- 3xx Redirection ---
  /** {@code 3xx Redirection} (HTTP Semantics) */
  public static final int SC_REDIRECTION = 300;
  /** {@code 300 Multiple Choices} (HTTP Semantics) */
  public static final int SC_MULTIPLE_CHOICES = 300;
  /** {@code 301 Moved Permanently} (HTTP Semantics) */
  public static final int SC_MOVED_PERMANENTLY = 301;
  /** {@code 302 Moved Temporarily} (Sometimes {@code Found}) (HTTP Semantics) */
  public static final int SC_MOVED_TEMPORARILY = 302;
  /** {@code 303 See Other} (HTTP Semantics) */
  public static final int SC_SEE_OTHER = 303;
  /** {@code 304 Not Modified} (HTTP Semantics) */
  public static final int SC_NOT_MODIFIED = 304;
  /** {@code 305 Use Proxy} (HTTP Semantics) */
  public static final int SC_USE_PROXY = 305;
  /** {@code 307 Temporary Redirect} (HTTP Semantics) */
  public static final int SC_TEMPORARY_REDIRECT = 307;
  /** {@code 308 Permanent Redirect} (HTTP Semantics) */
  public static final int SC_PERMANENT_REDIRECT = 308;
  // --- 4xx Client Error ---
  /** {@code 4xx Client Error} (HTTP Semantics) */
  public static final int SC_CLIENT_ERROR = 400;
  /** {@code 400 Bad Request} (HTTP Semantics) */
  public static final int SC_BAD_REQUEST = 400;
  /** {@code 401 Unauthorized} (HTTP Semantics) */
  public static final int SC_UNAUTHORIZED = 401;
  /** {@code 402 Payment Required} (HTTP Semantics) */
  public static final int SC_PAYMENT_REQUIRED = 402;
  /** {@code 403 Forbidden} (HTTP Semantics) */
  public static final int SC_FORBIDDEN = 403;
  /** {@code 404 Not Found} (HTTP Semantics) */
  public static final int SC_NOT_FOUND = 404;
  /** {@code 405 Method Not Allowed} (HTTP Semantics) */
  public static final int SC_METHOD_NOT_ALLOWED = 405;
  /** {@code 406 Not Acceptable} (HTTP Semantics) */
  public static final int SC_NOT_ACCEPTABLE = 406;
  /** {@code 407 Proxy Authentication Required} (HTTP Semantics)*/
  public static final int SC_PROXY_AUTHENTICATION_REQUIRED = 407;
  /** {@code 408 Request Timeout} (HTTP Semantics) */
  public static final int SC_REQUEST_TIMEOUT = 408;
  /** {@code 409 Conflict} (HTTP Semantics) */
  public static final int SC_CONFLICT = 409;
  /** {@code 410 Gone} (HTTP Semantics) */
  public static final int SC_GONE = 410;
  /** {@code 411 Length Required} (HTTP Semantics) */
  public static final int SC_LENGTH_REQUIRED = 411;
  /** {@code 412 Precondition Failed} (HTTP Semantics) */
  public static final int SC_PRECONDITION_FAILED = 412;
  /** {@code 413 Request Entity Too Large} (HTTP Semantics) */
  public static final int SC_REQUEST_TOO_LONG = 413;
  /** {@code 414 Request-URI Too Long} (HTTP Semantics) */
  public static final int SC_REQUEST_URI_TOO_LONG = 414;
  /** {@code 415 Unsupported Media Type} (HTTP Semantics) */
  public static final int SC_UNSUPPORTED_MEDIA_TYPE = 415;
  /** {@code 416 Requested Range Not Satisfiable} (HTTP Semantics) */
  public static final int SC_REQUESTED_RANGE_NOT_SATISFIABLE = 416;
  /** {@code 417 Expectation Failed} (HTTP Semantics) */
  public static final int SC_EXPECTATION_FAILED = 417;
  /** {@code 421 Misdirected Request} (HTTP Semantics) */
  public static final int SC_MISDIRECTED_REQUEST = 421;
  /** {@code 422 Unprocessable Content} (HTTP Semantics).
   * Replaces deprecated {code SC_UNPROCESSABLE_ENTITY}.
   */
  public static final int SC_UNPROCESSABLE_CONTENT = 422;
  /** {@code 426 Upgrade Required} (HTTP Semantics) */
  public static final int SC_UPGRADE_REQUIRED = 426;
  /**
   * Static constant for a 419 error.
   * {@code 419 Insufficient Space on Resource}
   * (WebDAV - draft-ietf-webdav-protocol-05?)
   * or {@code 419 Proxy Reauthentication Required}
   * (HTTP/1.1 drafts?)
   */
  public static final int SC_INSUFFICIENT_SPACE_ON_RESOURCE = 419;
  /**
   * Static constant for a 420 error.
   * {@code 420 Method Failure}
   * (WebDAV - draft-ietf-webdav-protocol-05?)
   */
  public static final int SC_METHOD_FAILURE = 420;
  /** {@code 423 Locked} (WebDAV - RFC 2518) */
  public static final int SC_LOCKED = 423;
  /** {@code 424 Failed Dependency} (WebDAV - RFC 2518) */
  public static final int SC_FAILED_DEPENDENCY = 424;
  /** {@code 425 Too Early} (Using Early Data in HTTP - RFC 8470) */
  public static final int SC_TOO_EARLY = 425;
  /** {@code 428 Precondition Required} (Additional HTTP Status Codes - RFC 6585) */
  public static final int SC_PRECONDITION_REQUIRED = 428;
  /** {@code 429 Too Many Requests} (Additional HTTP Status Codes - RFC 6585) */
  public static final int SC_TOO_MANY_REQUESTS = 429;
  /** {@code 431 Request Header Fields Too Large} (Additional HTTP Status Codes - RFC 6585) */
  public static final int SC_REQUEST_HEADER_FIELDS_TOO_LARGE = 431;
  /** {@code 451 Unavailable For Legal Reasons} (Legal Obstacles - RFC 7725) */
  public static final int SC_UNAVAILABLE_FOR_LEGAL_REASONS = 451;
  // --- 5xx Server Error ---
  /** {@code 500 Server Error} (HTTP Semantics) */
  public static final int SC_SERVER_ERROR = 500;
  /** {@code 500 Internal Server Error} (HTTP Semantics) */
  public static final int SC_INTERNAL_SERVER_ERROR = 500;
  /** {@code 501 Not Implemented} (HTTP Semantics) */
  public static final int SC_NOT_IMPLEMENTED = 501;
  /** {@code 502 Bad Gateway} (HTTP Semantics) */
  public static final int SC_BAD_GATEWAY = 502;
  /** {@code 503 Service Unavailable} (HTTP Semantics) */
  public static final int SC_SERVICE_UNAVAILABLE = 503;
  /** {@code 504 Gateway Timeout} (HTTP Semantics) */
  public static final int SC_GATEWAY_TIMEOUT = 504;
  /** {@code 505 HTTP Version Not Supported} (HTTP Semantics) */
  public static final int SC_HTTP_VERSION_NOT_SUPPORTED = 505;
  /** {@code 506 Variant Also Negotiates} ( Transparent Content Negotiation - RFC 2295) */
  public static final int SC_VARIANT_ALSO_NEGOTIATES = 506;
  /** {@code 507 Insufficient Storage} (WebDAV - RFC 2518) */
  public static final int SC_INSUFFICIENT_STORAGE = 507;
  /**
   * {@code 508 Loop Detected} (WebDAV - RFC 5842, p.33, section 7.2)
   */
  public static final int SC_LOOP_DETECTED = 508;
  /**
   * {@code 510 Not Extended} (An HTTP Extension Framework - RFC 2774, p. 10, section 7)
   */
  public static final int SC_NOT_EXTENDED = 510;
  /** {@code  511 Network Authentication Required} (Additional HTTP Status Codes - RFC 6585) */
  public static final int SC_NETWORK_AUTHENTICATION_REQUIRED = 511;

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
