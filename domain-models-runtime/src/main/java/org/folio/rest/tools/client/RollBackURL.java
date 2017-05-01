package org.folio.rest.tools.client;

import io.vertx.core.http.HttpMethod;

/**
 * @author shale
 *
 */
public class RollBackURL {

  String endpoint;
  HttpMethod method;
  BuildCQL cql;

  public RollBackURL(String endpoint, HttpMethod method, BuildCQL cql) {
    super();
    this.endpoint = endpoint;
    this.method = method;
    this.cql = cql;
  }

  public RollBackURL(String endpoint, HttpMethod method) {
    super();
    this.endpoint = endpoint;
    this.method = method;
  }

  public RollBackURL(String endpoint) {
    this(endpoint, HttpMethod.DELETE);
  }

}
