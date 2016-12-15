
package org.folio.rest.client;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;

public class TenantClient {

    private final static String GLOBAL_PATH = "/tenant";
    private String tenantId;
    private HttpClientOptions options;
    private HttpClient httpClient;

    public TenantClient(String host, int port, String tenantId, boolean keepAlive) {
        this.tenantId = tenantId;
        options = new HttpClientOptions();
        options.setLogActivity(true);
        options.setKeepAlive(keepAlive);
        options.setDefaultHost(host);
        options.setDefaultPort(port);
        httpClient = io.vertx.core.Vertx.vertx().createHttpClient(options);
    }

    public TenantClient(String host, int port, String tenantId) {
        this(host, port, tenantId, true);
    }

    /**
     * Convenience constructor for tests ONLY!<br>Connect to localhost on 8081 as folio_demo tenant.
     * 
     */
    public TenantClient() {
        this("localhost", 8081, "folio_demo", false);
    }

    /**
     * Service endpoint GLOBAL_PATH
     * 
     */
    public void delete(Handler<HttpClientResponse> responseHandler) {
        StringBuilder queryParams = new StringBuilder("?");
        io.vertx.core.http.HttpClientRequest request = httpClient.delete(GLOBAL_PATH);
        request.handler(responseHandler);
        request.putHeader("Accept", "text/html,text/plain");
        request.putHeader("Authorization", tenantId);
        request.putHeader("x-okapi-tenant", tenantId);
        request.end();
    }

    /**
     * Service endpoint GLOBAL_PATH
     * 
     */
    public void post(Handler<HttpClientResponse> responseHandler) {
        StringBuilder queryParams = new StringBuilder("?");
        io.vertx.core.http.HttpClientRequest request = httpClient.post(GLOBAL_PATH);
        request.handler(responseHandler);
        request.putHeader("Accept", "application/json,text/plain");
        request.putHeader("Authorization", tenantId);
        request.putHeader("x-okapi-tenant", tenantId);
        request.end();
    }

    /**
     * Service endpoint GLOBAL_PATH
     * 
     */
    public void get(Handler<HttpClientResponse> responseHandler) {
        StringBuilder queryParams = new StringBuilder("?");
        io.vertx.core.http.HttpClientRequest request = httpClient.get(GLOBAL_PATH);
        request.handler(responseHandler);
        request.putHeader("Accept", "text/html,text/plain");
        request.putHeader("Authorization", tenantId);
        request.putHeader("x-okapi-tenant", tenantId);
        request.end();
    }

    /**
     * Close the client. Closing will close down any pooled connections. Clients should always be closed after use.
     * 
     */
    public void close() {
        httpClient.close();
    }

    public String checksum() {
        return "078254be438fe1a9f471811cc01b9046";
    }

}
