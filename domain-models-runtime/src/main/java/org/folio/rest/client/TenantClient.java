
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
    public void post(Handler<HttpClientResponse> responseHandler) {
        StringBuilder queryParams = new StringBuilder("?");
        io.vertx.core.http.HttpClientRequest request = httpClient.post(GLOBAL_PATH);
        request.handler(responseHandler);
        request.putHeader("Accept", "application/json,text/plain");
        if(tenantId != null){
         request.putHeader("Authorization", tenantId);
         request.putHeader("x-okapi-tenant", tenantId);
        }
        request.end();
    }

    /**
     * Service endpoint GLOBAL_PATH
     * 
     */
    public void delete(Handler<HttpClientResponse> responseHandler) {
        StringBuilder queryParams = new StringBuilder("?");
        io.vertx.core.http.HttpClientRequest request = httpClient.delete(GLOBAL_PATH);
        request.handler(responseHandler);
        request.putHeader("Accept", "application/json,text/plain");
        if(tenantId != null){
         request.putHeader("Authorization", tenantId);
         request.putHeader("x-okapi-tenant", tenantId);
        }
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
        request.putHeader("Accept", "text/plain");
        if(tenantId != null){
         request.putHeader("Authorization", tenantId);
         request.putHeader("x-okapi-tenant", tenantId);
        }
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
        return "e4d0df9ff7fa6ef89fed79f477df63b3";
    }

}
