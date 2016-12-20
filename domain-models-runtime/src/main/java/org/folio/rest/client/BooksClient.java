
package org.folio.rest.client;

import java.math.BigDecimal;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;

public class BooksClient {

    private final static String GLOBAL_PATH = "/books";
    private String tenantId;
    private HttpClientOptions options;
    private HttpClient httpClient;

    public BooksClient(String host, int port, String tenantId, boolean keepAlive) {
        this.tenantId = tenantId;
        options = new HttpClientOptions();
        options.setLogActivity(true);
        options.setKeepAlive(keepAlive);
        options.setDefaultHost(host);
        options.setDefaultPort(port);
        io.vertx.core.Context context = io.vertx.core.Vertx.currentContext();
        if(context == null){
          httpClient = io.vertx.core.Vertx.vertx().createHttpClient(options);
        }
        else{
          httpClient = io.vertx.core.Vertx.currentContext().owner().createHttpClient(options);
        }
    }

    public BooksClient(String host, int port, String tenantId) {
        this(host, port, tenantId, true);
    }

    /**
     * Convenience constructor for tests ONLY!<br>Connect to localhost on 8081 as folio_demo tenant.
     * 
     */
    public BooksClient() {
        this("localhost", 8081, "folio_demo", false);
    }

    /**
     * Service endpoint GLOBAL_PATH
     * 
     */
    public void get(String author, BigDecimal publicationYear, BigDecimal rating, String isbn, Handler<HttpClientResponse> responseHandler) {
        StringBuilder queryParams = new StringBuilder("?");
        if(author != null) {queryParams.append("author="+author);
        queryParams.append("&");}
        if(publicationYear != null) {queryParams.append("publicationYear="+publicationYear);
        queryParams.append("&");}
        if(rating != null) {queryParams.append("rating="+rating);
        queryParams.append("&");}
        if(isbn != null) {queryParams.append("isbn="+isbn);
        queryParams.append("&");}
        io.vertx.core.http.HttpClientRequest request = httpClient.get(GLOBAL_PATH);
        request.handler(responseHandler);
        request.putHeader("Accept", "application/json");
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
    public void put(BigDecimal access_token, Handler<HttpClientResponse> responseHandler) {
        StringBuilder queryParams = new StringBuilder("?");
        if(access_token != null) {queryParams.append("access_token="+access_token);
        queryParams.append("&");}
        io.vertx.core.http.HttpClientRequest request = httpClient.put(GLOBAL_PATH);
        request.handler(responseHandler);
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
        return "784f047c91d4e0b8af33798100c907f4";
    }

}
