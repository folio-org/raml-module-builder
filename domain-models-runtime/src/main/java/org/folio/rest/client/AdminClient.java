
package org.folio.rest.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMultipart;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;

public class AdminClient {

    private final static String GLOBAL_PATH = "/admin";
    private String tenantId;
    private HttpClientOptions options;
    private HttpClient httpClient;

    public AdminClient(String host, int port, String tenantId, boolean keepAlive) {
        this.tenantId = tenantId;
        options = new HttpClientOptions();
        options.setLogActivity(true);
        options.setKeepAlive(keepAlive);
        options.setDefaultHost(host);
        options.setDefaultPort(port);
        httpClient = io.vertx.core.Vertx.vertx().createHttpClient(options);
    }

    public AdminClient(String host, int port, String tenantId) {
        this(host, port, tenantId, true);
    }

    /**
     * Convenience constructor for tests ONLY!<br>Connect to localhost on 8081 as folio_demo tenant.
     * 
     */
    public AdminClient() {
        this("localhost", 8081, "folio_demo", false);
    }

    /**
     * Service endpoint "/admin/jstack"+queryParams.toString()
     * 
     */
    public void putJstack(Handler<HttpClientResponse> responseHandler) {
        StringBuilder queryParams = new StringBuilder("?");
        io.vertx.core.http.HttpClientRequest request = httpClient.put("/admin/jstack"+queryParams.toString());
        request.handler(responseHandler);
        request.putHeader("Accept", "text/plain");
        request.putHeader("Authorization", tenantId);
        request.putHeader("x-okapi-tenant", tenantId);
        request.end();
    }

    /**
     * Service endpoint "/admin/jstack"+queryParams.toString()
     * 
     */
    public void getJstack(Handler<HttpClientResponse> responseHandler) {
        StringBuilder queryParams = new StringBuilder("?");
        io.vertx.core.http.HttpClientRequest request = httpClient.get("/admin/jstack"+queryParams.toString());
        request.handler(responseHandler);
        request.putHeader("Accept", "text/html,text/plain");
        request.putHeader("Authorization", tenantId);
        request.putHeader("x-okapi-tenant", tenantId);
        request.end();
    }

    /**
     * Service endpoint "/admin/loglevel"+queryParams.toString()
     * 
     */
    public void putLoglevel(org.folio.rest.jaxrs.resource.AdminResource.Level level, String java_package, Handler<HttpClientResponse> responseHandler) {
        StringBuilder queryParams = new StringBuilder("?");
        if(level != null) {queryParams.append("level="+level.toString());}
        queryParams.append("&");
        if(java_package != null) {queryParams.append("java_package="+java_package);}
        queryParams.append("&");
        io.vertx.core.http.HttpClientRequest request = httpClient.put("/admin/loglevel"+queryParams.toString());
        request.handler(responseHandler);
        request.putHeader("Accept", "application/json,text/plain");
        request.putHeader("Authorization", tenantId);
        request.putHeader("x-okapi-tenant", tenantId);
        request.end();
    }

    /**
     * Service endpoint "/admin/loglevel"+queryParams.toString()
     * 
     */
    public void getLoglevel(Handler<HttpClientResponse> responseHandler) {
        StringBuilder queryParams = new StringBuilder("?");
        io.vertx.core.http.HttpClientRequest request = httpClient.get("/admin/loglevel"+queryParams.toString());
        request.handler(responseHandler);
        request.putHeader("Accept", "application/json,text/plain");
        request.putHeader("Authorization", tenantId);
        request.putHeader("x-okapi-tenant", tenantId);
        request.end();
    }

    /**
     * Service endpoint "/admin/memory"+queryParams.toString()
     * 
     */
    public void getMemory(boolean history, Handler<HttpClientResponse> responseHandler) {
        StringBuilder queryParams = new StringBuilder("?");
        queryParams.append("history="+history);
        queryParams.append("&");
        io.vertx.core.http.HttpClientRequest request = httpClient.get("/admin/memory"+queryParams.toString());
        request.handler(responseHandler);
        request.putHeader("Accept", "text/html,text/plain");
        request.putHeader("Authorization", tenantId);
        request.putHeader("x-okapi-tenant", tenantId);
        request.end();
    }

    /**
     * Service endpoint "/admin/uploadbinary"+queryParams.toString()
     * 
     */
    public void postUploadbinary(org.folio.rest.jaxrs.resource.AdminResource.PersistMethod persist_method, String bus_address, String file_name, InputStream inputStream, Handler<HttpClientResponse> responseHandler)
        throws IOException
    {
        StringBuilder queryParams = new StringBuilder("?");
        if(persist_method != null) {queryParams.append("persist_method="+persist_method.toString());}
        queryParams.append("&");
        if(bus_address != null) {queryParams.append("bus_address="+bus_address);}
        queryParams.append("&");
        if(file_name != null) {queryParams.append("file_name="+file_name);}
        queryParams.append("&");
        io.vertx.core.buffer.Buffer buffer = io.vertx.core.buffer.Buffer.buffer();
        java.io.ByteArrayOutputStream result = new java.io.ByteArrayOutputStream();
        byte[] buffer1 = new byte[1024];
        int length;

        while ((length = inputStream.read(buffer1)) != -1) {
        result.write(buffer1, 0, length);
        }
        buffer.appendBytes(result.toByteArray());
        io.vertx.core.http.HttpClientRequest request = httpClient.post("/admin/uploadbinary"+queryParams.toString());
        request.handler(responseHandler);
        request.putHeader("Content-type", "application/octet-stream");
        request.putHeader("Accept", "text/plain");
        request.putHeader("Authorization", tenantId);
        request.putHeader("x-okapi-tenant", tenantId);
        request.putHeader("Content-Length", buffer.length()+"");
        request.setChunked(true);
        request.write(buffer);
        request.end();
    }

    /**
     * Service endpoint "/admin/uploadmultipart"+queryParams.toString()
     * 
     */
    public void postUploadmultipart(org.folio.rest.jaxrs.resource.AdminResource.PersistMethod persist_method, String bus_address, String file_name, MimeMultipart mimeMultipart, Handler<HttpClientResponse> responseHandler)
        throws IOException, MessagingException
    {
        StringBuilder queryParams = new StringBuilder("?");
        if(persist_method != null) {queryParams.append("persist_method="+persist_method.toString());}
        queryParams.append("&");
        if(bus_address != null) {queryParams.append("bus_address="+bus_address);}
        queryParams.append("&");
        if(file_name != null) {queryParams.append("file_name="+file_name);}
        queryParams.append("&");
        io.vertx.core.buffer.Buffer buffer = io.vertx.core.buffer.Buffer.buffer();
        if(mimeMultipart != null) {int parts = mimeMultipart.getCount();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts; i++){
        javax.mail.BodyPart bp = mimeMultipart.getBodyPart(i);
        sb.append("----BOUNDARY\r\n")
        .append("Content-Disposition: ").append(bp.getDisposition()).append("; name=\"").append(bp.getFileName())
        .append("\"; filename=\"").append(bp.getFileName()).append("\"\r\n")
        .append("Content-Type: application/octet-stream\r\n")
        .append("Content-Transfer-Encoding: binary\r\n")
        .append("\r\n").append( bp.getContent() ).append("\r\n\r\n");}
        buffer.appendString(sb.append("----BOUNDARY\r\n").toString());}
        io.vertx.core.http.HttpClientRequest request = httpClient.post("/admin/uploadmultipart"+queryParams.toString());
        request.handler(responseHandler);
        request.putHeader("Content-type", "multipart/form-data; boundary=--BOUNDARY");
        request.putHeader("Accept", "text/plain");
        request.putHeader("Authorization", tenantId);
        request.putHeader("x-okapi-tenant", tenantId);
        request.putHeader("Content-Length", buffer.length()+"");
        request.setChunked(true);
        request.write(buffer);
        request.end();
    }

    /**
     * Service endpoint "/admin/collstats"+queryParams.toString()
     * 
     */
    public void putCollstats(Reader reader, Handler<HttpClientResponse> responseHandler)
        throws Exception
    {
        StringBuilder queryParams = new StringBuilder("?");
        io.vertx.core.buffer.Buffer buffer = io.vertx.core.buffer.Buffer.buffer();
        if(reader != null){buffer.appendString(org.apache.commons.io.IOUtils.toString(reader));}
        io.vertx.core.http.HttpClientRequest request = httpClient.put("/admin/collstats"+queryParams.toString());
        request.handler(responseHandler);
        request.putHeader("Content-type", "application/json");
        request.putHeader("Accept", "application/json,text/plain");
        request.putHeader("Authorization", tenantId);
        request.putHeader("x-okapi-tenant", tenantId);
        request.putHeader("Content-Length", buffer.length()+"");
        request.setChunked(true);
        request.write(buffer);
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
        return "ad08ff10dc6d99dcd265eef94f1c819f";
    }

}
