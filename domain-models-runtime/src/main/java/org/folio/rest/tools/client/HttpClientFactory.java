package org.folio.rest.tools.client;

import io.vertx.core.DeploymentOptions;
import org.folio.rest.RestVerticle;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.folio.rest.tools.client.test.HttpClientMock2;

/**
 * Factory of HTTP clients with interface {@link HttpClientInterface}.
 *
 * <p>It returns an {@link HttpClientMock2} instance if mocking is enabled and
 * an {@link HttpModuleClient2} instance if mocking is disabled.
 *
 * <p>Mocking is disabled by default. It can be enabled by setting the system property
 * {@link HttpClientMock2.MOCK_MODE} to any value (for example in pom.xml),
 * or by setting the {@link DeploymentOptions} property {@link HttpClientMock2.MOCK_MODE}
 * to any value when deploying {@link RestVerticle}. It can programmatically been enabled
 * or disabled by calling {@link #setMockEnabled(boolean)}.
 *
 * @deprecated Use {@link io.vertx.ext.web.client.WebClient} for generic HTTP client or generated client and
 * mock server with {@link io.vertx.core.Vertx#createHttpServer()} or use a mocking utility.
 */
@Deprecated
public class HttpClientFactory {


  private static boolean mock = false;

  static {
    if(System.getProperty(HttpClientMock2.MOCK_MODE) != null ){
      mock = true;
    }
  }

  public static void setMockEnabled(boolean mock) {
    HttpClientFactory.mock = mock;
  }

  public static HttpClientInterface getHttpClient(String host, int port, String tenantId, boolean keepAlive, int connTO,
      int idleTO, boolean autoCloseConnections, long cacheTO) {
    if(mock){
      return new HttpClientMock2(host, port, tenantId, keepAlive, connTO, idleTO, autoCloseConnections, cacheTO);
    }else{
      return new HttpModuleClient2(host, port, tenantId, keepAlive, connTO, idleTO, autoCloseConnections, cacheTO);
    }
  }

  public static HttpClientInterface getHttpClient(String host, int port, String tenantId) {
    if(mock){
      return new HttpClientMock2(host, port, tenantId);
    }else{
      return new HttpModuleClient2(host, port, tenantId);
    }
  }

  public static HttpClientInterface getHttpClient(String absHost, String tenantId) {
    if(mock){
      return new HttpClientMock2(absHost,tenantId);
    }else{
      return new HttpModuleClient2(absHost, tenantId);
    }
  }

  public static HttpClientInterface getHttpClient(String host, int port, String tenantId, boolean autoCloseConnections) {
    if(mock){
      return new HttpClientMock2(host, port, tenantId, autoCloseConnections);
    }else{
      return new HttpModuleClient2(host, port, tenantId, autoCloseConnections);
    }
  }

  public static HttpClientInterface getHttpClient(String absHost, String tenantId, boolean autoCloseConnections) {
    if(mock){
      return new HttpClientMock2(absHost, tenantId, autoCloseConnections);
    }else{
      return new HttpModuleClient2(absHost, tenantId, autoCloseConnections);
    }
  }
}
