package org.folio.rest.tools.client;

import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.folio.rest.tools.client.test.HttpClientMock2;

/**
 * @author shale
 *
 */
public class HttpClientFactory {


  private static boolean mock = false;

  static {
    if(System.getProperty("mock.httpclient") != null){
      mock = true;
    }
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
