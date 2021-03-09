package org.folio.rest.tools.client;

import io.vertx.core.Vertx;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.folio.rest.tools.client.test.HttpClientMock2;

/**
 * @author shale
 *
 */
public class HttpClientFactory {

  static boolean isMock() {
    return Vertx.currentContext().config().containsKey(HttpClientMock2.MOCK_MODE);
  }

  public static HttpClientInterface getHttpClient(String host, int port, String tenantId, boolean keepAlive, int connTO,
      int idleTO, boolean autoCloseConnections, long cacheTO) {
    if (isMock()) {
      return new HttpClientMock2(host, port, tenantId, keepAlive, connTO, idleTO, autoCloseConnections, cacheTO);
    } else {
      return new HttpModuleClient2(host, port, tenantId, keepAlive, connTO, idleTO, autoCloseConnections, cacheTO);
    }
  }

  public static HttpClientInterface getHttpClient(String host, int port, String tenantId) {
    if (isMock()) {
      return new HttpClientMock2(host, port, tenantId);
    } else {
      return new HttpModuleClient2(host, port, tenantId);
    }
  }

  public static HttpClientInterface getHttpClient(String absHost, String tenantId) {
    if (isMock()) {
      return new HttpClientMock2(absHost,tenantId);
    } else {
      return new HttpModuleClient2(absHost, tenantId);
    }
  }

  public static HttpClientInterface getHttpClient(String host, int port, String tenantId, boolean autoCloseConnections) {
    if (isMock()) {
      return new HttpClientMock2(host, port, tenantId, autoCloseConnections);
    } else {
      return new HttpModuleClient2(host, port, tenantId, autoCloseConnections);
    }
  }

  public static HttpClientInterface getHttpClient(String absHost, String tenantId, boolean autoCloseConnections) {
    if (isMock()) {
      return new HttpClientMock2(absHost, tenantId, autoCloseConnections);
    } else {
      return new HttpModuleClient2(absHost, tenantId, autoCloseConnections);
    }
  }
}
