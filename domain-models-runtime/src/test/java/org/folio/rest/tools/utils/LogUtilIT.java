package org.folio.rest.tools.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.impl.SocketAddressImpl;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// renaming to LogUtilTest fails on Jenkins
class LogUtilIT {
  private static Appender appender;
  private static Logger logger;

  @BeforeAll
  static void setUp() {
    appender = new Appender();
    logger = (Logger) LogManager.getLogger(LogUtil.class);
    logger.addAppender(appender);
  }

  @AfterAll
  static void tearDown() {
    logger.removeAppender(appender);
  }

  @BeforeEach
  void reset() {
    appender.message.setLength(0);
  }

  String message() {
    return appender.message.toString();
  }

  @Test
  void logNullRountingContext() {
    LogUtil.formatStatsLogMessage(null, 2, "tenantId", "body");
    assertThat(message(), is("2 tid=tenantId body"));
  }

  @Test
  void logRoutingContextReturningNull() {
    LogUtil.formatStatsLogMessage(mock(RoutingContext.class), 3, "diku", "myBody");
    assertThat(message(), is("null null null null null -1 -1 3 tid=diku null myBody"));
  }

  @Test
  void logRequestAndResponseReturningNullOrZero() {
    RoutingContext routingContext = mock(RoutingContext.class);
    when(routingContext.request()).thenReturn(mock(HttpServerRequest.class));
    when(routingContext.response()).thenReturn(mock(HttpServerResponse.class));
    LogUtil.formatStatsLogMessage(routingContext, 4, "diku", "myBody");
    assertThat(message(), is("null null null null null 0 0 4 tid=diku null myBody"));
  }

  @Test
  void logRemoteAddress() {
    RoutingContext routingContext = mock(RoutingContext.class);
    when(routingContext.request()).thenReturn(mock(HttpServerRequest.class));
    when(routingContext.request().remoteAddress()).thenReturn(new SocketAddressImpl("remoteAddress"));
    LogUtil.formatStatsLogMessage(routingContext, 5, "beeUni", "aBody");
    assertThat(message(), is("remoteAddress null null null null -1 -1 5 tid=beeUni null aBody"));
  }

  private static class Appender extends AbstractAppender {
    StringBuilder message = new StringBuilder();

    protected Appender() {
      super("MockedAppender", null, null, false, null);
      start();
    }

    @Override
    public void append(LogEvent event) {
      message.append(event.getMessage().getFormattedMessage());
    }
  }
}
