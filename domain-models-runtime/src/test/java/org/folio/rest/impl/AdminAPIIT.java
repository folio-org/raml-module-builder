package org.folio.rest.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.folio.HttpStatus;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import io.vertx.core.Vertx;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class AdminAPIIT {
  private static final String tenantId = "folio_shared";
  protected static Vertx vertx;
  private static Map<String,String> okapiHeaders = new HashMap<>();

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4j2LogDelegateFactory");
    okapiHeaders.put("TenantId", tenantId);
  }

  @Rule
  public Timeout rule = Timeout.seconds(20);

  @BeforeClass
  public static void setUpClass() {
    vertx = VertxUtils.getVertxWithExceptionHandler();
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void canGetAdminDbCacheSummary(TestContext context) {
    new AdminAPI().getAdminDbCacheSummary(okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      OutStream outStream = (OutStream) response.getEntity();
      assertThat(outStream.getData().toString(), containsString("pg_statistic"));
    }), vertx.getOrCreateContext());
  }
}
