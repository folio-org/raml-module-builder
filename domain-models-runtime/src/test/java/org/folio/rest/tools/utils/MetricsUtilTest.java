package org.folio.rest.tools.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import org.junit.jupiter.api.Test;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;

class MetricsUtilTest {

  @Test
  void testConfig() {
    VertxOptions vopt = new VertxOptions();
    MetricsUtil.config(vopt, null, null, null, null);
    verifyConfig(vopt, "http://localhost:8086", "rmb", null, null);
    MetricsUtil.config(vopt, "a", "b", "c", "d");
    verifyConfig(vopt, "a", "b", "c", "d");
  }

  private void verifyConfig(VertxOptions vopt, String url, String db, String user, String pass) {
    JsonObject jo = vopt.getMetricsOptions().toJson().getJsonObject("influxDbOptions");
    assertEquals(url, jo.getString("uri"));
    assertEquals(db, jo.getString("db"));
    if (user == null) {
      assertFalse(jo.containsKey("userName"));
    } else {
      assertEquals(user, jo.getString("userName"));
    }
    if (pass == null) {
      assertFalse(jo.containsKey("password"));
    } else {
      assertEquals(pass, jo.getString("password"));
    }
  }

}
