package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.folio.okapi.testing.UtilityClassTester;
import org.folio.rest.RestVerticle;
import org.junit.Test;

public class TenantToolTest {
  @Test
  public void utilityClass() {
    UtilityClassTester.assertUtilityClass(TenantTool.class);
  }

  @Test
  public void testNull() {
    assertEquals("folio_shared", TenantTool.calculateTenantId(null));
  }

  @Test
  public void testA() {
    assertEquals("a", TenantTool.calculateTenantId("a"));
  }

  @Test(expected = NullPointerException.class)
  public void headersNull() {
    TenantTool.tenantId(null);
  }

  @Test
  public void undefined() {
    assertEquals("folio_shared", TenantTool.tenantId(Collections.emptyMap()));
  }

  private Map<String, String> map(String key, String value) {
    Map<String, String> map = new HashMap<>(1);
    map.put(key, value);
    return map;
  }

  @Test
  public void b() {
    assertEquals("b", TenantTool.tenantId(map(RestVerticle.OKAPI_HEADER_TENANT, "b")));
  }
}
