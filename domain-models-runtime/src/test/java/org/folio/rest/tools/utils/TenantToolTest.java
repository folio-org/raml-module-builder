package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;
import org.folio.okapi.testing.UtilityClassTester;
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

  @Test
  public void lowerCase() {
    assertEquals("a", TenantTool.tenantId(Map.of("foo", "x", "x-okapi-tenant", "a")));
  }

  @Test
  public void upperCase() {
    assertEquals("b", TenantTool.tenantId(Map.of("bar", "y", "X-OKAPI-TENANT", "b", "baz", "z")));
  }
}
