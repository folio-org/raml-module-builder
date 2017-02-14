package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;

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
}
