package org.folio.cql2pgjson.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class CqlUtilsTest {

  @Test
  public void testGetFieldNameFromCqlField() {
    String expected = "jsonb";
    String cqlField = "diku_mod_users.users.jsonb";
    assertEquals(expected, CqlUtils.getFieldNameFromCqlField(cqlField));
    cqlField = "users.jsonb";
    assertEquals(expected, CqlUtils.getFieldNameFromCqlField(cqlField));
    cqlField = "jsonb";
    assertEquals(expected, CqlUtils.getFieldNameFromCqlField(cqlField));
  }

  @Test
  public void testGetTableNameFromCqlField() {
    String expected = "users";
    String cqlField = "diku_mod_users.users.jsonb";
    assertEquals(expected, CqlUtils.getTableNameFromCqlField(cqlField));
    cqlField = "users.jsonb";
    assertEquals(expected, CqlUtils.getTableNameFromCqlField(cqlField));
    cqlField = "jsonb";
    assertNull(CqlUtils.getTableNameFromCqlField(cqlField));
  }

  @Test
  public void testGetFieldNameFromIndexJson() {
    String expected = "users.jsonb";
    String indexJson = "users.jsonb->'address'->'zip'";
    assertEquals(expected, CqlUtils.getFieldNameFromIndexJson(indexJson));
  }

  @Test
  public void testGetIndexFromIndexJson() {
    String expected = "address.zip";
    String indexJson = "users.jsonb->'address'->'zip'";
    assertEquals(expected, CqlUtils.getIndexNameFromIndexJson(indexJson));
  }
}
