package org.folio.rest.persist.ddlgen;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ForeignKeysTest {

  @Test
  void testAlias() {
    ForeignKeys fk = new ForeignKeys();
    assertNull(fk.getTableAlias());
    assertNull(fk.getTargetTableAlias());

    fk.setTableAlias("a");
    assertEquals("a", fk.getTableAlias());

    fk.setTargetTable("b");
    assertEquals("b", fk.getTargetTable());
    assertEquals(null, fk.getTargetTableAlias());

    fk.setTargetTableAlias("c");
    assertEquals("c", fk.getTargetTableAlias());

    String result = fk.toString();
    assertTrue(result.contains("tableAlias=a"));
    assertTrue(result.contains("targetTable=b"));
    assertTrue(result.contains("targetTableAlias=c"));
  }
}
