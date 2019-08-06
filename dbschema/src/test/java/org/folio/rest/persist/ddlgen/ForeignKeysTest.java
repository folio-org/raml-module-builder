package org.folio.rest.persist.ddlgen;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ForeignKeysTest {

  @Test
  void testAlias() {

    ForeignKeys fk = new ForeignKeys();
    assertNull(fk.getTableAlias());
    assertNull(fk.getTargetTableAlias());

    String expected = "a";
    fk.setTableAlias(expected);
    assertEquals(expected, fk.getTableAlias());

    // return target if target alias is null
    expected = "b";
    fk.setTargetTable(expected);
    assertEquals(expected, fk.getTargetTableAlias());

    // return target alias if it is not null
    expected = "c";
    fk.setTargetTableAlias(expected);
    assertEquals(expected, fk.getTargetTableAlias());

    expected = fk.toString();
    assertTrue(expected.contains("tableAlias=a"));
    assertTrue(expected.contains("targetTable=b"));
    assertTrue(expected.contains("targetTableAlias=c"));
  }
}
