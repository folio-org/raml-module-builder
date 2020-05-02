package org.folio.rest.persist.ddlgen;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class TableTest {
  @Test
  void invalidTableName() {
    Table table = new Table();
    assertThrows(IllegalArgumentException.class, () -> table.setTableName("foo&bar"));
  }
}
