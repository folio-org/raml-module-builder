package org.folio.dbschema;

import static org.junit.jupiter.api.Assertions.*;

import org.folio.dbschema.Table;
import org.junit.jupiter.api.Test;

class TableTest {
  @Test
  void invalidTableName() {
    Table table = new Table();
    assertThrows(IllegalArgumentException.class, () -> table.setTableName("foo&bar"));
  }
}
