package org.folio.dbschema;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;

import org.folio.dbschema.ForeignKeys;
import org.folio.dbschema.TableOperation;
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

    assertNull(fk.getTargetPath());
    fk.setTargetPath(Collections.singletonList(null));
    assertEquals(1, fk.getTargetPath().size());

    String result = fk.toString();
    assertTrue(result.contains("tableAlias=a"));
    assertTrue(result.contains("targetTable=b"));
    assertTrue(result.contains("targetTableAlias=c"));
  }

  @Test
  void constructors() {
    ForeignKeys foreignKeys2 = new ForeignKeys("other", "bee");
    ForeignKeys foreignKeys3 = new ForeignKeys("ref", "honey", TableOperation.DELETE);
    assertEquals("other", foreignKeys2.getFieldPath());
    assertEquals("ref", foreignKeys3.getFieldPath());
    assertEquals("bee", foreignKeys2.getTargetTable());
    assertEquals("honey", foreignKeys3.getTargetTable());
    assertEquals(TableOperation.ADD, foreignKeys2.gettOps());
    assertEquals(TableOperation.DELETE, foreignKeys3.gettOps());
  }

  @Test
  void targetTableQuote() {
    assertThrows(IllegalArgumentException.class, () -> new ForeignKeys("otherId", "bee's"));
    assertThrows(IllegalArgumentException.class, () -> new ForeignKeys("otherId", "rock'n'roll", TableOperation.ADD));
  }
}
