package org.folio.dbschema;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.folio.util.ResourceUtil;
import org.junit.jupiter.api.Test;

class SchemaTest {

  private Schema getSchema() {
    return getSchema("schema.json");
  }

  private Schema getSchema(String filename) {
    try {
      String dbJson = ResourceUtil.asString(filename);
      return ObjectMapperTool.getMapper().readValue(dbJson, Schema.class);
    } catch (IOException e) {
      System.out.println("XXXX");
      throw new UncheckedIOException(e);
    }
  }

  @Test
  void canSetupSchema() {
    Schema schema = getSchema();
    assertThat(schema.getTables().get(0).getMode(), is(nullValue()));
    assertThat(schema.getTables().get(0).getFullTextIndex().get(0).getFieldPath(), is(nullValue()));
    schema.setup();
    assertThat(schema.getTables().get(0).getMode(), is("new"));
    assertThat(schema.getTables().get(0).getFullTextIndex().get(0).getFieldPath(), is("(jsonb->>'title')"));
  }

  @Test
  public void failsWhenAuditingTableNameIsMissing() {
    Schema schema = getSchema();
    schema.getTables().get(0).setAuditingTableName(null);
    Exception e = assertThrows(IllegalArgumentException.class, () -> schema.setup());
    assertThat(e.getMessage(), is("auditingTableName missing for table item having \"withAuditing\": true"));
  }

  @Test
  public void failsWhenAuditingFieldNameIsMissing() {
    Schema schema = getSchema();
    schema.getTables().get(0).setAuditingFieldName(null);
    Exception e = assertThrows(IllegalArgumentException.class, () -> schema.setup());
    assertThat(e.getMessage(), is("auditingFieldName missing for table item having \"withAuditing\": true"));
  }

  @Test
  public void failsWhenFullTextIsCaseSensitive() {
    Schema schema = getSchema();
    schema.getTables().get(0).getFullTextIndex().get(0).setCaseSensitive(true);
    Exception e = assertThrows(IllegalArgumentException.class, () -> schema.setup());
    assertThat(e.getMessage(), is("full text index does not support case sensitive: title"));
  }
  
  @Test 
  public void testOptimisticLockingMode() {
    Schema schema = getSchema("schemaWithOptimisticLocking.json");
    assertThat(schema.getTables().get(0).getWithOptimisticLocking(), is(OptimisticLockingMode.OFF));
    assertThat(schema.getTables().get(1).getWithOptimisticLocking(), is(OptimisticLockingMode.LOG));
    assertThat(schema.getTables().get(2).getWithOptimisticLocking(), is(OptimisticLockingMode.FAIL));
    assertThat(schema.getTables().get(3).getWithOptimisticLocking(), nullValue());
  }
}
