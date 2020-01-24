package org.folio.rest.persist.ddlgen;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.util.ResourceUtil;
import org.junit.jupiter.api.Test;

class SchemaTest {

  private Schema getSchema() {
    try {
      String dbJson = ResourceUtil.asString("schema.json");
      return ObjectMapperTool.getMapper().readValue(dbJson, org.folio.rest.persist.ddlgen.Schema.class);
    } catch (IOException e) {
      System.out.println("XXXX");
      throw new UncheckedIOException(e);
    }
  }

  @Test
  void canSetupSchema() {
    getSchema().setup();
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
}
