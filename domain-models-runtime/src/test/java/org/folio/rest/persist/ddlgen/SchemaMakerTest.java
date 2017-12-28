package org.folio.rest.persist.ddlgen;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

import java.io.IOException;

import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.util.ResourceUtil;
import org.junit.Test;

import freemarker.template.TemplateException;

public class SchemaMakerTest {
  private String tidy(String s) {
    return s
        .replaceAll("-- [^\n\r]*", " ")  // remove comment
        .replaceAll("[ \n\r]+", " ")     // merge multiple whitespace characters
        .replaceAll(" *\\( *", "(")      // remove space before and after (
        .replaceAll(" *\\) *", ")")      // remove space before and after )
        .replaceAll(";", ";\n");         // one line per sql statement
  }

  @Test
  public void lowerUnaccentIndex() throws IOException, TemplateException {
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
        PomReader.INSTANCE.getVersion(), PomReader.INSTANCE.getRmbVersion());

    String json = ResourceUtil.asString("templates/db_scripts/caseinsensitive.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
    assertThat(tidy(schemaMaker.generateDDL()), containsString(
        "CREATE INDEX IF NOT EXISTS item_title_idx ON harvard_circ.item(lower(f_unaccent(jsonb->>'title'))"));
  }
}
