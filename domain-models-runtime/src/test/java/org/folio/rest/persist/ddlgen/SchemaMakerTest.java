package org.folio.rest.persist.ddlgen;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.util.ResourceUtil;
import org.hamcrest.junit.ExpectedException;
import org.junit.Rule;
import org.junit.Test;

import freemarker.template.TemplateException;

public class SchemaMakerTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private String tidy(String s) {
    return s
        .replaceAll("-- [^\n\r]*", " ")  // remove comment
        .replaceAll("[ \n\r]+", " ")     // merge multiple whitespace characters
        .replaceAll(" *\\( *", "(")      // remove space before and after (
        .replaceAll(" *\\) *", ")")      // remove space before and after )
        .replaceAll(";", ";\n");         // one line per sql statement
  }

  @Test
  public void canCreateAuditedTable() throws IOException, TemplateException {
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.UPDATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4");
    String json = ResourceUtil.asString("templates/db_scripts/schemaWithAudit.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
    String result = schemaMaker.generateDDL();

    assertThat(result, containsString("CREATE TABLE IF NOT EXISTS harvard_circ.audit_test_tenantapi"));
    assertThat(result, containsString("CREATE OR REPLACE FUNCTION harvard_circ.audit_test_tenantapi_changes() RETURNS TRIGGER"));
    // index for field in audit table
    assertThat(result, containsString("CREATE INDEX IF NOT EXISTS audit_test_tenantapi_item_id_idx "));
  }

  @Test
  public void failsWhenGenerateID() throws Exception {
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-0.2.1-SNAPSHOT.2", "mod-foo-18.2.1-SNAPSHOT.2");
    String json = ResourceUtil.asString("templates/db_scripts/schemaGenerateId.json");
    thrown.expectMessage("Unrecognized field \"generateId\"");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
    schemaMaker.generateDDL();
  }

  @Test
  public void failsWhenPkColumnName() throws Exception {
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-0.2.1-SNAPSHOT.2", "mod-foo-18.2.1-SNAPSHOT.2");
    String json = ResourceUtil.asString("templates/db_scripts/schemaPkColumnName.json");
    thrown.expectMessage("Unrecognized field \"pkColumnName\"");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
    schemaMaker.generateDDL();
  }

  @Test
  public void pkColumnName() {
    assertThat(new Table().getPkColumnName(), is("id"));
    assertThat(new View().getPkColumnName(), is("id"));
  }

  @Test
  public void failsWhenPopulateJsonWithId() throws  TemplateException {

    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-0.2.1-SNAPSHOT.2", "mod-foo-18.2.1-SNAPSHOT.2");
    try {
      String json = ResourceUtil.asString("templates/db_scripts/schemaPopulateJsonWithId.json");
      schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
      schemaMaker.generateDDL();
      fail();

    } catch(IOException e) {
      assertThat(tidy(e.getMessage()), containsString(
          "Unrecognized field \"populateJsonWithId\""));
    }
  }

  @Test
  public void lowerUnaccentIndex() throws IOException, TemplateException {

    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-0.2.1-SNAPSHOT.2", "mod-foo-18.2.1-SNAPSHOT.2");

    String json = ResourceUtil.asString("templates/db_scripts/caseinsensitive.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
    assertThat(tidy(schemaMaker.generateDDL()), containsString(
        "CREATE INDEX IF NOT EXISTS item_title_idx ON harvard_circ.item(lower(f_unaccent(jsonb->>'title'))"));
  }

  @Test
  public void scriptExistsAndDoesntUpgrade() throws IOException, TemplateException {

    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.UPDATE,
      "mod-foo-18.2.1-SNAPSHOT.9", "mod-foo-18.2.3");

    String json = ResourceUtil.asString("templates/db_scripts/scriptexists.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));

    assertThat(tidy(schemaMaker.generateDDL()), containsString(
        "select * from start;"));

    assertFalse("generated schema contains 'select * from end;' but it should not",
      tidy(schemaMaker.generateDDL()).contains("select * from end;"));
  }

  @Test
  public void createBothScriptsPresent() throws IOException, TemplateException {

    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-18.2.1-SNAPSHOT.9", "mod-foo-18.2.3");

    String json = ResourceUtil.asString("templates/db_scripts/scriptexists.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));

    assertThat(tidy(schemaMaker.generateDDL()), containsString(
        "select * from start;"));

    assertThat(tidy(schemaMaker.generateDDL()), containsString(
        "select * from end;"));
  }

  @Test
  public void createBothScriptsPresent2() throws IOException, TemplateException {

    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4");

    String json = ResourceUtil.asString("templates/db_scripts/scriptexists.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));

    assertThat(tidy(schemaMaker.generateDDL()), containsString(
        "select * from start;"));

    assertThat(tidy(schemaMaker.generateDDL()), containsString(
        "select * from end;"));
  }


  @Test
  public void delete() throws IOException, TemplateException {

    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.DELETE,
      "mod-foo-18.2.3", "mod-foo-18.2.4");

    String json = ResourceUtil.asString("templates/db_scripts/scriptexists.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));

    assertThat(tidy(schemaMaker.generateDDL()), containsString(
        "REVOKE ALL PRIVILEGES ON DATABASE"));
  }

  @Test
  public void createNeitherScripts() throws IOException, TemplateException {

    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.UPDATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4");

    String json = ResourceUtil.asString("templates/db_scripts/scriptexists.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));

    assertFalse("generated schema contains 'select * from start;' but it should not",
      tidy(schemaMaker.generateDDL()).contains("select * from start;"));

    assertFalse("generated schema contains 'select * from end;' but it should not",
      tidy(schemaMaker.generateDDL()).contains("select * from end;"));
  }

  @Test
  public void createBothScriptsPresentNoVersionsPassedIn() throws IOException, TemplateException {

    //TODO , once validation of versions is added this should change

    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      "18.2.0", "18.2.3");

    String json = ResourceUtil.asString("templates/db_scripts/scriptexists.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));

    assertThat(tidy(schemaMaker.generateDDL()), containsString(
        "select * from start;"));

    assertThat(tidy(schemaMaker.generateDDL()), containsString(
        "select * from end;"));
  }

  @Test
  public void badVersions() throws IOException, TemplateException {

    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      null, null);

    String json = ResourceUtil.asString("templates/db_scripts/scriptexists.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));

    assertThat(tidy(schemaMaker.generateDDL()), containsString(
        "select * from start;"));

    assertThat(tidy(schemaMaker.generateDDL()), containsString(
        "select * from end;"));
  }

  /**
   * If specified, gin index should contain f_unaccent and lower functions.
   * As reported in https://issues.folio.org/browse/MODINVSTOR-266 and
   * https://issues.folio.org/browse/RMB-330
   *
   * @throws IOException
   * @throws TemplateException
   */
  @Test
  public void ginIndex() throws IOException, TemplateException {

    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      null, null);

    String json = ResourceUtil.asString("templates/db_scripts/test_indexes.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));

    String ddl = tidy(schemaMaker.generateDDL());

    // by default all indexes are wrapped with lower/f_unaccent
    // except full text index which uses to_tsvector to normalize text token
    assertTrue(ddl.contains("(lower(f_unaccent(jsonb->>'id')))"));
    assertTrue(ddl.contains("(lower(f_unaccent(jsonb->>'name')))"));
    assertTrue(ddl.contains("((lower(f_unaccent(jsonb->>'type')))text_pattern_ops)"));
    assertTrue(ddl.contains("GIN((lower(f_unaccent(jsonb->>'title')))gin_trgm_ops)"));
    assertTrue(ddl.contains("GIN(to_tsvector('english',(jsonb->>'title')))"));
  }

}
