package org.folio.rest.persist.ddlgen;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.util.ResourceUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
    assertThat(result, containsString("CREATE INDEX IF NOT EXISTS audit_test_tenantapi_testTenantapiAudit_id_idx "));
    // auditingSnippets
    assertThat(result, allOf(containsString("PERFORM 1;"), containsString("PERFORM 2;"), containsString("PERFORM 3;"),
                             containsString("var1 TEXT;"), containsString("var2 TEXT;"), containsString("var3 TEXT;")));
  }

  @Test
  public void canCreateIndexPath() throws IOException, TemplateException {
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.UPDATE,
        "mod-foo-18.2.3", "mod-foo-18.2.4");
    String json = ResourceUtil.asString("templates/db_scripts/compoundIndex.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));

    String result = schemaMaker.getSchema().getTables().get(0).getFullTextIndex().get(0).getFinalSqlExpression("tablea");
    assertThat(result,containsString("concat_space_sql(tablea.jsonb->>'field1' , tablea.jsonb->>'field2')"));
    result = schemaMaker.getSchema().getTables().get(0).getGinIndex().get(0).getFinalSqlExpression("tablea");
    assertThat(result,containsString("concat_space_sql(tablea.jsonb->>'firstName' , tablea.jsonb->>'lastName')"));
  }

  @Test
  public void canCreateIndexPath2() throws IOException, TemplateException {
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.UPDATE,
        "mod-foo-18.2.3", "mod-foo-18.2.4");
    String json = ResourceUtil.asString("templates/db_scripts/compoundIndex.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
    String result = schemaMaker.getSchema().getTables().get(1).getGinIndex().get(0).getFinalSqlExpression("tableb");
    assertThat(result,containsString("lower(concat_space_sql(jsonb->>'city', jsonb->>'state'))"));
    result = schemaMaker.getSchema().getTables().get(1).getFullTextIndex().get(0).getFinalSqlExpression("tableb");
    assertThat(result,containsString("lower(concat_space_sql(jsonb->>'field1', jsonb->>'field2'))"));
  }

  @Test
  public void canCreateCompoundIndex() throws IOException, TemplateException {
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.UPDATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4");
    String json = ResourceUtil.asString("templates/db_scripts/compoundIndex.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
    String result = schemaMaker.generateDDL();

    assertThat(result,containsString("CREATE INDEX IF NOT EXISTS tablea_ftfield_idx_ft"));
    assertThat(result, containsString("concat_space_sql(tablea.jsonb->>'firstName' , tablea.jsonb->>'lastName')"));

    assertThat(result,containsString("((concat_space_sql(concat_array_object_values(tablea.jsonb->'user','firstName') , concat_array_object_values(tablea.jsonb->'user','lastName'))) gin_trgm_ops)"));
    assertThat(result,containsString("((concat_space_sql(concat_array_object_values(tablea.jsonb->'user'->'info','firstName') , concat_array_object_values(tablea.jsonb->'user'->'info','lastName'))) gin_trgm_ops)"));
    assertThat(result,containsString("( to_tsvector('simple', lower(concat_space_sql(concat_array_object_values(tablea.jsonb->'field1','firstName') , concat_array_object_values(tablea.jsonb->'field2','lastName')))) )"));
    assertThat(result,containsString("( to_tsvector('simple', lower(concat_space_sql(concat_array_object_values(tablea.jsonb->'field1'->'info','firstName') , concat_array_object_values(tablea.jsonb->'field2'->'info','lastName')))) )"));
  }

  @Test
  public void canCreateSQLExpressionIndex() throws IOException, TemplateException {
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.UPDATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4");
    String json = ResourceUtil.asString("templates/db_scripts/compoundIndex.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));
    String result = schemaMaker.generateDDL();

    assertThat(result, containsString("lower(concat_space_sql(jsonb->>'field1', jsonb->>'field2'))"));
  }

  @Test
  public void failsWhenAuditingTableNameIsMissing() throws Exception {
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.UPDATE,
        "mod-foo-18.2.3", "mod-foo-18.2.4");
    String json = ResourceUtil.asString("templates/db_scripts/schemaWithAudit.json");
    Schema schema  = ObjectMapperTool.getMapper().readValue(json, Schema.class);
    schema.getTables().get(0).setAuditingTableName(null);
    schemaMaker.setSchema(schema);

    thrown.expectMessage("auditingTableName missing");
    schemaMaker.generateDDL();
  }

  @Test
  public void failsWhenAuditingFieldNameIsMissing() throws Exception {
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.UPDATE,
        "mod-foo-18.2.3", "mod-foo-18.2.4");
    String json = ResourceUtil.asString("templates/db_scripts/schemaWithAudit.json");
    Schema schema  = ObjectMapperTool.getMapper().readValue(json, Schema.class);
    schema.getTables().get(0).setAuditingFieldName(null);
    schemaMaker.setSchema(schema);

    thrown.expectMessage("auditingFieldName missing");
    schemaMaker.generateDDL();
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
        "CREATE INDEX IF NOT EXISTS item_title_idx ON harvard_circ.item(left(lower(f_unaccent(jsonb->>'title')),600))"));
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
  public void createScriptFromFilePresent() throws IOException, TemplateException {

    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4");

    String json = ResourceUtil.asString("templates/db_scripts/scriptWithSnippetPath.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));

    assertThat(tidy(schemaMaker.generateDDL()), containsString(
      "select * from file_start;"));
  }

  @Test
  public void createScriptFromFileAndSnippetPresent() throws IOException, TemplateException {

    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4");

    String json = ResourceUtil.asString("templates/db_scripts/scriptWithSnippetPathAndSnippet.json");
    schemaMaker.setSchema(ObjectMapperTool.getMapper().readValue(json, Schema.class));

    assertThat(tidy(schemaMaker.generateDDL()), containsString(
      "select * from start;"));

    assertThat(tidy(schemaMaker.generateDDL()), containsString(
      "select * from file_start;"));
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
    // except full text which only obeys f_unaccent
    assertThat(ddl, containsString("(left(lower(f_unaccent(jsonb->>'id')),600))"));
    assertThat(ddl, containsString("(left(lower(f_unaccent(jsonb->>'name')),600))"));
    assertThat(ddl, containsString("((lower(f_unaccent(jsonb->>'type')))text_pattern_ops)"));
    assertThat(ddl, containsString("GIN((lower(f_unaccent(jsonb->>'title')))gin_trgm_ops)"));
    assertThat(ddl, containsString("GIN(to_tsvector('simple', f_unaccent(jsonb->>'title')))"));
    assertThat(ddl, containsString("GIN(to_tsvector('simple',(jsonb->>'author')))"));
  }

}
