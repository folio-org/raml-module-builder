package org.folio.rest.persist.ddlgen;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Arrays;

import org.folio.dbschema.ForeignKeys;
import org.folio.dbschema.Schema;
import org.folio.dbschema.Table;
import org.folio.dbschema.TenantOperation;
import org.folio.dbschema.View;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.util.ResourceUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;

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

  /**
   * @param name resource path of the input schema json file
   * @return Schema the input file was converted into
   */
  private Schema schema(String name) throws JsonMappingException, JsonProcessingException {
    String json = ResourceUtil.asString(name);
    return ObjectMapperTool.getMapper().readValue(json, Schema.class);
  }

  /**
   * @return SchemaMaker constructed from the schemaName json file and the other parameters.
   */
  private SchemaMaker schemaMaker(String tenant, String module, TenantOperation mode,
      String previousVersion, String newVersion, String schemaName)
          throws JsonMappingException, JsonProcessingException {
    SchemaMaker schemaMaker = new SchemaMaker(tenant, module, mode, previousVersion, newVersion);
    schemaMaker.setSchema(schema(schemaName));
    return schemaMaker;
  }

  @Test
  void failGenerateOptimisticLocking() {
    assertThrows(RuntimeException.class, () -> SchemaMaker.generateOptimisticLocking("tenant", null, "table"));
  }

  @Test
  public void testCreateCreate() throws IOException, TemplateException {
    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.CREATE,
        "mod-foo-18.2.3", null, "templates/db_scripts/schemaWithAudit.json");
    String result = schemaMaker.generateCreate();
    assertThat(result, containsString("CREATE SCHEMA harvard_circ"));
    assertThat(result, containsString("rmb_job"));
    assertThat(result, not(containsString("CREATE INDEX IF NOT EXISTS audit_")));
  }

  @Test
  public void testCreateUpdate() throws IOException, TemplateException {
    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.UPDATE,
        "mod-foo-18.2.3", "mod-foo-18.2.4", "templates/db_scripts/schemaWithAudit.json");
    String result = schemaMaker.generateCreate();
    assertThat(result, not(containsString("CREATE SCHEMA harvard_circ")));
    assertThat(result, containsString("rmb_job"));
    assertThat(result, not(containsString("CREATE INDEX IF NOT EXISTS audit_")));
  }

  @Test
  public void testCreateIndexesOnly() throws IOException, TemplateException {
    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.CREATE,
        "mod-foo-18.2.3", null, "templates/db_scripts/schemaWithAudit.json");
    String result = schemaMaker.generateIndexesOnly();
    assertThat(result, not(containsString("CREATE SCHEMA harvard_circ")));
    assertThat(result, containsString("CREATE INDEX IF NOT EXISTS audit_"));
  }

  @Test
  public void canCreateAuditedTable() throws IOException, TemplateException {
    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.UPDATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4", "templates/db_scripts/schemaWithAudit.json");
    String result = schemaMaker.generateSchemas();

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
    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.UPDATE,
        "mod-foo-18.2.3", "mod-foo-18.2.4", "templates/db_scripts/compoundIndex.json");

    String result = schemaMaker.getSchema().getTables().get(0).getFullTextIndex().get(0).getFinalSqlExpression("tablea");
    assertThat(result,containsString("concat_space_sql(tablea.jsonb->>'field1' , tablea.jsonb->>'field2')"));
    result = schemaMaker.getSchema().getTables().get(0).getGinIndex().get(0).getFinalSqlExpression("tablea");
    assertThat(result,containsString("concat_space_sql(tablea.jsonb->>'firstName' , tablea.jsonb->>'lastName')"));
  }

  @Test
  public void canCreateIndexPath2() throws IOException, TemplateException {
    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.UPDATE,
        "mod-foo-18.2.3", "mod-foo-18.2.4", "templates/db_scripts/compoundIndex.json");
    String result = schemaMaker.getSchema().getTables().get(1).getGinIndex().get(0).getFinalSqlExpression("tableb");
    assertThat(result,containsString("lower(concat_space_sql(jsonb->>'city', jsonb->>'state'))"));
    result = schemaMaker.getSchema().getTables().get(1).getFullTextIndex().get(0).getFinalSqlExpression("tableb");
    assertThat(result,containsString("lower(concat_space_sql(jsonb->>'field1', jsonb->>'field2'))"));
  }

  @Test
  public void canCreateCompoundIndex() throws IOException, TemplateException {
    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.UPDATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4", "templates/db_scripts/compoundIndex.json");
    String result = schemaMaker.generateSchemas();

    assertThat(result, containsString("CREATE INDEX IF NOT EXISTS tablea_ftfield_idx_ft"));
    assertThat(result, containsString("concat_space_sql(tablea.jsonb->>'firstName' , tablea.jsonb->>'lastName')"));

    assertThat(result,containsString("((lower(concat_space_sql(concat_array_object_values(tablea.jsonb->'user','firstName') , concat_array_object_values(tablea.jsonb->'user','lastName')))) public.gin_trgm_ops)"));
    assertThat(result,containsString("((lower(concat_space_sql(concat_array_object_values(tablea.jsonb->'user'->'info','firstName') , concat_array_object_values(tablea.jsonb->'user'->'info','lastName')))) public.gin_trgm_ops)"));
    assertThat(result,containsString("( get_tsvector(concat_space_sql(concat_array_object_values(tablea.jsonb->'field1','firstName') , concat_array_object_values(tablea.jsonb->'field2','lastName'))) )"));
    assertThat(result,containsString("( get_tsvector(concat_space_sql(concat_array_object_values(tablea.jsonb->'field1'->'info','firstName') , concat_array_object_values(tablea.jsonb->'field2'->'info','lastName'))) )"));
  }

  @Test
  public void canCreateSQLExpressionIndex() throws IOException, TemplateException {
    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.UPDATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4", "templates/db_scripts/compoundIndex.json");
    String result = schemaMaker.generateSchemas();

    assertThat(result, containsString("lower(concat_space_sql(jsonb->>'field1', jsonb->>'field2'))"));
  }

  @Test
  public void failsWhenGenerateID() throws Exception {
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-0.2.1-SNAPSHOT.2", "mod-foo-18.2.1-SNAPSHOT.2");
    assertThat(assertThrows(UnrecognizedPropertyException.class, () -> {
      schemaMaker.setSchema(schema("templates/db_scripts/schemaGenerateId.json"));
    }).getMessage(), containsString("Unrecognized field \"generateId\""));
  }

  @Test
  public void failsWhenPkColumnName() throws Exception {
    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-0.2.1-SNAPSHOT.2", "mod-foo-18.2.1-SNAPSHOT.2");
    assertThat(assertThrows(UnrecognizedPropertyException.class, () -> {
      schemaMaker.setSchema(schema("templates/db_scripts/schemaPkColumnName.json"));
    }).getMessage(), containsString("Unrecognized field \"pkColumnName\""));
  }

  @Test
  public void pkColumnName() {
    assertThat(new Table().getPkColumnName(), is("id"));
    assertThat(new View().getPkColumnName(), is("id"));
  }

  @Test
  public void failsWhenPopulateJsonWithId() throws IOException, TemplateException {

    SchemaMaker schemaMaker = new SchemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-0.2.1-SNAPSHOT.2", "mod-foo-18.2.1-SNAPSHOT.2");
    IOException e = assertThrows(IOException.class, () -> {
      schemaMaker.setSchema(schema("templates/db_scripts/schemaPopulateJsonWithId.json"));
    });
    assertThat(tidy(e.getMessage()), containsString("Unrecognized field \"populateJsonWithId\""));
  }

  @Test
  public void lowerUnaccentIndex() throws IOException, TemplateException {

    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-0.2.1-SNAPSHOT.2", "mod-foo-18.2.1-SNAPSHOT.2",
      "templates/db_scripts/caseinsensitive.json");
    assertThat(tidy(schemaMaker.generateSchemas()), containsString(
        "CREATE INDEX IF NOT EXISTS item_title_idx ON harvard_circ.item ' "
        + "|| $rmb$(left(lower(f_unaccent(jsonb->>'title')),600))$rmb$)"));
  }

  @Test
  public void scriptExistsAndDoesntUpgrade() throws IOException, TemplateException {

    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.UPDATE,
      "mod-foo-18.2.1-SNAPSHOT.9", "mod-foo-18.2.3", "templates/db_scripts/scriptexists.json");

    assertThat(tidy(schemaMaker.generateSchemas()), containsString(
        "select * from start;"));

    assertThat("generated schema contains 'select * from end;' but it shouldn't",
      tidy(schemaMaker.generateSchemas()), not(containsString("select * from end;")));
  }

  @Test
  public void createBothScriptsPresent() throws IOException, TemplateException {

    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-18.2.1-SNAPSHOT.9", "mod-foo-18.2.3", "templates/db_scripts/scriptexists.json");

    assertThat(tidy(schemaMaker.generateSchemas()), containsString(
        "select * from start;"));

    assertThat(tidy(schemaMaker.generateSchemas()), containsString(
        "select * from end;"));
  }

  @Test
  public void createBothScriptsPresent2() throws IOException, TemplateException {

    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4", "templates/db_scripts/scriptexists.json");

    assertThat(tidy(schemaMaker.generateSchemas()), containsString(
        "select * from start;"));

    assertThat(tidy(schemaMaker.generateSchemas()), containsString(
        "select * from end;"));
  }

  @Test
  public void createScriptFromFilePresent() throws IOException, TemplateException {

    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4", "templates/db_scripts/scriptWithSnippetPath.json");

    assertThat(tidy(schemaMaker.generateSchemas()), containsString(
      "select * from file_start;"));
  }

  @Test
  public void createScriptFromFileAndSnippetPresent() throws IOException, TemplateException {

    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.CREATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4", "templates/db_scripts/scriptWithSnippetPathAndSnippet.json");

    assertThat(tidy(schemaMaker.generateSchemas()), containsString(
      "select * from start;"));

    assertThat(tidy(schemaMaker.generateSchemas()), containsString(
      "select * from file_start;"));
  }

  @Test
  public void deleteSchema() throws IOException, TemplateException {

    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.UPDATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4", "templates/db_scripts/scriptexists.json");

    assertThat(tidy(schemaMaker.generatePurge()), allOf(
        containsString("DROP SCHEMA "), containsString("DROP ROLE ")));
  }

  @Test
  public void createNeitherScripts() throws IOException, TemplateException {

    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.UPDATE,
      "mod-foo-18.2.3", "mod-foo-18.2.4", "templates/db_scripts/scriptexists.json");

    assertThat("generated schema contains 'select * from start;' but it should not",
      tidy(schemaMaker.generateSchemas()), not(containsString("select * from start;")));

    assertThat("generated schema contains 'select * from end;' but it should not",
      tidy(schemaMaker.generateSchemas()), not(containsString("select * from end;")));
  }

  @Test
  public void createBothScriptsPresentNoVersionsPassedIn() throws IOException, TemplateException {

    //TODO , once validation of versions is added this should change

    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.CREATE,
      "18.2.0", "18.2.3", "templates/db_scripts/scriptexists.json");

    assertThat(tidy(schemaMaker.generateSchemas()), containsString(
        "select * from start;"));

    assertThat(tidy(schemaMaker.generateSchemas()), containsString(
        "select * from end;"));
  }

  @Test
  public void badVersions() throws IOException, TemplateException {

    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.CREATE,
      null, null, "templates/db_scripts/scriptexists.json");

    assertThat(tidy(schemaMaker.generateSchemas()), containsString(
        "select * from start;"));

    assertThat(tidy(schemaMaker.generateSchemas()), containsString(
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

    SchemaMaker schemaMaker = schemaMaker("harvard", "circ", TenantOperation.CREATE,
      null, null, "templates/db_scripts/test_indexes.json");

    String ddl = tidy(schemaMaker.generateSchemas());

    // by default all indexes are wrapped with lower/f_unaccent
    // except full text which only obeys f_unaccent
    assertThat(ddl, containsString("(left(lower(f_unaccent(jsonb->>'title')),600))"));  // index
    assertThat(ddl, containsString("(lower(f_unaccent(jsonb->>'name')))"));             // unique index
    assertThat(ddl, containsString("((lower(f_unaccent(jsonb->>'type')))text_pattern_ops)"));
    assertThat(ddl, containsString("GIN ' || $rmb$((lower(f_unaccent(jsonb->>'title')))public.gin_trgm_ops)"));
    assertThat(ddl, containsString("GIN ' || $rmb$(get_tsvector(f_unaccent(jsonb->>'title')))"));
    assertThat(ddl, containsString("GIN ' || $rmb$(get_tsvector((jsonb->>'author')))"));
  }

  @Test
  public void deleteOldTables() throws Exception {
    SchemaMaker schemaMaker = schemaMaker("myTenant", "myModule", TenantOperation.UPDATE,
        "1.0.0", "2.0.0", "templates/db_scripts/schema.json");
    schemaMaker.setPreviousSchema(schema("templates/db_scripts/indexUpgrade.json"));
    String ddl = schemaMaker.generateSchemas();
    assertThat(ddl, containsString("DROP TABLE IF EXISTS myTenant_myModule.tablea CASCADE;"));
    assertThat(ddl, containsString("DROP TABLE IF EXISTS myTenant_myModule.tableb CASCADE;"));
    assertThat(ddl, containsString("DROP TABLE IF EXISTS myTenant_myModule.tablec CASCADE;"));
    assertThat(ddl, containsString("DROP TABLE IF EXISTS myTenant_myModule.tabled CASCADE;"));
    assertThat(ddl, containsString("DROP COLUMN IF EXISTS refField CASCADE;"));  // foreign key
    assertThat(ddl, not(containsString("ADD COLUMN IF NOT EXISTS refField")));
    assertThat(ddl, containsString("DROP FUNCTION IF EXISTS myTenant_myModule.update_test_tenantapi_references()"));
    assertThat(ddl, not(containsString("CREATE OR REPLACE FUNCTION myTenant_myModule.update_test_tenantapi_references()")));
  }

  @Test
  public void createForeignKey() throws Exception {
    SchemaMaker schemaMaker = schemaMaker("myTenant", "myModule", TenantOperation.UPDATE,
        "1.0.0", "2.0.0", "templates/db_scripts/indexUpgrade.json");
    schemaMaker.setPreviousSchema(schema("templates/db_scripts/schema.json"));
    String ddl = schemaMaker.generateSchemas();
    assertThat(ddl, containsString("ADD COLUMN IF NOT EXISTS refField"));
    assertThat(ddl, not(containsString("DROP COLUMN IF EXISTS refField")));
    assertThat(ddl, containsString("CREATE OR REPLACE FUNCTION myTenant_myModule.update_test_tenantapi_references()"));
    assertThat(ddl, not(containsString("DROP FUNCTION IF EXISTS myTenant_myModule.update_test_tenantapi_references()")));
  }

  @Test
  public void changeForeignKey1to2() throws Exception {
    SchemaMaker schemaMaker = schemaMaker("myTenant", "myModule", TenantOperation.UPDATE,
        "1.0.0", "2.0.0", "templates/db_scripts/foreignKey2.json");
    schemaMaker.setPreviousSchema(schema("templates/db_scripts/foreignKey1.json"));
    String ddl = schemaMaker.generateSchemas();
    // a, f -> b, c, d, e, f
    assertThat(ddl, containsString("DROP COLUMN IF EXISTS ref_a"));
    assertThat(ddl, containsString("ADD COLUMN IF NOT EXISTS ref_b"));
    assertThat(ddl, containsString("ADD COLUMN IF NOT EXISTS ref_c"));
    assertThat(ddl, containsString("ADD COLUMN IF NOT EXISTS ref_d"));
    assertThat(ddl, containsString("ADD COLUMN IF NOT EXISTS ref_e"));
    assertThat(ddl, containsString("ADD COLUMN IF NOT EXISTS ref_f"));
    assertThat(ddl, not(containsString("ADD COLUMN IF NOT EXISTS ref_a")));
    assertThat(ddl, not(containsString("DROP COLUMN IF EXISTS ref_b")));
    assertThat(ddl, not(containsString("DROP COLUMN IF EXISTS ref_c")));
    assertThat(ddl, not(containsString("DROP COLUMN IF EXISTS ref_d")));
    assertThat(ddl, not(containsString("DROP COLUMN IF EXISTS ref_e")));
    assertThat(ddl, not(containsString("DROP COLUMN IF EXISTS ref_f")));
  }

  @Test
  public void changeForeignKey2to1() throws Exception {
    SchemaMaker schemaMaker = schemaMaker("myTenant", "myModule", TenantOperation.UPDATE,
        "1.0.0", "2.0.0", "templates/db_scripts/foreignKey1.json");
    schemaMaker.setPreviousSchema(schema("templates/db_scripts/foreignKey2.json"));
    String ddl = schemaMaker.generateSchemas();
    // b, c, d, e, f -> a, f
    assertThat(ddl, containsString("ADD COLUMN IF NOT EXISTS ref_a"));
    assertThat(ddl, containsString("DROP COLUMN IF EXISTS ref_b"));
    assertThat(ddl, containsString("DROP COLUMN IF EXISTS ref_c"));
    assertThat(ddl, containsString("DROP COLUMN IF EXISTS ref_d"));
    assertThat(ddl, containsString("DROP COLUMN IF EXISTS ref_e"));
    assertThat(ddl, containsString("ADD COLUMN IF NOT EXISTS ref_f"));
    assertThat(ddl, not(containsString("DROP COLUMN IF EXISTS ref_a")));
    assertThat(ddl, not(containsString("ADD COLUMN IF NOT EXISTS ref_b")));
    assertThat(ddl, not(containsString("ADD COLUMN IF NOT EXISTS ref_c")));
    assertThat(ddl, not(containsString("ADD COLUMN IF NOT EXISTS ref_d")));
    assertThat(ddl, not(containsString("ADD COLUMN IF NOT EXISTS ref_e")));
    assertThat(ddl, not(containsString("DROP COLUMN IF EXISTS ref_f")));
  }

  @ParameterizedTest
  @CsvSource({
    "a,  , a,  , true",
    " , b,  , b, true",
    "a, b, a, b, true",
    "a, b, c, d, false",
    "a, a, b, b, false",
    "a, b, b, a, false",
    " ,  ,  ,  , false",
  })
  public void sameForeignKeyNullsString(String fieldNameA, String fieldPathA,
                                        String fieldNameB, String fieldPathB, boolean expected) {
    ForeignKeys a = new ForeignKeys();
    if (fieldNameA != null) {
      a.setFieldName(fieldNameA);
    }
    if (fieldPathA != null) {
      a.setFieldPath(fieldPathA);
    }
    ForeignKeys b = new ForeignKeys();
    if (fieldNameB != null) {
      b.setFieldName(fieldNameB);
    }
    if (fieldPathB != null) {
      b.setFieldPath(fieldPathB);
    }
    assertThat(SchemaMaker.sameForeignKey(a, b), is(expected));
  }

  @Test
  public void optimisticLocking() throws Exception {
    String tenant = "olTenant";
    String module = "olModule";
    SchemaMaker schemaMaker = schemaMaker(tenant, module, TenantOperation.UPDATE,
        "1.0.0", "2.0.0", "templates/db_scripts/schemaWithOptimisticLocking.json");
    String ddl = schemaMaker.generateSchemas();
    // trigger will be created for tab_ol_log, tab_ol_fail
    Arrays.asList("tab_ol_log", "tab_ol_fail").forEach(tab -> {
      assertThat(ddl, containsString(
          String.format("CREATE OR REPLACE FUNCTION %s_%s.%s_set_ol_version()",
              tenant, module, tab)));
      assertThat(ddl, containsString(
          String.format("DROP TRIGGER IF EXISTS set_%s_ol_version_trigger", tab)));
      assertThat(ddl, containsString(
          String.format("CREATE TRIGGER set_%s_ol_version_trigger", tab)));
    });
    // trigger will not be created for for tabl_ol_off and tab_ol_none
    assertThat(ddl, not(containsString(
        String.format("CREATE OR REPLACE FUNCTION %s_%s.%s_set_ol_version()",
            tenant, module, "tab_ol_none"))));
    assertThat(ddl, not(containsString(
        String.format("CREATE TRIGGER set_%s_ol_version_trigger", "D"))));
    assertThat(ddl, containsString(
        String.format("DROP TRIGGER IF EXISTS set_%s_ol_version_trigger", "tab_ol_none")));
    assertThat(ddl, containsString(
        String.format("DROP FUNCTION IF EXISTS %s_%s.%s_set_ol_version()",
            tenant, module, "tab_ol_none")));
  }

}
