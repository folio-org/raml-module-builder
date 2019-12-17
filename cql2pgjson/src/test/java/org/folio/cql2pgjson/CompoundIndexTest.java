package org.folio.cql2pgjson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

import org.junit.Test;

public class CompoundIndexTest {

  @Test
  public void multiFieldNamesNonUniqueIndex() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("keys == x").toString();
    String expected = "WHERE CASE WHEN length(lower(f_unaccent('x'))) <= 600 "
        + "THEN left(lower(f_unaccent(concat_space_sql(tablea.jsonb->>'key1' , tablea.jsonb->>'key2'))),600) LIKE lower(f_unaccent('x')) "
        + "ELSE left(lower(f_unaccent(concat_space_sql(tablea.jsonb->>'key1' , tablea.jsonb->>'key2'))),600)  LIKE  left(lower(f_unaccent('x')),600) "
        + "AND lower(f_unaccent(concat_space_sql(tablea.jsonb->>'key1' , tablea.jsonb->>'key2')))  LIKE  lower(f_unaccent('x')) END";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldNamesUniqueIndex() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("barcode == y").toString();
    String expected = "WHERE CASE WHEN length(lower(f_unaccent('y'))) <= 600 "
      + "THEN left(lower(f_unaccent(concat_space_sql(tablea.jsonb->>'department' , tablea.jsonb->>'staffnumber'))),600) LIKE lower(f_unaccent('y')) "
      + "ELSE left(lower(f_unaccent(concat_space_sql(tablea.jsonb->>'department' , tablea.jsonb->>'staffnumber'))),600)  LIKE  left(lower(f_unaccent('y')),600) "
      + "AND lower(f_unaccent(concat_space_sql(tablea.jsonb->>'department' , tablea.jsonb->>'staffnumber')))  LIKE  lower(f_unaccent('y')) END";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldNamesGIN() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("fullname == \"John Smith\"").toString();
    String expected = "WHERE CASE WHEN length(lower('John Smith')) <= 600 "
        + "THEN left(lower(concat_space_sql(tablea.jsonb->>'firstName' , tablea.jsonb->>'lastName')),600) LIKE lower('John Smith') "
        + "ELSE left(lower(concat_space_sql(tablea.jsonb->>'firstName' , tablea.jsonb->>'lastName')),600)  LIKE  left(lower('John Smith'),600) "
        + "AND lower(concat_space_sql(tablea.jsonb->>'firstName' , tablea.jsonb->>'lastName'))  LIKE  lower('John Smith') END";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldNamesFT() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfield = \"John Smith\"").toString();
    String expected = "WHERE to_tsvector('simple', lower(concat_space_sql(tablea.jsonb->>'field1' , tablea.jsonb->>'field2'))) @@ replace((to_tsquery('simple', ('''John''')) && to_tsquery('simple', ('''Smith''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void SQLExpressionGIN() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("address == \"Boston MA\"").toString();
    String expected =  "LIKE lower('Boston MA') ELSE left(lower(concat_space_sql(jsonb->>'city', jsonb->>'state')),600)  ";
    assertThat(sql, containsString(expected ));
  }

  @Test
  public void SQLExpressionFT() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfield = \"Boston MA\"").toString();
    String expected = "WHERE to_tsvector('simple', lower(concat_space_sql(jsonb->>'field1', jsonb->>'field2'))) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesSpacesFT() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablec");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tablecftindex = \"Boston MA\"").toString();
    String expected = "WHERE to_tsvector('simple', lower(concat_space_sql(tablec.jsonb->>'firstName' , tablec.jsonb->>'lastName'))) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesSpacesGIN() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablec");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tablecginindex == \"Boston MA\"").toString();
    String expected = "lower('Boston MA') ELSE left(lower(concat_space_sql(tablec.jsonb->>'firstName' , tablec.jsonb->>'lastName')),600)";
    assertThat(sql, containsString(expected ));
  }

  @Test
  public void multiFieldnamesMultipartFT() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tabled");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tabledftindex = \"Boston MA\"").toString();
    String expected = "WHERE to_tsvector('simple', lower(concat_space_sql(tabled.jsonb->'proxy'->'personal'->>'city' , tabled.jsonb->'proxy'->'personal'->>'state'))) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesFTStar() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfieldstar = \"Boston MA\"").toString();
    String expected = "WHERE to_tsvector('simple', lower(concat_space_sql(concat_array_object_values(tablea.jsonb->'field1','city') , concat_array_object_values(tablea.jsonb->'field2','state')))) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesFTDotStar() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfielddotstar = \"Boston MA\"").toString();
    String expected = "WHERE to_tsvector('simple', lower(concat_space_sql(concat_array_object_values(tablea.jsonb->'field3'->'info','city') , concat_array_object_values(tablea.jsonb->'field3'->'info','state')))) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesFTDotStarPlain() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfielddotstarplain = \"Boston MA\"").toString();
    String expected = "WHERE to_tsvector('simple', lower(concat_space_sql(concat_array_object(tablea.jsonb->'field3'->'info') , concat_array_object(tablea.jsonb->'field3'->'data')))) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesGINStar() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ginfieldstar == \"Boston MA\"").toString();
    String expected = "lower('Boston MA') ELSE left(lower(concat_space_sql(concat_array_object_values(tablea.jsonb->'field1','city') , concat_array_object_values(tablea.jsonb->'field2','state'))),600)";
    assertThat(sql,containsString(expected));
  }

  @Test
  public void multiFieldnamesGINDotStar() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ginfielddotstar == \"Boston MA\"").toString();
    String expected = "lower('Boston MA') ELSE left(lower(concat_space_sql(concat_array_object_values(tablea.jsonb->'field3'->'info','city') , concat_array_object_values(tablea.jsonb->'field3'->'info','state'))),600)";
    assertThat(sql,containsString(expected));
  }

  @Test
  public void multiFieldnamesGINDotStarPlain() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ginfielddotstarplain == \"Boston MA\"").toString();
    String expected = "lower('Boston MA') ELSE left(lower(concat_space_sql(concat_array_object(tablea.jsonb->'field3'->'info') , concat_array_object(tablea.jsonb->'field3'->'data'))),600)";
    assertThat(sql,containsString(expected));
  }
  @Test
  public void multiFieldnamesMultipartGIN() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tabled");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tabledginindex == \"Boston MA\"").toString();
    String expected = "lower('Boston MA') ELSE left(lower(concat_space_sql(tabled.jsonb->'proxy'->'personal'->>'city' , tabled.jsonb->'proxy'->'personal'->>'state')),600)  ";
    assertThat(sql,containsString(expected));
  }
}
