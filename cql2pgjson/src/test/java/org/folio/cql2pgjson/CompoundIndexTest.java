package org.folio.cql2pgjson;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CompoundIndexTest {

  @Test
  public void multiFieldNamesGIN() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("fullname == \"John Smith\"").toString();
    String expected = "WHERE CASE WHEN length('John Smith') <= 600 THEN left(lower(concat_space_sql(tablea.jsonb->>'firstName' , tablea.jsonb->>'lastName')),600) LIKE left(lower('John Smith'),600)  ELSE left(tablea->>'fullname',600)  LIKE left('John Smith',600)  AND tablea->>'fullname'  LIKE  'John Smith' END";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldNamesFT() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfield = \"John Smith\"").toString();
    String expected = "WHERE to_tsvector('simple', concat_space_sql(tablea.jsonb->>'field1' , tablea.jsonb->>'field2')) @@ replace((to_tsquery('simple', ('''John''')) && to_tsquery('simple', ('''Smith''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void SQLExpressionGIN() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("address == \"Boston MA\"").toString();
    String expected = "WHERE CASE WHEN length('Boston MA') <= 600 THEN left(lower(concat_space_sql(jsonb->>'city', jsonb->>'state')),600) LIKE left(lower('Boston MA'),600)  ELSE left(tableb->>'address',600)  LIKE left('Boston MA',600)  AND tableb->>'address'  LIKE  'Boston MA' END";
    assertEquals(expected, sql);
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
    String expected = "WHERE to_tsvector('simple', concat_space_sql(tablec.jsonb->>'firstName' , tablec.jsonb->>'lastName')) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesSpacesGIN() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablec");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tablecginindex == \"Boston MA\"").toString();
    String expected = "WHERE CASE WHEN length('Boston MA') <= 600 THEN left(lower(concat_space_sql(tablec.jsonb->>'firstName' , tablec.jsonb->>'lastName')),600) LIKE left(lower('Boston MA'),600)  ELSE left(tablec->>'tablecginindex',600)  LIKE left('Boston MA',600)  AND tablec->>'tablecginindex'  LIKE  'Boston MA' END";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesMultipartFT() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tabled");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tabledftindex = \"Boston MA\"").toString();
    String expected = "WHERE to_tsvector('simple', concat_space_sql(tabled.jsonb->'proxy'->'personal'->>'city' , tabled.jsonb->'proxy'->'personal'->>'state')) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesFTStar() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfieldstar = \"Boston MA\"").toString();
    String expected = "WHERE to_tsvector('simple', concat_space_sql(concat_array_object_values(tablea.jsonb->'field1','city') , concat_array_object_values(tablea.jsonb->'field2','state'))) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesFTDotStar() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfielddotstar = \"Boston MA\"").toString();
    String expected = "WHERE to_tsvector('simple', concat_space_sql(concat_array_object_values(tablea.jsonb->'field3'->'info','city') , concat_array_object_values(tablea.jsonb->'field3'->'info','state'))) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesFTDotStarPlain() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfielddotstarplain = \"Boston MA\"").toString();
    String expected = "WHERE to_tsvector('simple', concat_space_sql(concat_array_object(tablea.jsonb->'field3'->'info') , concat_array_object(tablea.jsonb->'field3'->'data'))) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesGINStar() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ginfieldstar == \"Boston MA\"").toString();
    String expected = "WHERE CASE WHEN length('Boston MA') <= 600 THEN left(lower(concat_space_sql(concat_array_object_values(tablea.jsonb->'field1','city') , concat_array_object_values(tablea.jsonb->'field2','state'))),600) LIKE left(lower('Boston MA'),600)  ELSE left(tablea->>'ginfieldstar',600)  LIKE left('Boston MA',600)  AND tablea->>'ginfieldstar'  LIKE  'Boston MA' END";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesGINDotStar() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ginfielddotstar == \"Boston MA\"").toString();
    String expected = "WHERE CASE WHEN length('Boston MA') <= 600 THEN left(lower(concat_space_sql(concat_array_object_values(tablea.jsonb->'field3'->'info','city') , concat_array_object_values(tablea.jsonb->'field3'->'info','state'))),600) LIKE left(lower('Boston MA'),600)  ELSE left(tablea->>'ginfielddotstar',600)  LIKE left('Boston MA',600)  AND tablea->>'ginfielddotstar'  LIKE  'Boston MA' END";
    assertEquals(expected, sql);
  }

  @Test
  public void multiFieldnamesGINDotStarPlain() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ginfielddotstarplain == \"Boston MA\"").toString();
    String expected = "WHERE CASE WHEN length('Boston MA') <= 600 THEN left(lower(concat_space_sql(concat_array_object(tablea.jsonb->'field3'->'info') , concat_array_object(tablea.jsonb->'field3'->'data'))),600) LIKE left(lower('Boston MA'),600)  ELSE left(tablea->>'ginfielddotstarplain',600)  LIKE left('Boston MA',600)  AND tablea->>'ginfielddotstarplain'  LIKE  'Boston MA' END";
    assertEquals(expected, sql);
  }
  @Test
  public void multiFieldnamesMultipartGIN() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tabled");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tabledginindex == \"Boston MA\"").toString();
    String expected = "WHERE CASE WHEN length('Boston MA') <= 600 THEN left(lower(concat_space_sql(tabled.jsonb->'proxy'->'personal'->>'city' , tabled.jsonb->'proxy'->'personal'->>'state')),600) LIKE left(lower('Boston MA'),600)  ELSE left(tabled->>'tabledginindex',600)  LIKE left('Boston MA',600)  AND tabled->>'tabledginindex'  LIKE  'Boston MA' END";
    assertEquals(expected, sql);
  }
}
