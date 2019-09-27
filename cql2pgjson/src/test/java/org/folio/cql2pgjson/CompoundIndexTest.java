package org.folio.cql2pgjson;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CompoundIndexTest {

  @Test
  public void compoundIndexMultiFieldNames() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("fullname == John Smith").toString();
    String expected = "WHERE lower(concat_space_sql(tablea.jsonb->>'firstName' , tablea.jsonb->>'lastName')) LIKE lower('John Smith')";
    assertEquals(expected, sql);
  }

  @Test
  public void compoundIndexMultiFieldNames2() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfield = John Smith").toString();
    String expected = "WHERE to_tsvector('simple', concat_space_sql(tablea.jsonb->>'field1' , tablea.jsonb->>'field2')) @@ replace((to_tsquery('simple', ('''John''')) && to_tsquery('simple', ('''Smith''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }

  @Test
  public void compoundIndexSQLExpression() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("address == Boston MA").toString();
    String expected = "WHERE lower(concat_space_sql(jsonb->>'city', jsonb->>'state')) LIKE lower('Boston MA')";
    assertEquals(expected, sql);
  }

  @Test
  public void compoundIndexSQLExpression2() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfield = Boston MA").toString();
    String expected = "WHERE to_tsvector('simple', lower(concat_space_sql(jsonb->>'field1', jsonb->>'field2'))) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }
  @Test
  public void compoundIndexMultiFieldnames3() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablec");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tablecftindex = Boston MA").toString();
    String expected = "WHERE to_tsvector('simple', concat_space_sql(tablec.jsonb->>'firstName' , tablec.jsonb->>'lastName')) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }
  @Test
  public void compoundIndexMultiFieldnames4() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablec");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tablecginindex == Boston MA").toString();
    String expected = "WHERE lower(concat_space_sql(tablec.jsonb->>'firstName' , tablec.jsonb->>'lastName')) LIKE lower('Boston MA')";
    assertEquals(expected, sql);
  }
  @Test
  public void compoundIndexMultiFieldnames5() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tabled");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tabledftindex = Boston MA").toString();
    String expected = "WHERE to_tsvector('simple', concat_space_sql(tabled.jsonb->'proxy'->'personal'->>'firstName' , tabled.jsonb->'proxy'->'personal'->>'lastName')) @@ replace((to_tsquery('simple', ('''Boston''')) && to_tsquery('simple', ('''MA''')))::text, '&', '<->')::tsquery";
    assertEquals(expected, sql);
  }
  @Test
  public void compoundIndexMultiFieldnames6() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tabled");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tabledginindex == Boston MA").toString();
    String expected = "WHERE lower(concat_space_sql(tabled.jsonb->'proxy'->'personal'->>'firstName' , tabled.jsonb->'proxy'->'personal'->>'lastName')) LIKE lower('Boston MA')";
    assertEquals(expected, sql);
  }
}
