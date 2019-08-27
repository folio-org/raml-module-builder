package org.folio.cql2pgjson;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CompoundIndexTest {

  @Test
  public void compoundIndexTestMultiFieldNames() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndexTest.json");
    String sql = cql2pgJson.toSql("fullname == John Smith").toString();
    String expected = "";
    assertEquals(expected, sql);
  }

  @Test
  public void compoundIndexTestMultiFieldNames2() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndexTest.json");
    String sql = cql2pgJson.toSql("ftfield = John Smith").toString();
    String expected = "";
    assertEquals(expected, sql);
  }

  @Test
  public void compoundIndexTestSQLExpression() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndexTest.json");
    String sql = cql2pgJson.toSql("address == Boston MA").toString();
    String expected = "";
    assertEquals(expected, sql);
  }

  @Test
  public void compoundIndexTestSQLExpression2() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndexTest.json");
    String sql = cql2pgJson.toSql("ftfield = Boston MA").toString();
    String expected = "";
    assertEquals(expected, sql);
  }
}
