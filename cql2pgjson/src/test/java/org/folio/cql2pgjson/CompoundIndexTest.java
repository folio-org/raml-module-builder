package org.folio.cql2pgjson;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CompoundIndexTest {

  @Test
  public void compoundIndexTest() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndexTest.json");
    String sql = cql2pgJson.toSql("fullname == 'John Smith'").toString();
    String expected = "";
    assertEquals(expected, sql);
  }
}
