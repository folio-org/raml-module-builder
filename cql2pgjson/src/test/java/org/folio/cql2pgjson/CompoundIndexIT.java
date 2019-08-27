package org.folio.cql2pgjson;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.folio.cql2pgjson.exception.QueryValidationException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CompoundIndexIT extends DatabaseTestBase {
  private CQL2PgJSON cql2pgJsonTablea;
  private CQL2PgJSON cql2pgJsonTableb;

  @BeforeClass
  static public void setUpDatabase()  {
    setupDatabase();
    runSqlFile("compoundIndex.sql");
  }

  @AfterClass
  static public void destroyDatabase() {

  }

  @Before
  public void runSetup() throws Exception {
    cql2pgJsonTablea = new CQL2PgJSON("tablea.jsonb");
    cql2pgJsonTablea.setDbSchemaPath("templates/db_scripts/compoundIndexTest.json");
    cql2pgJsonTableb = new CQL2PgJSON("tableb.jsonb");
    cql2pgJsonTableb.setDbSchemaPath("templates/db_scripts/compoundIndexTest.json");

  }
  private List<String> cqla(String cql) {
    try {
      String sql = cql2pgJsonTablea.toSql(cql).toString();
      return firstColumn("select jsonb->>'firstname' from tablea " + sql);
    } catch (QueryValidationException e) {
      throw new RuntimeException(e);
    }
  }
  private List<String> cqlb(String cql) {
    try {
      String sql = cql2pgJsonTableb.toSql(cql).toString();
      return firstColumn("select jsonb->>'city' from tableb " + sql);
    } catch (QueryValidationException e) {
      throw new RuntimeException(e);
    }
  }
  @Test
  public void compoundIndexTestMultiFieldNames() throws Exception {
    assertThat(cqla("fullname == first1 last1"), containsInAnyOrder("first1"));
  }

  @Test
  public void compoundIndexTestMultiFieldNames2() throws Exception {
    assertThat( cqlb("ftfield = John Smith"),  containsInAnyOrder("first1"));
  }

}
