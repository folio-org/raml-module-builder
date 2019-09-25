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
    closeDatabase();
  }

  @Before
  public void runSetup() throws Exception {
    cql2pgJsonTablea = new CQL2PgJSON("tablea.jsonb");
    cql2pgJsonTablea.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    cql2pgJsonTableb = new CQL2PgJSON("tableb.jsonb");
    cql2pgJsonTableb.setDbSchemaPath("templates/db_scripts/compoundIndex.json");

  }
  private List<String> cqla(String cql) {
    try {
      String sql = cql2pgJsonTablea.toSql(cql).toString();
      return firstColumn("select jsonb->>'firstName' from tablea " + sql);
    } catch (QueryValidationException e) {
      throw new RuntimeException(e);
    }
  }
  private List<String> cqlb(String cql) {
    try {
      String sql = cql2pgJsonTableb.toSql(cql).toString();
      return firstColumn("select jsonb->>'field1' from tableb " + sql);
    } catch (QueryValidationException e) {
      throw new RuntimeException(e);
    }
  }
  @Test
  public void compoundIndexTestMultiFieldNames() throws Exception {
    assertThat(cqla("fullname == Tom Jones"), containsInAnyOrder("Tom"));
  }

  @Test
  public void compoundIndexTestMultiFieldNames2() throws Exception {
    assertThat( cqlb("address == Boston MA"),  containsInAnyOrder("first0"));
  }
  @Test
  public void compoundIndexTestMultiFieldNames3() throws Exception {
    assertThat(cqla("ftfield = first0 last0"), containsInAnyOrder("Mike"));
  }

  @Test
  public void compoundIndexTestMultiFieldNames4() throws Exception {
    assertThat( cqlb("ftfield = first1 last1"),  containsInAnyOrder("first1"));
  }
}
