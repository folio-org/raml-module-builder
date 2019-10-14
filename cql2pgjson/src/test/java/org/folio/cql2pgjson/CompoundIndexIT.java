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
  private CQL2PgJSON cql2pgJsonTablec;
  private CQL2PgJSON cql2pgJsonTabled;
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
    cql2pgJsonTablec = new CQL2PgJSON("tablec.jsonb");
    cql2pgJsonTablec.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    cql2pgJsonTabled = new CQL2PgJSON("tabled.jsonb");
    cql2pgJsonTabled.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
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

  private List<String> cqlc(String cql) {
    try {
      String sql = cql2pgJsonTablec.toSql(cql).toString();
      return firstColumn("select jsonb->>'user' from tablec " + sql);
    } catch (QueryValidationException e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> cqld(String cql) {
    try {
      String sql = cql2pgJsonTabled.toSql(cql).toString();
      return firstColumn("select jsonb->>'user' from tabled " + sql);
    } catch (QueryValidationException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void multiFieldNamesGIN() throws Exception {
    assertThat(cqla("fullname == \"Tom Jones\""), containsInAnyOrder("Tom"));
  }

  @Test
  public void sqlExpressionGIN() throws Exception {
    assertThat( cqlb("address == \"Boston MA\""),  containsInAnyOrder("first0"));
  }
  @Test
  public void multiFieldNamesFT() throws Exception {
    assertThat(cqla("ftfield = \"first0 last0\""), containsInAnyOrder("Mike"));
  }

  @Test
  public void sqlExpressionFT() throws Exception {
    assertThat( cqlb("ftfield = \"first1 last1\""),  containsInAnyOrder("first1"));
  }

  @Test
  public void multiFieldNamesSpacesGIN() throws Exception {
    assertThat( cqlc("tablecginindex == \"Mike Smith\""),  containsInAnyOrder("12"));
  }

  @Test
  public void multiFieldNamesSpacesFT() throws Exception {
    assertThat( cqlc("tablecftindex = \"Tom Jones\""),  containsInAnyOrder("23"));
  }

  @Test
  public void multiFieldNamesMultipartindexpathGIN() throws Exception {
    assertThat( cqld("tabledginindex == \"Austin TX\""),  containsInAnyOrder("Charles"));
  }

  @Test
  public void multiFieldNamesMultipartindexpathFT() throws Exception {
    assertThat( cqld("tabledftindex = \"Chicago IL\""),  containsInAnyOrder("Bob"));
  }

  @Test
  public void multiFieldNamesDotStarGIN() throws Exception {
    assertThat( cqla("ginfielddotstar == \"Boston\""),  containsInAnyOrder("Mike","Tom"));
  }

  @Test
  public void multiFieldNamesDotStarFT() throws Exception {
    assertThat( cqla("ftfielddotstar = \"PA\""),  containsInAnyOrder("Mary", "Mike"));
  }
}
