package org.z3950.zing.cql.cql2pgjson;

import static org.hamcrest.Matchers.*;

import org.folio.cql2pgjson.CQL2PgJSON;

import org.junit.runner.RunWith;

import static org.junit.Assert.assertThat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junitparams.JUnitParamsRunner;


@RunWith(JUnitParamsRunner.class)
public class ForeignKeyGenerationIT extends DatabaseTestBase {
  private CQL2PgJSON cql2pgJson;

  @BeforeClass
  public static void runOnceBeforeClass() throws Exception {
    setupDatabase();
    runSqlFile("foreignKey.sql");

  }

  @AfterClass
  public static void runOnceAfterClass() {
    closeDatabase();
  }

  @Before
  public void before() throws Exception {
    cql2pgJson = new CQL2PgJSON("tablea.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
  }

  @Test
  public void foreignKeySearch0() throws Exception {
    String sql = cql2pgJson.toSql("tableb.prefix == x0").toString();
    assertThat(firstColumn("select jsonb->>'name' from tablea " + sql), is(empty()));
  }

  @Test
  public void foreignKeySearch1() throws Exception {
    String sql = cql2pgJson.toSql("tableb.prefix == x1").toString();
    assertThat(firstColumn("select jsonb->>'name' from tablea " + sql), containsInAnyOrder("test1"));
  }

  @Test
  public void foreignKeySearch2() throws Exception {
    String sql = cql2pgJson.toSql("tableb.prefix == x2").toString();
    // two tableb records match, but they both reference the same tablea record, therefore "test2" should be
    // returned one time only
    assertThat(firstColumn("select jsonb->>'name' from tablea " + sql), containsInAnyOrder("test2"));
  }

  @Test
  public void foreignKeyFilter1() throws Exception {
    String sql = cql2pgJson.toSql("id == tableb.tableaId").toString();
    assertThat(firstColumn("select jsonb->>'name' from tablea " + sql), containsInAnyOrder("test1", "test2"));
  }
}
