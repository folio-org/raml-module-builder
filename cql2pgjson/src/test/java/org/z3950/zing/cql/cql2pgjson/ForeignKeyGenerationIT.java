package org.z3950.zing.cql.cql2pgjson;

import static org.hamcrest.Matchers.*;

import org.junit.runner.RunWith;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import junitparams.JUnitParamsRunner;


@RunWith(JUnitParamsRunner.class)
public class ForeignKeyGenerationIT extends DatabaseTestBase {
  private CQL2PgJSON cql2pgJson;

  @BeforeClass
  public static void runOnceBeforeClass() throws Exception {
    setupDatabase();
    runSqlFile("joinExample.sql");

  }

  @AfterClass
  public static void runOnceAfterClass() {
    closeDatabase();
  }

  @Before
  public void before() throws Exception {
    cql2pgJson = new CQL2PgJSON("tablea.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/joinExample_schema.json");
  }

  @Test
  public void foreignKeySearch0() throws Exception {
    String sql = cql2pgJson.toSql("tableb.tableb_data == x0").toString();
    assertThat(firstColumn("select jsonb->>'name' from tablea " + sql), is(empty()));
  }

  @Test
  public void foreignKeySearch1() throws Exception {
    String sql = cql2pgJson.toSql("tableb.tableb_data == x1").toString();
    assertThat(firstColumn("select jsonb->>'name' from tablea " + sql), containsInAnyOrder("test1"));
  }

  @Test
  public void foreignKeyFilter1() throws Exception {
    String sql = cql2pgJson.toSql("id == tableb.tableaId").toString();
    // return a single tablea record even if two tableb records match
    assertTrue(firstColumn("select jsonb->>'name' from tablea " + sql).size() == 2);
  }
}
