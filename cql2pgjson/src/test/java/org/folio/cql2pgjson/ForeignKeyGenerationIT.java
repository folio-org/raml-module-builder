package org.folio.cql2pgjson;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;

import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;

@RunWith(JUnitParamsRunner.class)
public class ForeignKeyGenerationIT extends DatabaseTestBase {
  @BeforeClass
  public static void runOnceBeforeClass() {
    setupDatabase();
    runSqlFile("foreignKey.sql");
  }

  @AfterClass
  public static void runOnceAfterClass() {
    closeDatabase();
  }

  /** Search tablea and return name of each tablea result row */
  private List<String> cqla(String cql) {
    try {
      CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.jsonb");
      cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
      String sql = cql2pgJson.toSql(cql).toString();
      return firstColumn("select jsonb->>'name' from tablea " + sql);
    } catch (QueryValidationException | FieldException e) {
      throw new RuntimeException(e);
    }
  }

  /** Search tableb and return prefix of each tableb result row */
  private List<String> cqlb(String cql) {
    try {
      CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb.jsonb");
      cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
      String sql = cql2pgJson.toSql(cql).toString();
      return firstColumn("select jsonb->>'prefix' from tableb " + sql);
    } catch (QueryValidationException | FieldException e) {
      throw new RuntimeException(e);
    }
  }

  /** Search tablec and return cindex of each tablec result row */
  private List<String> cqlc(String cql) {
    try {
      CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablec.jsonb");
      cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
      String sql = cql2pgJson.toSql(cql).toString();
      return firstColumn("select jsonb->>'cindex' from tablec " + sql);
    } catch (QueryValidationException | FieldException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void foreignKeySearch0() {
    assertThat(cqla("tableb.prefix == x0"), is(empty()));
  }

  @Test
  public void foreignKeySearchParentChild() {
    assertThat(cqlb("tablea.name == test1"), containsInAnyOrder("x1"));
  }

  @Test
  public void foreignKeySearchMulti() {
    assertThat(cqla("tablec.cindex == z1"), containsInAnyOrder("test1"));
  }

  @Test
  public void foreignKeySearchMulti2() {
    assertThat(cqla("tablec.cindex == z2"), containsInAnyOrder("test2"));
  }

  @Test
  public void foreignKeySearchMultiWild() {
    assertThat(cqla("tablec.cindex == *"), containsInAnyOrder("test1", "test2"));
  }

  @Test
  public void foreignKeySearchMultiChildParent() {
    assertThat(cqlc("tablea.name == test3"), is(empty()));
  }

  @Test
  public void foreignKeySearchMultiWildChildParent() {
    assertThat(cqlc("tablea.name == *"), containsInAnyOrder("z1", "z2", "z3", "z4"));
  }

  @Test
  public void foreignKeySearch1() {
    assertThat(cqla("tableb.prefix == x1"), containsInAnyOrder("test1"));
  }

  @Test
  public void foreignKeySearch2() {
    // two tableb records match, but they both reference the same tablea record, therefore "test2" should be
    // returned one time only
    assertThat(cqla("tableb.prefix == x2"), containsInAnyOrder("test2"));
  }

  @Test
  public void searchTableaByTablebWild1() {
    assertThat(cqla("tableb.prefix == *"), containsInAnyOrder("test1", "test2", "test3", "test4"));
  }

  @Test
  public void searchTableaByTablebWild2() {
    assertThat(cqla("tableb.prefix = *"), containsInAnyOrder("test1", "test2", "test3", "test4"));
  }

  @Test
  public void uuidConstant() {
    assertThat(cqla("tableb.name == 33333333-3333-3333-3333-333333333333"), is(empty()));
  }

  @Test
  public void searchTableaByTablebId() {
    assertThat(cqla("tableb.id == B5555555-5555-4000-8000-000000000000"), containsInAnyOrder("test3"));
  }

  @Test
  public void searchTablebByTablefId() {
    assertThat(cqlb("tablefId == F1111111-1111-4000-8000-000000000000"), containsInAnyOrder("x6"));
  }

  @Test
  public void searchTableaByTablebTablefId() {
    assertThat(cqla("tableb.tablefId == F1111111-1111-4000-8000-000000000000"), containsInAnyOrder("test4"));
  }

  @Test
  public void sqlInjection1() {
    //check to see if we can execute some arbitrary sql
    assertThat(cqla("tableb.prefix == \"x0')));((('DROP tableb\""), containsInAnyOrder("test3") );
    //then check to see if the drop table worked by checking to see if there is anything there
    assertThat(cqla("tableb.prefix == \"x0')));((('DROP tableb\"").size() > 0, is(true));
  }

  @Test
  public void sqlInjection2() {
    //this test is to see if I can query more items then intended
    //if the test passes it means it was not successful
    assertThat(cqla("tableb.prefix == \"x0')  or 1=1)--\""), is(empty()) );
  }

   @Test
   public void fieldNameContainsDot() throws Exception {
     assertThat(cqlb("copyrightStatus.name==cc1"), containsInAnyOrder("x1"));
   }
}
