package org.z3950.zing.cql.cql2pgjson;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;

import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.cql2pgjson.exception.ServerChoiceIndexesException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;


@RunWith(JUnitParamsRunner.class)
public class ForeignKeyGenerationTest  {
  private static Logger logger = Logger.getLogger(CQL2PgJSONTest.class.getName());
  private static CQL2PgJSON cql2pgJson;

  @BeforeClass
  public static void runOnceBeforeClass() throws Exception {
  }
  @AfterClass
  public static void runOnceAfterClass() {
  }
  @Test
  public void ForeignKeySearch() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/joinExample_schema.json");
    String sql = cql2pgJson.toSql("tableb.tableb_data == 11111111-1111-1111-1111-111111111111").getWhere();
    // default pkColumnName is id without underscore
    assertEquals(" ( SELECT tableb.jsonb->>'tableb_data' from tableb WHERE (lower(f_unaccent(tablea.jsonb->>'id'::text)) = lower(f_unaccent(tableb.jsonb->>'tableb_data'::text)))) LIKE lower(f_unaccent(11111111-1111-1111-1111-111111111111))", sql);
  }
  @Test
  public void ForeignKeySearchFailureDueToTable() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.jsonb" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/joinExample_schema.json");
    try {
      String sql = cql2pgJson.toSql("tablec.tablec_data == 11111111-1111-1111-1111-111111111111").getWhere();
      assertEquals("lower(f_unaccent(tablea.jsonb->'tablec'->>'tablec_data')) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111'))",sql);
      sql = cql2pgJson.toSql("ardgsdfgdsfg.tableb_data == 11111111-1111-1111-1111-111111111111").getWhere();
      assertEquals("lower(f_unaccent(tablea.jsonb->'ardgsdfgdsfg'->>'tableb_data')) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111'))",sql);
    } catch(Exception e) {
      e.printStackTrace();
    }
    // default pkColumnName is id without underscore
    //assertEquals(" ( SELECT TableA.jsonb->>'tableb_data' from TableB WHERE (lower(f_unaccent(TableA.jsonb->>'id'::text)) = lower(f_unaccent(TableB.jsonb->>'tableb_data'::text)))) LIKE lower(f_unaccent(11111111-1111-1111-1111-111111111111))", sql);
  }
  @Test
  public void ForeignKeySearchFailureDueToIncorrectIdFormat() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.jsonb" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/joinExample_schema.json");
    try {
      String sql = cql2pgJson.toSql("tableb.tableb_data == 11111111111111111111111111111111").getWhere();
      assertEquals("",sql);
      sql = cql2pgJson.toSql("tableb.tableb_data == 1").getWhere();
      assertEquals("",sql);
    } catch(Exception e) {
      e.printStackTrace();
    }
    // default pkColumnName is id without underscore
    //assertEquals(" ( SELECT TableA.jsonb->>'tableb_data' from TableB WHERE (lower(f_unaccent(TableA.jsonb->>'id'::text)) = lower(f_unaccent(TableB.jsonb->>'tableb_data'::text)))) LIKE lower(f_unaccent(11111111-1111-1111-1111-111111111111))", sql);
  }
  @Test
  public void ForeignKeyFilter() throws FieldException, QueryValidationException, ServerChoiceIndexesException, FieldException, QueryValidationException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/joinExample_schema.json");
    String sql = cql2pgJson.toSql("id == tableb.tableb_data").getWhere();
    // default pkColumnName is id without underscore
    assertEquals(" IN  ( SELECT tableb.jsonb->>'tableb_data' from tableb)", sql);
  }
  @Test
  public void ForeignKeyFilterFailureDueToTable() throws FieldException, QueryValidationException, ServerChoiceIndexesException, FieldException, QueryValidationException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/joinExample_schema.json");
    try {
      String sql = cql2pgJson.toSql("id == tablec.tablec_data").getWhere();
      assertEquals("false /* id == invalid UUID */",sql);
      sql = cql2pgJson.toSql("tablec_data == id").getWhere();
      assertEquals("lower(f_unaccent(tablea.jsonb->>'tablec_data')) LIKE lower(f_unaccent('id'))",sql);
    } catch(Exception e) {
      e.printStackTrace();
    }
    // default pkColumnName is id without underscore
    //assertEquals("IN  ( SELECT TableC.jsonb->>'tableb_data' from TableC)", sql);
  }
}
