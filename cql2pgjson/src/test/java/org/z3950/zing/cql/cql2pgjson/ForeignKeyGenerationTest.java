package org.z3950.zing.cql.cql2pgjson;


import static org.junit.Assert.*;
import org.junit.runner.RunWith;

import java.util.logging.Logger;

import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.cql2pgjson.exception.ServerChoiceIndexesException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import junitparams.JUnitParamsRunner;



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
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.tableb_data == 11111111-1111-1111-1111-111111111111").getWhere();
    // default pkColumnName is id without underscore
    assertEquals(" ( SELECT tableb.jsonb->>'tableb_data' from tableb WHERE (lower(f_unaccent(tablea.jsonb->>'id'::text)) = lower(f_unaccent(tableb.jsonb->>'tableaId'::text)))) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111'))", sql);
  }
  @Test
  public void ForeignKeySearchWithMissingFK() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyMissingFK.json");
    String sql = cql2pgJson.toSql("tableb.tableb_data == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("", sql);
  }
  @Test
  public void ForeignKeySearchWithMalformedFK() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyMalformedFK.json");
    String sql = cql2pgJson.toSql("tableb.tableb_data == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("", sql);
  }
  @Test
  public void ForeignKeySearchFailureDueToTable() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.jsonb" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
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
  public void ForeignKeyFilter() throws FieldException, QueryValidationException, ServerChoiceIndexesException, FieldException, QueryValidationException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("id == tableb.tableb_data").getWhere();
    // default pkColumnName is id without underscore
    assertEquals("tablea.id IN  ( SELECT Cast ( tableb.jsonb->>'tableb_data'as UUID) from tableb)", sql);
  }
  @Test
  public void ForeignKeyFilterWithMissingFK() throws FieldException, QueryValidationException, ServerChoiceIndexesException, FieldException, QueryValidationException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyMissingFK.json");
    String sql = cql2pgJson.toSql("id == tableb.tableb_data").getWhere();
    // default pkColumnName is id without underscore
    assertEquals("", sql);
  }
  @Test
  public void ForeignKeyFilterWithMalformedFK() throws FieldException, QueryValidationException, ServerChoiceIndexesException, FieldException, QueryValidationException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyMalformedFK.json");
    String sql = cql2pgJson.toSql("id == tableb.tableb_data").getWhere();
    // default pkColumnName is id without underscore
    assertEquals("", sql);
  }
  @Test
  public void ForeignKeyFilterFailureDueToTable() throws FieldException, QueryValidationException, ServerChoiceIndexesException, FieldException, QueryValidationException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
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
