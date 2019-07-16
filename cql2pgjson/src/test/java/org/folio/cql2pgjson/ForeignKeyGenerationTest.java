package org.folio.cql2pgjson;

import static org.junit.Assert.*;
import org.junit.runner.RunWith;

import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.cql2pgjson.exception.ServerChoiceIndexesException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import junitparams.JUnitParamsRunner;

@RunWith(JUnitParamsRunner.class)
public class ForeignKeyGenerationTest  {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void ForeignKeySearchNumeric() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.blah == /number 123452").getWhere();
    assertEquals("tablea.id IN  ( SELECT (tableb.jsonb->>'tableaId')::UUID from tableb WHERE (tableb.jsonb->>'blah')::NUMERIC = ('123452')::NUMERIC)", sql);
  }

  @Test
  public void ForeignKeySearch() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("tablea.id IN  ( SELECT (tableb.jsonb->>'tableaId')::UUID from tableb WHERE lower(tableb.jsonb->>'prefix') = lower('11111111-1111-1111-1111-111111111111'))", sql);
  }

  @Test
  public void ForeignKeySearchParentChild() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tablea.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("(tableb.jsonb->>'tableaId')::UUID IN  ( SELECT id from tablea WHERE lower(tablea.jsonb->>'prefix') = lower('11111111-1111-1111-1111-111111111111'))", sql);
  }

  @Test
  public void ForeignKeySearchWithLowerConstant() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.prefix == x0").getWhere();
    assertEquals("tablea.id IN  ( SELECT (tableb.jsonb->>'tableaId')::UUID from tableb WHERE lower(tableb.jsonb->>'prefix') = lower('x0'))", sql);
  }
  @Test
  public void ForeignKeySearchWithStar() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.prefix = *").getWhere();
    assertEquals("tablea.id IN  ( SELECT (tableb.jsonb->>'tableaId')::UUID from tableb WHERE true)", sql);
  }
  @Test
  public void ForeignKeySearchWithFUnaccent() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.otherindex == y0").getWhere();
    assertEquals("tablea.id IN  ( SELECT (tableb.jsonb->>'tableaId')::UUID from tableb WHERE f_unaccent(tableb.jsonb->>'otherindex') = f_unaccent('y0'))", sql);
  }

  @Test
  public void ForeignKeySearchWithMissingFK() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyMissingFK.json");

    thrown.expect(QueryValidationException.class);
    thrown.expectMessage("No foreignKey for table tableb found");
    cql2pgJson.toSql("tableb.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
  }

  @Test
  public void ForeignKeySearchWithMalformedFK() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyMalformedFK.json");

    thrown.expect(QueryValidationException.class);
    thrown.expectMessage("Missing target table");
    thrown.expectMessage("field tableaId");
    cql2pgJson.toSql("tableb.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
  }

  @Test
  public void ForeignKeySearchFailureDueToTable() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.jsonb" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tablex.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("lower(f_unaccent(tablea.jsonb->'tablex'->>'prefix')) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111'))",sql);
    sql = cql2pgJson.toSql("ardgsdfgdsfg.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("lower(f_unaccent(tablea.jsonb->'ardgsdfgdsfg'->>'prefix')) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111'))",sql);
  }

  @Test
  public void invalidCurrentTable() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("invalid");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.id == 1").toString();
    assertTrue(sql, sql.contains("'tableb'"));
  }

}
