package org.folio.cql2pgjson;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;

import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.cql2pgjson.exception.ServerChoiceIndexesException;
import org.junit.Ignore;
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
    assertEquals("tablea.id IN  ( SELECT (tableb.jsonb->>'tableaId')::UUID from tableb WHERE (tableb.jsonb->>'blah')::numeric =123452)", sql);
  }

  @Test
  public void ForeignKeySearch() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("tablea.id IN  ( SELECT (tableb.jsonb->>'tableaId')::UUID from tableb WHERE lower(f_unaccent(tableb.jsonb->>'prefix')) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111')))", sql);
  }

  @Test
  public void ForeignKeySearchParentChild() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tablea.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("(tableb.jsonb->>'tableaId')::UUID IN  ( SELECT id from tablea WHERE lower(f_unaccent(tablea.jsonb->>'prefix')) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111')))", sql);
  }

  @Test
  public void ForeignKeySearchWithLowerConstant() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.gprefix == x0").getWhere();
    assertEquals("tablea.id IN  ( SELECT (tableb.jsonb->>'tableaId')::UUID from tableb WHERE lower(tableb.jsonb->>'gprefix') LIKE lower('x0'))", sql);
  }

  @Test
  public void ForeignKeySearchWithLowerConstantwithft() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.ftprefix = x0").getWhere();
    assertEquals("tablea.id IN  ( SELECT (tableb.jsonb->>'tableaId')::UUID from tableb WHERE to_tsvector('simple', tableb.jsonb->>'ftprefix') @@ replace((to_tsquery('simple', ('''x0''')))::text, '&', '<->')::tsquery)", sql);
  }

  @Test
  public void ForeignKeySearchftStar() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.prefix = *").getWhere();
    assertEquals("tablea.id IN  ( SELECT (tableb.jsonb->>'tableaId')::UUID from tableb WHERE true)", sql);
  }

  @Test
  public void ForeignKeySearchftStarParentChild() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tablea.prefix = *").getWhere();
    assertEquals("(tableb.jsonb->>'tableaId')::UUID IN  ( SELECT id from tablea WHERE true)", sql);
  }

  @Test
  public void ForeignKeySearchLikeStar() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.prefix == *").getWhere();
    assertEquals("tablea.id IN  ( SELECT (tableb.jsonb->>'tableaId')::UUID from tableb WHERE lower(f_unaccent(tableb.jsonb->>'prefix')) LIKE lower(f_unaccent('%')))", sql);
  }
  @Test
  public void foreignKeySearchMulti() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tablec.cindex == z1").getWhere();
    assertEquals("tablea.id IN  ( SELECT (tableb.jsonb->>'tableaId')::UUID from tableb WHERE tableb.id IN  ( SELECT (tablec.jsonb->>'tablebId')::UUID from tablec WHERE lower(f_unaccent(tablec.jsonb->>'cindex')) LIKE lower(f_unaccent('z1'))))",sql);
  }
  @Test
  public void ForeignKeySearchWithFUnaccent() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.otherindex >= y0").getWhere();
    assertEquals("tablea.id IN  ( SELECT (tableb.jsonb->>'tableaId')::UUID from tableb WHERE tableb.jsonb->>'otherindex' >='y0')", sql);
  }

  @Ignore
  @Test
  public void ForeignKeySearchWithMissingFK() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyMissingFK.json");

    thrown.expect(QueryValidationException.class);
    thrown.expectMessage("No foreignKey for table tableb found");
    cql2pgJson.toSql("tableb.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
  }

  @Ignore
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

  @Test
  public void testSearchInstanceByItemBarcode() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("instance");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/subquery.json");
    String sql = cql2pgJson.toSql("item.barcode == 7834324634").toString();
    String expected = "WHERE instance.id IN  ( SELECT (holdings_record.jsonb->>'instanceId')::UUID from holdings_record WHERE holdings_record.id IN  ( SELECT (item.jsonb->>'holdingsRecordId')::UUID from item WHERE lower(f_unaccent(item.jsonb->>'barcode')) LIKE lower(f_unaccent('7834324634'))))";
    assertEquals(expected, sql);
  }

  @Test
  public void testSearchItemByInstanceTitle() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("item");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/subquery.json");
    String sql = cql2pgJson.toSql("instance.title = 'Olmsted in Chicago'").toString();
    String expected = "WHERE (item.jsonb->>'holdingsRecordId')::UUID IN  ( SELECT id from holdings_record WHERE (holdings_record.jsonb->>'instanceId')::UUID IN  ( SELECT id from instance WHERE to_tsvector('simple', f_unaccent(instance.jsonb->>'title')) @@ replace((to_tsquery('simple', f_unaccent(''',Olmsted''')) && to_tsquery('simple', f_unaccent('''in''')) && to_tsquery('simple', f_unaccent('''Chicago,''')))::text, '&', '<->')::tsquery))";
    assertEquals(expected, sql);
  }

}
