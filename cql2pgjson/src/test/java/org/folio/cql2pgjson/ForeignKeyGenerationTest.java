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
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb WHERE (tableb.jsonb->>'blah')::numeric =123452)", sql);
  }

  @Test
  public void ForeignKeySearchChildParent() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb "
        + "WHERE lower(f_unaccent(tableb.jsonb->>'prefix')) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111')))", sql);
  }

  @Test
  public void ForeignKeySearchParentChild() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tablea.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("tableb.tableaId IN  ( SELECT id FROM tablea "
        + "WHERE lower(f_unaccent(tablea.jsonb->>'prefix')) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111')))", sql);
  }

  @Test
  public void ForeignKeySearchWithLowerConstant() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.gprefix == x0").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb WHERE lower(tableb.jsonb->>'gprefix') LIKE lower('x0'))", sql);
  }

  @Test
  public void ForeignKeySearchWithLowerConstantwithft() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.ftprefix = x0").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb "
        + "WHERE to_tsvector('simple', tableb.jsonb->>'ftprefix') @@ replace((to_tsquery('simple', ('''x0''')))::text, '&', '<->')::tsquery)", sql);
  }

  @Test
  public void ForeignKeySearchftStar() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.prefix = *").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb WHERE true)", sql);
  }

  @Test
  public void ForeignKeySearchftStarParentChild() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tablea.prefix = *").getWhere();
    assertEquals("tableb.tableaId IN  ( SELECT id FROM tablea WHERE true)", sql);
  }

  @Test
  public void foreignKeyChildParentDisabled() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tabled.json");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    // "targetTableAlias" is disabled for tablec therefore tablec.id = *
    // looks into the tabled table and checks the tablec field and its id subfield - this is always true;
    // if it were enabled it would check that a tablec record exists for that id.
    String sql = cql2pgJson.toSql("tablec.id = *").getWhere();
    assertEquals("true", sql);
  }

  @Test
  public void foreignKeyParentChildDisabled() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablec.json");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    // "tableAlias" is disabled for tabled therefore tabled.id = *
    // looks into the tablec table and checks the tabled field and its id subfield - this is always true;
    // if it were enabled it would check that a tablec record exists for that id.
    String sql = cql2pgJson.toSql("tabled.id = *").getWhere();
    assertEquals("true", sql);
  }

  @Test
  public void ForeignKeySearchLikeStar() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.prefix == *").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb "
        + "WHERE lower(f_unaccent(tableb.jsonb->>'prefix')) LIKE lower(f_unaccent('%')))", sql);
  }

  @Test
  public void foreignKeySearchMulti() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tablec.cindex == z1").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb "
        + "WHERE tableb.id IN  ( SELECT tablebId FROM tablec "
        + "WHERE lower(f_unaccent(tablec.jsonb->>'cindex')) LIKE lower(f_unaccent('z1'))))",sql);
  }

  @Test
  public void ForeignKeySearchWithFUnaccent() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKey.json");
    String sql = cql2pgJson.toSql("tableb.otherindex >= y0").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb WHERE tableb.jsonb->>'otherindex' >='y0')", sql);
  }

  @Test
  public void ForeignKeySearchWithMalformedFK() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyMalformedFK.json");

    thrown.expectMessage("foreignKey not found");
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
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyInstanceItem.json");
    String sql = cql2pgJson.toSql("item.barcode == 7834324634").toString();
    String expected = "WHERE instance.id IN  ( SELECT instanceId FROM holdings_record "
        + "WHERE holdings_record.id IN  ( SELECT holdingsRecordId FROM item "
        + "WHERE lower(f_unaccent(item.jsonb->>'barcode')) LIKE lower(f_unaccent('7834324634'))))";
    assertEquals(expected, sql);
  }

  @Test
  public void testSearchItemByInstanceTitle() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("item");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyInstanceItem.json");
    String sql = cql2pgJson.toSql("instance.title = Olmsted").toString();
    String expected = "WHERE item.holdingsRecordId IN  ( SELECT id FROM holdings_record "
        + "WHERE holdings_record.instanceId IN  ( SELECT id FROM instance "
        + "WHERE to_tsvector('simple', f_unaccent(instance.jsonb->>'title')) @@ replace((to_tsquery('simple', f_unaccent('''Olmsted''')))::text, '&', '<->')::tsquery))";
    assertEquals(expected, sql);
  }

}
