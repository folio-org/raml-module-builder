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

  private CQL2PgJSON cql2pgJson(String field, String schema) throws FieldException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON(field);
    cql2pgJson.setDbSchemaPath("templates/db_scripts/" + schema);
    return cql2pgJson;
  }

  @Test
  public void searchNumeric() throws Exception {
    String sql = cql2pgJson("tablea.json", "foreignKey.json")
        .toSql("tableb.blah == /number 123452").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb WHERE (tableb.jsonb->>'blah')::numeric ='123452')", sql);
  }

  @Test
  public void searchChildParent() throws Exception {
    String sql = cql2pgJson("tablea.json", "foreignKey.json")
        .toSql("tableb.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb WHERE"
        + " CASE WHEN length(lower(f_unaccent('11111111-1111-1111-1111-111111111111'))) <= 600"
        + " THEN left(lower(f_unaccent(tableb.jsonb->>'prefix')),600) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111'))"
        + " ELSE left(lower(f_unaccent(tableb.jsonb->>'prefix')),600) LIKE left(lower(f_unaccent('11111111-1111-1111-1111-111111111111')),600)"
        + " AND lower(f_unaccent(tableb.jsonb->>'prefix')) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111'))"
        + " END)", sql);
  }

  @Test
  public void searchParentChild() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    String sql = cql2pgJson("tableb.json", "foreignKey.json")
        .toSql("tablea.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("tableb.tableaId IN  ( SELECT id FROM tablea WHERE"
        + " CASE WHEN length(lower(f_unaccent('11111111-1111-1111-1111-111111111111'))) <= 600"
        + " THEN left(lower(f_unaccent(tablea.jsonb->>'prefix')),600) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111'))"
        + " ELSE left(lower(f_unaccent(tablea.jsonb->>'prefix')),600) LIKE left(lower(f_unaccent('11111111-1111-1111-1111-111111111111')),600)"
        + " AND lower(f_unaccent(tablea.jsonb->>'prefix')) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111'))"
        + " END)", sql);
  }

  @Test
  public void searchWithLowerConstant() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    String sql = cql2pgJson("tablea.json", "foreignKey.json")
        .toSql("tableb.gprefix == x0").getWhere();  // ginx index
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb WHERE"
        + " lower(tableb.jsonb->>'gprefix') LIKE lower('x0'))", sql);
  }

  @Test
  public void searchWithLowerConstantwithft() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    String sql = cql2pgJson("tablea.json", "foreignKey.json")
        .toSql("tableb.ftprefix = x0").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb "
        + "WHERE get_tsvector(tableb.jsonb->>'ftprefix') @@ tsquery_phrase('x0'))", sql);
  }

  @Test
  public void searchftStar() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    String sql = cql2pgJson("tablea.json", "foreignKey.json")
        .toSql("tableb.prefix = *").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb WHERE true)", sql);
  }

  @Test
  public void searchftStarParentChild() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    String sql = cql2pgJson("tableb.json", "foreignKey.json")
        .toSql("tablea.prefix = *").getWhere();
    assertEquals("tableb.tableaId IN  ( SELECT id FROM tablea WHERE true)", sql);
  }

  @Test
  public void childParentDisabled() throws Exception {
    String sql = cql2pgJson("tabled.json", "foreignKey.json")
        .toSql("tablec.id = *").getWhere();
    // "targetTableAlias" is disabled for tablec therefore tablec.id = *
    // looks into the tabled table and checks the tablec field and its id subfield - this is always true;
    // if it were enabled it would check that a tablec record exists for that id.
    assertEquals("true", sql);
  }

  @Test
  public void parentChildDisabled() throws Exception {
    String sql = cql2pgJson("tablec.json", "foreignKey.json")
        .toSql("tabled.id = *").getWhere();
    // "tableAlias" is disabled for tabled therefore tabled.id = *
    // looks into the tablec table and checks the tabled field and its id subfield - this is always true;
    // if it were enabled it would check that a tablec record exists for that id.
    assertEquals("true", sql);
  }

  @Test
  public void searchLikeStar() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    String sql = cql2pgJson("tablea.json", "foreignKey.json")
        .toSql("tableb.prefix == *").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb WHERE "
        + "CASE WHEN length(lower(f_unaccent('%'))) <= 600 "
        + "THEN left(lower(f_unaccent(tableb.jsonb->>'prefix')),600) LIKE lower(f_unaccent('%')) "
        + "ELSE left(lower(f_unaccent(tableb.jsonb->>'prefix')),600) LIKE left(lower(f_unaccent('%')),600) "
        + "AND lower(f_unaccent(tableb.jsonb->>'prefix')) LIKE lower(f_unaccent('%')) END)", sql);
  }

  @Test
  public void searchMulti() throws Exception {
    String sql = cql2pgJson("tablea.json", "foreignKey.json")
        .toSql("tablec.cindex == z1").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb "
        + "WHERE tableb.id IN  ( SELECT tablebId FROM tablec "
        + "WHERE lower(f_unaccent(tablec.jsonb->>'cindex')) LIKE lower(f_unaccent('z1'))))",sql);
  }

  @Test
  public void searchWithFUnaccent() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    String sql = cql2pgJson("tablea.json", "foreignKey.json")
        .toSql("tableb.otherindex >= y0").getWhere();
    assertEquals("tablea.id IN  ( SELECT tableaId FROM tableb WHERE "
        + "CASE WHEN length(f_unaccent('y0')) <= 600 "
        + "THEN left(f_unaccent(tableb.jsonb->>'otherindex'),600) >= f_unaccent('y0') "
        + "ELSE left(f_unaccent(tableb.jsonb->>'otherindex'),600) >= left(f_unaccent('y0'),600) "
        + "AND f_unaccent(tableb.jsonb->>'otherindex') >= f_unaccent('y0') END)", sql);
  }

  @Test
  public void searchWithMalformedFK() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    thrown.expectMessage("foreignKey not found");
    cql2pgJson("tablea.json", "foreignKeyMalformedFK.json")
        .toSql("tableb.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
  }

  @Test
  public void searchFailureDueToTable() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    CQL2PgJSON cql2pgJson = cql2pgJson("tablea.jsonb", "foreignKey.json");
    String sql = cql2pgJson.toSql("tablex.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("lower(f_unaccent(tablea.jsonb->'tablex'->>'prefix')) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111'))",sql);
    sql = cql2pgJson.toSql("ardgsdfgdsfg.prefix == 11111111-1111-1111-1111-111111111111").getWhere();
    assertEquals("lower(f_unaccent(tablea.jsonb->'ardgsdfgdsfg'->>'prefix')) LIKE lower(f_unaccent('11111111-1111-1111-1111-111111111111'))",sql);
  }

  @Test
  public void invalidCurrentTable() throws Exception {
    String sql = cql2pgJson("invalid", "foreignKey.json")
        .toSql("tableb.id == 1").toString();
    assertTrue(sql, sql.contains("'tableb'"));
  }

  @Test
  public void searchInstanceByItemBarcode() throws Exception {
    String sql = cql2pgJson("instance", "foreignKeyInstanceItem.json")
        .toSql("item.barcode == 7834324634").toString();
    String expected = "WHERE instance.id IN  ( SELECT instanceId FROM holdings_record "
        + "WHERE holdings_record.id IN  ( SELECT holdingsRecordId FROM item "
        + "WHERE lower(f_unaccent(item.jsonb->>'barcode')) LIKE lower(f_unaccent('7834324634'))))";
    assertEquals(expected, sql);
  }

  @Test
  public void searchItemByInstanceTitle() throws Exception {
    String sql = cql2pgJson("item", "foreignKeyInstanceItem.json")
        .toSql("instance.title = Olmsted").toString();
    String expected = "WHERE item.holdingsRecordId IN  ( SELECT id FROM holdings_record "
        + "WHERE holdings_record.instanceId IN  ( SELECT id FROM instance "
        + "WHERE get_tsvector(f_unaccent(instance.jsonb->>'title')) @@ tsquery_phrase(f_unaccent('Olmsted'))))";
    assertEquals(expected, sql);
  }

  @Test
  public void searchInstanceByHoldingsId() throws Exception {
    String sql = cql2pgJson("instance", "foreignKeyInstanceItem.json")
        .toSql("holdingsRecord.id==53cf956f-c1df-410b-8bea-27f712cca7c0").toString();
    String expected = "WHERE instance.id IN  ( SELECT instanceId FROM holdings_record "
        + "WHERE id='53cf956f-c1df-410b-8bea-27f712cca7c0')";
    assertEquals(expected, sql);
  }

  @Test
  public void searchHoldingByPermanentLocationId() throws Exception {
    String sql = cql2pgJson("holdings_record.jsonb", "foreignKeyInstanceItem.json")
        .toSql("permanentLocationId==53cf956f-c1df-410b-8bea-27f712cca7c0").toString();
    String expected = "WHERE permanentLocationId='53cf956f-c1df-410b-8bea-27f712cca7c0'";
    assertEquals(expected, sql);
  }

  @Test
  public void searchInstanceByHoldingsPermanentLocationId() throws Exception {
    String sql = cql2pgJson("instance.jsonb", "foreignKeyInstanceItem.json")
        .toSql("holdingsRecord.permanentLocationId==53cf956f-c1df-410b-8bea-27f712cca7c0").toString();
    String expected = "WHERE instance.id IN  ( SELECT instanceId FROM holdings_record "
        + "WHERE permanentLocationId='53cf956f-c1df-410b-8bea-27f712cca7c0')";
    assertEquals(expected, sql);
  }

  @Test
  public void searchItemByHoldingsPermanentLocationId() throws Exception {
    String sql = cql2pgJson("instance.jsonb", "foreignKeyInstanceItem.json")
        .toSql("holdingsRecord.permanentLocationId==53cf956f-c1df-410b-8bea-27f712cca7c0").toString();
    String expected = "WHERE instance.id IN  ( SELECT instanceId FROM holdings_record "
        + "WHERE permanentLocationId='53cf956f-c1df-410b-8bea-27f712cca7c0')";
    assertEquals(expected, sql);
  }


    @Test
  public void searchItemByHoldingsPermanentLocationName() throws Exception {
      CQL2PgJSON cql2PgJSON = cql2pgJson("coursereserves_reserves", "courseReservesSchema.json");
      String sql = cql2PgJSON
        .toSql("(courseListingId = 1bd04c24-8f8c-4e33-b69c-359146e43808) AND copyrightStatus.name==cc").toString();
    String expected = "WHERE (courseListingId='1bd04c24-8f8c-4e33-b69c-359146e43808') AND (coursereserves_reserves.copyrightTracking_copyrightStatusId IN  "
        + "( SELECT id FROM coursereserves_copyrightstates WHERE lower(f_unaccent(coursereserves_copyrightstates.jsonb->>'name')) "
        + "LIKE lower(f_unaccent('cc'))))";
    assertEquals(expected, sql);
  }
}
