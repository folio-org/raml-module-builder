package org.folio.cql2pgjson;

import static org.junit.Assert.*;

import org.junit.runner.RunWith;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import junitparams.JUnitParamsRunner;

@RunWith(JUnitParamsRunner.class)
public class ForeignKeyGenerationWithJoinTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  // existing ones using Subquery

  @Test
  public void testSearchInstanceByItemBarcode() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("instance.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyInstanceItem.json");
    String sql = cql2pgJson.toSql("item.barcode == 7834324634").toString();
    String expected = "WHERE instance.id IN  ( SELECT (holdings_record.jsonb->>'instanceId')::UUID from holdings_record WHERE holdings_record.id IN  ( SELECT (item.jsonb->>'holdingsRecordId')::UUID from item WHERE lower(f_unaccent(item.jsonb->>'barcode')) LIKE lower(f_unaccent('7834324634'))))";
    assertEquals(expected, sql);
  }

  @Test
  public void testSearchItemByInstanceTitle() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("item.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyInstanceItem.json");
    String sql = cql2pgJson.toSql("instance.title = 'Olmsted in Chicago'").toString();
    String expected = "WHERE (item.jsonb->>'holdingsRecordId')::UUID IN  ( SELECT id from holdings_record WHERE (holdings_record.jsonb->>'instanceId')::UUID IN  ( SELECT id from instance WHERE to_tsvector('simple', f_unaccent(instance.jsonb->>'title')) @@ replace((to_tsquery('simple', f_unaccent(''',Olmsted''')) && to_tsquery('simple', f_unaccent('''in''')) && to_tsquery('simple', f_unaccent('''Chicago,''')))::text, '&', '<->')::tsquery))";
    assertEquals(expected, sql);
  }

  // new ones using JOIN

  @Test
  public void testSearchInstanceOrderbyHoldingsPermanentLocation() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("instance.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyInstanceItem.json");
    String cql = "title == abc* sortBy holdingsRecord.permanentLocationId";
    String sql = cql2pgJson.toSql(cql).toString();
    System.out.println(cql);
    System.out.println(sql);
    String expected = "SELECT instance FROM instance LEFT OUTER JOIN holdings_record ON (holdings_record.jsonb->>'instanceId')::UUID = instance.id WHERE lower(f_unaccent(instance.jsonb->>'title')) LIKE lower(f_unaccent('abc%')) ORDER BY lower(f_unaccent(holdings_record.jsonb->>'permanentLocationId'))";
    assertEquals(expected, sql);
  }

  @Test
  public void testSearchInstanceOrderbyItemBarcode() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("instance.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyInstanceItem.json");
    String cql = "title == abc* sortBy item.barcode";
    String sql = cql2pgJson.toSql(cql).toString();
    System.out.println(cql);
    System.out.println(sql);
    String expected = "SELECT instance FROM instance LEFT OUTER JOIN holdings_record ON (holdings_record.jsonb->>'instanceId')::UUID = instance.id LEFT OUTER JOIN item ON (item.jsonb->>'holdingsRecordId')::UUID = holdings_record.id WHERE lower(f_unaccent(instance.jsonb->>'title')) LIKE lower(f_unaccent('abc%')) ORDER BY lower(f_unaccent(item.jsonb->>'barcode'))";
    assertEquals(expected, sql);
  }

  @Test
  public void testSearchItemSorbyHoldingsPermanentLocation() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("item.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyInstanceItem.json");
    String cql = "barcode == 123* sortBy holdingsRecord.permanentLocationId";
    String sql = cql2pgJson.toSql(cql).toString();
    System.out.println(cql);
    System.out.println(sql);
    String expected = "SELECT item FROM item LEFT OUTER JOIN holdings_record ON (item.jsonb->>'holdingsRecordId')::UUID = holdings_record.id WHERE lower(f_unaccent(item.jsonb->>'barcode')) LIKE lower(f_unaccent('123%')) ORDER BY lower(f_unaccent(holdings_record.jsonb->>'permanentLocationId'))";
    assertEquals(expected, sql);
  }

  @Test
  public void testSearchItemSorbyInstanceTitle() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("item.jsonb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/foreignKeyInstanceItem.json");
    String cql = "barcode == 123* sortBy instance.title";
    String sql = cql2pgJson.toSql(cql).toString();
    System.out.println(cql);
    System.out.println(sql);
    String expected = "SELECT item FROM item LEFT OUTER JOIN holdings_record ON (item.jsonb->>'holdingsRecordId')::UUID = holdings_record.id LEFT OUTER JOIN instance ON (holdings_record.jsonb->>'instanceId')::UUID = instance.id WHERE lower(f_unaccent(item.jsonb->>'barcode')) LIKE lower(f_unaccent('123%')) ORDER BY lower(f_unaccent(instance.jsonb->>'title'))";
    assertEquals(expected, sql);
  }

}
