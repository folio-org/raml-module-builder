package org.folio.cql2pgjson.util;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.model.DbFkInfo;
import org.folio.cql2pgjson.model.DbIndex;
import org.folio.rest.persist.ddlgen.Index;
import org.folio.rest.persist.ddlgen.Schema;
import org.folio.rest.persist.ddlgen.Table;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.util.ResourceUtil;
import org.junit.Before;
import org.junit.Test;

public class DbSchemaUtilsTest {

  private static Index newIndex(String name) {
    Index index = new Index();
    index.setFieldName(name);
    return index;
  }

  private Table table;

  @Before
  public void setupSchema() {
    Schema schema = new Schema();

    table = new Table();
    table.setTableName("users");

    table.setFullTextIndex(Arrays.asList(newIndex("name")));
    table.setGinIndex(Arrays.asList(newIndex("name")));
    table.setIndex(Arrays.asList(newIndex("name")));

    table.setUniqueIndex(Arrays.asList(newIndex("email")));
    table.setLikeIndex(Arrays.asList(newIndex("address")));

    schema.setTables(Arrays.asList(table));
  }

  @Test
  public void testGetDbIndexForName() {
    DbIndex dbIndex = DbSchemaUtils.getDbIndex(table, "name");
    assertTrue(dbIndex.isFt());
    assertTrue(dbIndex.isGin());
    assertTrue(dbIndex.isOther());
  }

  @Test
  public void testGetDbIndexForEmail() {
    DbIndex dbIndex = DbSchemaUtils.getDbIndex(table, "email");
    assertFalse(dbIndex.isFt());
    assertFalse(dbIndex.isGin());
    assertTrue(dbIndex.isOther());
  }

  @Test
  public void testGetDbIndexForAddress() {
    DbIndex dbIndex = DbSchemaUtils.getDbIndex(table, "address");
    assertFalse(dbIndex.isFt());
    assertFalse(dbIndex.isGin());
    assertTrue(dbIndex.isOther());
  }

  @Test
  public void testGetDbIndexForNullTable() {
    DbIndex dbIndex = DbSchemaUtils.getDbIndex(null, "address");
    assertFalse(dbIndex.isFt());
    assertFalse(dbIndex.isGin());
    assertFalse(dbIndex.isOther());
  }

  @Test
  public void testFindForeignKeys() throws Exception {
    // pathA: f -> e -> d -> c -> b -> a
    // pathB: f -> e -> c -> b -> a
    String schemaPath = "templates/db_scripts/foreignKeyPath.json";
    String dbJson = ResourceUtil.asString(schemaPath, CQL2PgJSON.class);
    Schema dbSchema = ObjectMapperTool.getMapper().readValue(dbJson, org.folio.rest.persist.ddlgen.Schema.class);

    // the shorter path
    List<DbFkInfo> list = DbSchemaUtils.findForeignKeysFromSourceTableToTargetAlias(dbSchema, "f", "a");
    assertEquals("f", list.get(0).getTable());
    assertEquals("e", list.get(1).getTable());
    assertEquals("c", list.get(2).getTable());
    assertEquals("b", list.get(3).getTable());

    // target alias
    list = DbSchemaUtils.findForeignKeysFromSourceTableToTargetAlias(dbSchema, "f", "bAlias");
    assertEquals("f", list.get(0).getTable());
    assertEquals("e", list.get(1).getTable());
    assertEquals("c", list.get(2).getTable());

    // source alias
    list = DbSchemaUtils.findForeignKeysFromSourceAliasToTargetTable(dbSchema, "eAlias", "b");
    assertEquals("e", list.get(0).getTable());
    assertEquals("d", list.get(1).getTable());
    assertEquals("c", list.get(2).getTable());

    // source alias 2
    list = DbSchemaUtils.findForeignKeysFromSourceAliasToTargetTable(dbSchema, "e2Alias", "b");
    assertEquals("e", list.get(0).getTable());
    assertEquals("c", list.get(1).getTable());
  }

}
