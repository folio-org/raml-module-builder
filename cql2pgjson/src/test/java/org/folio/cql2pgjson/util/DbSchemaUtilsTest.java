package org.folio.cql2pgjson.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.*;

import java.io.IOException;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DbSchemaUtilsTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

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

  private Schema schema(String schemaPath) {
    try {
      String dbJson = ResourceUtil.asString(schemaPath, CQL2PgJSON.class);
      return ObjectMapperTool.getMapper().readValue(dbJson, org.folio.rest.persist.ddlgen.Schema.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFindForeignKeys() throws Exception {
    // pathA: f -> e -> d -> c -> b -> a
    // pathB: f -> e -> c -> b -> a
    Schema dbSchema = schema("templates/db_scripts/foreignKeyPath.json");

    // child->parent with targetAlias
    List<DbFkInfo> list = DbSchemaUtils.findForeignKeysFromSourceTableToTargetAlias(dbSchema, "f", "a");
    assertEquals("f", list.get(0).getTable());
    assertEquals("e", list.get(1).getTable());
    assertEquals("c", list.get(2).getTable());
    assertEquals("b", list.get(3).getTable());

    // child->parent with targetAlias
    list = DbSchemaUtils.findForeignKeysFromSourceTableToTargetAlias(dbSchema, "f", "bAlias");
    assertEquals("f", list.get(0).getTable());
    assertEquals("e", list.get(1).getTable());
    assertEquals("c", list.get(2).getTable());

    // parent->child with sourceAlias
    list = DbSchemaUtils.findForeignKeysFromSourceAliasToTargetTable(dbSchema, "eAlias", "b");
    assertEquals("e", list.get(0).getTable());
    assertEquals("d", list.get(1).getTable());
    assertEquals("c", list.get(2).getTable());

    // parent->child with sourceAlias
    list = DbSchemaUtils.findForeignKeysFromSourceAliasToTargetTable(dbSchema, "e2Alias", "b");
    assertEquals("e", list.get(0).getTable());
    assertEquals("c", list.get(1).getTable());

    list = DbSchemaUtils.findForeignKeysFromSourceTableToTargetAlias(dbSchema, "nonexistingTable", "a");
    assertThat(list, is(empty()));
  }

  @Test
  public void pathTableNotFound() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("table not found");
    thrown.expectMessage("targetPath=[invalidId, invalidId]");

    Schema dbSchema = schema("templates/db_scripts/foreignKeyPath.json");
    DbSchemaUtils.findForeignKeysFromSourceTableToTargetAlias(dbSchema, "i", "a");
  }

  @Test
  public void pathForeignKeyNotFound() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("foreignKey not found");
    thrown.expectMessage("fieldName=nonexisting");

    Schema dbSchema = schema("templates/db_scripts/foreignKeyPath.json");
    DbSchemaUtils.findForeignKeysFromSourceTableToTargetAlias(dbSchema, "i", "b");
  }

}
