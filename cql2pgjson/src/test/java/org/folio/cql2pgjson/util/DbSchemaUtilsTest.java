package org.folio.cql2pgjson.util;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.folio.cql2pgjson.model.DbIndex;
import org.folio.rest.persist.ddlgen.Index;
import org.folio.rest.persist.ddlgen.Schema;
import org.folio.rest.persist.ddlgen.Table;
import org.junit.Before;
import org.junit.Test;

public class DbSchemaUtilsTest {

  private static Index newIndex(String name) {
    Index index = new Index();
    index.setFieldName(name);
    return index;
  }

  private Schema schema;

  @Before
  public void setupSchema() {
    schema = new Schema();

    Table table = new Table();
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
    DbIndex dbIndex = DbSchemaUtils.getDbIndex(schema, "users", "name");
    assertTrue(dbIndex.isFt());
    assertTrue(dbIndex.isGin());
    assertTrue(dbIndex.isOther());
  }

  @Test
  public void testGetDbIndexForEmail() {
    DbIndex dbIndex = DbSchemaUtils.getDbIndex(schema, "users", "email");
    assertFalse(dbIndex.isFt());
    assertFalse(dbIndex.isGin());
    assertTrue(dbIndex.isOther());
  }

  @Test
  public void testGetDbIndexForAddress() {
    DbIndex dbIndex = DbSchemaUtils.getDbIndex(schema, "users", "address");
    assertFalse(dbIndex.isFt());
    assertFalse(dbIndex.isGin());
    assertTrue(dbIndex.isOther());
  }

}
