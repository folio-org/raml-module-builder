package org.folio.cql2pgjson;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.folio.rest.persist.ddlgen.Table;
import org.junit.Test;

public class DBSchemaTest {

  @Test
  public void makeInstanceWithSpecifiedDBSchemaPath() throws Exception {
    Path dbSchemaPath = Paths.get("./test_db_schema.json");
    if(dbSchemaPath == null) {
      throw new Exception("Can't find path");
    }
    CQL2PgJSON cql2pgjson = new CQL2PgJSON("instance.jsonb");
    cql2pgjson.setDbSchemaPath(dbSchemaPath.toString());
    List<Table> tables = cql2pgjson.getDbSchema().getTables();
    String[] tableArray = new String[]{ "loan_type", "material_type", "service_point_user" };
    for(String tableName : tableArray) {
      boolean found = false;
      for (Table table : tables) {
        if (tableName.equalsIgnoreCase(table.getTableName())) {
          found = true;
          break;
        }
      }
      assertThat(tableName, found, is(true));
    }
  }
}
