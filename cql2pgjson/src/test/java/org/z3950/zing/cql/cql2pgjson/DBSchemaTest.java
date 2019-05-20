package org.z3950.zing.cql.cql2pgjson;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.folio.rest.persist.ddlgen.Table;
import org.junit.Test;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;

public class DBSchemaTest {

  @Test
  public void makeInstanceWithSpecifiedDBSchemaPath() throws Exception {
    Path dbSchemaPath = Paths.get(ClassLoader.getSystemResource("test_db_schema.json").toURI());
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
      if(!found) {
        throw new Exception(String.format("Missing tableName '%s' in db schema", tableName));
      }
    }
  }
}
