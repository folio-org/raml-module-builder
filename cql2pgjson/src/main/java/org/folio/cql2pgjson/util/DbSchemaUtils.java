package org.folio.cql2pgjson.util;

import java.util.Arrays;
import java.util.List;

import org.folio.cql2pgjson.model.DbIndex;
import org.folio.rest.persist.ddlgen.Index;
import org.folio.rest.persist.ddlgen.Schema;
import org.folio.rest.persist.ddlgen.Table;

/**
 * Help method to extract info from RMB db schema.json
 */
public class DbSchemaUtils {

  private DbSchemaUtils() {
  }

  /**
   * For given index name, check if database has matching indexes.
   *
   * @param schema
   * @param indexJson
   * @return
   */
  public static DbIndex getDbIndex(Schema schema, String indexJson) {

    DbIndex dbIndexStatus = new DbIndex();

    String fieldName = CqlUtils.getFieldNameFromIndexJson(indexJson);
    String tableName = CqlUtils.getTableNameFromCqlField(fieldName);

    String indexName = CqlUtils.getIndexNameFromIndexJson(indexJson);

    if (schema.getTables() != null && !schema.getTables().isEmpty()) {
      for (Table table : schema.getTables()) {
        if (table.getTableName().equalsIgnoreCase(tableName)) {
          dbIndexStatus.setFt(checkDbIndex(indexName, table.getFullTextIndex()));
          dbIndexStatus.setGin(checkDbIndex(indexName, table.getGinIndex()));
          for (List<Index> index : Arrays.asList(table.getIndex(),
              table.getUniqueIndex(), table.getLikeIndex())) {
            dbIndexStatus.setOther(checkDbIndex(indexName, index));
            if (dbIndexStatus.isOther()) {
              break;
            }
          }
        }
      }
    }

    return dbIndexStatus;
  }

  private static boolean checkDbIndex(String cqlIndex, List<Index> indexes) {
    if (indexes != null && !indexes.isEmpty()) {
      for (Index i : indexes) {
        if (cqlIndex.equals(i.getFieldName()) || cqlIndex.equals(i.getFieldPath())) {
          return true;
        }
      }
    }
    return false;
  }

}
