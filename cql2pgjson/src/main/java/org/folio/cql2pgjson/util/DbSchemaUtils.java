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
   * @param tableName
   * @param indexName
   * @return
   */
  public static DbIndex getDbIndex(Schema schema, String tableNameAndField, String indexName) {
    DbIndex dbIndexStatus = new DbIndex();

    if (tableNameAndField != null && schema.getTables() != null) {
      // remove .jsonb or similar suffix
      final String tableName = tableNameAndField.replaceAll("\\.[^.]+$", "");

      for (Table table : schema.getTables()) {
        if (table.getTableName().equalsIgnoreCase(tableName)) {
          dbIndexStatus.setFt(checkDbIndex(indexName, table.getFullTextIndex()));
          Index fulltextIndex = getDbIndex(indexName, table.getFullTextIndex());
          if (fulltextIndex != null) {
            dbIndexStatus.setModifiers(fulltextIndex.getModifiers());
          }
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
    return getDbIndex(cqlIndex, indexes) != null;
  }

  private static Index getDbIndex(String cqlIndex, List<Index> indexes) {
    if (indexes != null) {
      for (Index i : indexes) {
        if (cqlIndex.equals(i.getFieldName())) {
          return i;
        }
      }
    }
    return null;
  }

}
