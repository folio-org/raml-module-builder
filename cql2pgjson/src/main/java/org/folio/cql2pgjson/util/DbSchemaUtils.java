package org.folio.cql2pgjson.util;

import java.util.Arrays;
import java.util.List;

import org.folio.cql2pgjson.model.DbIndex;
import org.folio.rest.persist.ddlgen.Index;
import org.folio.rest.persist.ddlgen.Table;

/**
 * Help method to extract info from RMB db schema.json
 */
public class DbSchemaUtils {

  private DbSchemaUtils() {
  }

  /**
   * Get index info for some table
   *
   * @param table
   * @param indexName
   * @return
   */
  public static DbIndex getDbIndex(Table table, String indexName) {
    DbIndex dbIndexStatus = new DbIndex();

    if (table != null) {
      dbIndexStatus.setFt(checkDbIndex(indexName, table.getFullTextIndex()));
      Index fulltextIndex = getIndex(indexName, table.getFullTextIndex());
      dbIndexStatus.setGin(checkDbIndex(indexName, table.getGinIndex()));
      for (List<Index> index : Arrays.asList(table.getIndex(),
        table.getUniqueIndex(), table.getLikeIndex())) {
        dbIndexStatus.setOther(checkDbIndex(indexName, index));
        if (dbIndexStatus.isOther()) {
          break;
        }
      }
    }
    return dbIndexStatus;
  }

  private static boolean checkDbIndex(String cqlIndex, List<Index> indexes) {
    return getIndex(cqlIndex, indexes) != null;
  }

  public static Index getIndex(String cqlIndex, List<Index> indexes) {
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
