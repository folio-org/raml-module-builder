package org.folio.cql2pgjson.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.folio.cql2pgjson.model.DbFkInfo;
import org.folio.cql2pgjson.model.DbIndex;
import org.folio.rest.persist.ddlgen.ForeignKeys;
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

  /**
   * Get {@link Index} for given cqlIndex name.
   *
   * @param cqlIndex
   * @param indexes
   * @return
   */
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

  /**
   * Get {@link Table} for given table name in {@link Schema}
   *
   * @param dbSchema
   * @param tableName
   * @return table; null if not found
   */
  public static Table getTable(Schema dbSchema, String tableName) {
    for (Table table : dbSchema.getTables()) {
      if (tableName.equals(table.getTableName())) {
        return table;
      }
    }
    return null;
  }

  /**
   * Find a list of {@link ForeignKeys} from source table to target table alias
   *
   * @param dbSchema
   * @param srcTabName
   * @param targetTabAlias
   *
   * @return a list of {@link ForeignKeys}; empty if none found
   */
  public static List<DbFkInfo> findForeignKeysFromSourceTableToTargetAlias(Schema dbSchema, String srcTabName,
      String targetTabAlias) {
    return findForeignKeys(dbSchema, srcTabName, targetTabAlias, true);
  }

  private static List<DbFkInfo> findForeignKeys(Schema dbSchema, String srcTabName, String targetTabName,
      boolean useTargetAlias) {
    List<DbFkInfo> list = new ArrayList<>();
    Table srcTab = getTable(dbSchema, srcTabName);
    if (srcTab == null || srcTab.getForeignKeys() == null) {
      return list;
    }
    // direct FK
    for (ForeignKeys fk : srcTab.getForeignKeys()) {
      String targetName = useTargetAlias ? fk.getTargetTableAlias() : fk.getTargetTable();
      if (targetTabName.equals(targetName)) {
        list.add(new DbFkInfo(srcTab.getTableName(), fk.getFieldName(), fk.getTargetTable()));
        return list;
      }
    }
    // find the shortest path
    for (ForeignKeys fk : srcTab.getForeignKeys()) {
      updateFkList(list, dbSchema, srcTab, fk, targetTabName, useTargetAlias);
    }
    return list;
  }

  private static void updateFkList(List<DbFkInfo> list, Schema dbSchema, Table srcTab, ForeignKeys fk,
      String targetTabName, boolean useTargetAlias) {
    List<DbFkInfo> childList = findForeignKeys(dbSchema, fk.getTargetTable(), targetTabName, useTargetAlias);
    if (!childList.isEmpty()) {
      if (!list.isEmpty() && (list.size() > (childList.size() + 1))) {
        list.clear();
      }
      list.add(new DbFkInfo(srcTab.getTableName(), fk.getFieldName(), fk.getTargetTable()));
      list.addAll(childList);
    }
  }

  /**
   * Find a list of {@link ForeignKeys} from source table alias and target table.
   *
   * @param dbSchema
   * @param srcTabAlias
   * @param targetTabName
   * @return list; empty if none found
   */
  public static List<DbFkInfo> findForeignKeysFromSourceAliasToTargetTable(Schema dbSchema, String srcTabAlias,
      String targetTabName) {
    List<DbFkInfo> list = new ArrayList<>();
    for (Table table : dbSchema.getTables()) {
      if (table.getForeignKeys() == null) {
        continue;
      }
      for (ForeignKeys fk : table.getForeignKeys()) {
        String tabAlias = fk.getTableAlias() == null ? table.getTableName() : fk.getTableAlias();
        if (srcTabAlias.equals(tabAlias)) {
          // direct FK
          if (targetTabName.equals(fk.getTargetTable())) {
            list.add(new DbFkInfo(table.getTableName(), fk.getFieldName(), fk.getTargetTable()));
            return list;
          } else {
            // find the shortest path
            updateFkList(list, dbSchema, table, fk, targetTabName, false);
          }
        }
      }
    }
    return list;
  }
}
