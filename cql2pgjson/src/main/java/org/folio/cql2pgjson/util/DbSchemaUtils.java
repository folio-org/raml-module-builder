package org.folio.cql2pgjson.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.folio.cql2pgjson.model.DbFkInfo;
import org.folio.cql2pgjson.model.DbIndex;
import org.folio.dbschema.ForeignKeys;
import org.folio.dbschema.Index;
import org.folio.dbschema.Schema;
import org.folio.dbschema.Table;
import org.folio.dbschema.TableOperation;

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
    return new DbIndex(table, indexName);
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
        if (TableOperation.DELETE == i.gettOps()) {
          continue;
        }
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
   * Return DbFkInfo for the foreign key entry of table for the given fieldName.
   * Throws IllegalStateException if not found.
   */
  private static DbFkInfo getForeignKey(Table table, String fieldName) {
    for (ForeignKeys fk : table.getForeignKeys()) {
      if (fieldName.equals(fk.getFieldName())) {
        return new DbFkInfo(table.getTableName(), fieldName, fk.getTargetTable());
      }
    }
    throw new IllegalStateException("foreignKey not found for table=" + table.getTableName()
      + ", fieldName=" + fieldName);
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

    Table srcTab = getTable(dbSchema, srcTabName);
    if (srcTab == null || srcTab.getForeignKeys() == null) {
      return Collections.emptyList();
    }

    for (ForeignKeys fk : srcTab.getForeignKeys()) {
      if (targetTabAlias.equals(fk.getTargetTableAlias())) {
        return findForeignKeys(dbSchema, srcTab, fk);
      }
    }
    return Collections.emptyList();
  }

  /**
   * Find a list of {@link ForeignKeys} from source table alias to target table.
   *
   * Examples:
   *
   * srcTabAlias=holdingsRecord and targetTabName=item return
   * [(table=item, field=holdingsRecordId, targetTable=holdings_record)].
   *
   * srcTabAlias=instance and targetTabName=item return
   * [(table=item, field=holdingsRecordId, targetTable=holdings_record),
   * (table=holdings_record, field=instanceId, targetTable=instance)].
   *
   *
   * @param dbSchema
   * @param sourceTableAlias
   * @param targetTable
   * @return list; empty if none found
   */
  public static List<DbFkInfo> findForeignKeysFromSourceAliasToTargetTable(Schema dbSchema, String sourceTableAlias,
      String targetTable) {

    for (Table table : dbSchema.getTables()) {
      if (table.getForeignKeys() == null) {
        continue;
      }
      for (ForeignKeys fk : table.getForeignKeys()) {
        if (! sourceTableAlias.equals(fk.getTableAlias()) ||
            ! targetTable.equals(fk.getTargetTable())       ) {
          continue;
        }
        List<DbFkInfo> list = findForeignKeys(dbSchema, table, fk);
        if (! list.isEmpty()) {
          return list;
        }
      }
    }
    return Collections.emptyList();
  }

  /**
   * Find foreign keys info about foreignKeys via its "fieldName" or "targetPath" property.
   */
  private static List<DbFkInfo> findForeignKeys(Schema dbSchema, Table table, ForeignKeys foreignKeys) {
    // join one table with a second table only?
    if (foreignKeys.getFieldName() != null) {
      List<DbFkInfo> list = new ArrayList<>();
      list.add(new DbFkInfo(table.getTableName(), foreignKeys.getFieldName(), foreignKeys.getTargetTable()));
      return list;
    }

    // join with several tables using a path?
    List<String> targetPath = foreignKeys.getTargetPath();
    if (targetPath == null || targetPath.isEmpty()) {
      return Collections.emptyList();
    }

    List<DbFkInfo> list = new ArrayList<>();
    for (String fieldName : targetPath) {
      DbFkInfo dbFkInfo = getForeignKey(table, fieldName);
      list.add(dbFkInfo);
      table = getTable(dbSchema, dbFkInfo.getTargetTable());
      if (table == null) {
        throw new IllegalStateException(
            "table not found for tableName=" + dbFkInfo.getTargetTable() + ", targetPath=" + targetPath);
      }
    }
    return list;
  }
}
