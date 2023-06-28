package org.folio.dbschema;

import java.util.List;

import org.folio.dbschema.util.SqlUtil;

/**
 * A foreign key relation from the current table (= child table) to a target table (=parent table).
 */
public class ForeignKeys extends Field {

  private String targetTable;
  private String targetTableAlias;
  private String tableAlias;
  private List<String> targetPath;

  public ForeignKeys() {
    super();
  }

  public ForeignKeys(String fieldPath, String targetTable) {
    super();
    super.fieldPath = fieldPath;
    setTargetTable(targetTable);
  }

  public ForeignKeys(String fieldPath, String targetTable, TableOperation tOps) {
    super();
    super.fieldPath = fieldPath;
    this.tOps = tOps;
    setTargetTable(targetTable);
  }

  /**
   * @return the name ("tableName") of the parent table
   */
  public String getTargetTable() {
    return targetTable;
  }

  /**
   * @param targetTable the name ("tableName") of the parent table
   * @throws IllegalArgumentException on invalid targetTable, see {@link SqlUtil#validateSqlIdentifier(String)}
   */
  public void setTargetTable(String targetTable) {
    SqlUtil.validateSqlIdentifier(targetTable);
    this.targetTable = targetTable;
  }

  /**
   * Get the alias name of the parent/target table to be used in CQL queries,
   * it can differ from the database parent/target table name, for example
   * targetTable="holdings_records", targetTableAlias="holdingsRecords".
   * Null indicates that using this child->parent foreign key relation is disabled for CQL queries.
   * @return the alias name, or null if disabled.
   */
  public String getTargetTableAlias() {
    return targetTableAlias;
  }

  /**
   * Set the alias name of the parent/target table to be used in CQL queries,
   * it can differ from the database parent/target table name, for example
   * targetTable="holdings_records", targetTableAlias="holdingsRecords".
   * Null indicates that using this child->parent foreign key relation is disabled for CQL queries.
   * @param targetTableAlias the alias name, or null if disabled
   */
  public void setTargetTableAlias(String targetTableAlias) {
    this.targetTableAlias = targetTableAlias;
  }

  /**
   * Get the alias name of the current child table to be used in CQL queries to access
   * it from the parent/target table (access holdings records from instance),
   * the tableAlias can differ from the database child table name, for example
   * tableName="holdings_records", tableAlias="holdingsRecords".
   * Null indicates that using this parent->child foreign key relation is disabled for CQL queries.
   * @return the alias name, or null if disabled.
   */
  public String getTableAlias() {
    return tableAlias;
  }

  /**
   * Set the alias name of the current child table to be used in CQL queries to access
   * it from the parent/target table (access holdings records from instance),
   * the tableAlias can differ from the database child table name, for example
   * tableName="holdings_records", tableAlias="holdingsRecords".
   * Null indicates that using this parent->child foreign key relation is disabled for CQL queries.
   */
  public void setTableAlias(String tableAlias) {
    this.tableAlias = tableAlias;
  }

  /**
   * Get the list of fieldNames for a join of more than two tables.
   * @return list of fieldNames, null if this is a two table join using fieldName.
   */
  public List<String> getTargetPath() {
    return targetPath;
  }

  /**
   * Set the list of fieldNames for a join of more than two tables.
   */
  public void setTargetPath(List<String> targetPath) {
    this.targetPath = targetPath;
  }

  /**
   * Set fieldPath using fieldName. Normalize fieldName.
   */
  @Override
  public void setup() {
    if (getFieldName() == null) {  // e.g. "targetPath"
      return;
    }
    //NOTE , FK are created on fields without the lowercasing / unaccenting
    //meaning, there needs to be an index created without lowercasing / unaccenting
    //otherwise no index will be used
    setFieldPath(convertDotPath2PostgresNotation("NEW", getFieldName(), true , null, false));
    setFieldName(normalizeFieldName(getFieldName()));
  }

  @Override
  public String toString() {
    return "ForeignKeys [tableAlias=" + tableAlias + ", targetTable=" + targetTable
        + ", targetTableAlias=" + targetTableAlias
        + ", fieldName=" + fieldName
        + (targetPath == null ? "" : ", targetPath=" + targetPath)
        + ", tOps=" + gettOps()
        + "]";
  }

}
