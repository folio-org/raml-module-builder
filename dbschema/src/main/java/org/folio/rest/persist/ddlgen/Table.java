package org.folio.rest.persist.ddlgen;

import java.util.List;

/**
 * @author shale
 *
 */
public class Table extends Versioned {

  static final String PK_COLUMN_NAME = "id";

  private String mode;
  private String tableName;
  private boolean withMetadata;
  private boolean withAuditing;
  private List<Index> likeIndex;
  private List<Index> uniqueIndex;
  private List<Index> index;
  private List<Index> ginIndex;
  private List<Index> fullTextIndex;
  private List<ForeignKeys> foreignKeys;
  private String customSnippetPath;
  private List<AddFields> addFields;
  private List<DeleteFields> deleteFields;
  private AuditingSnippet auditingSnippet;
  private String auditingTableName;
  private String auditingFieldName;

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public boolean isWithMetadata() {
    return withMetadata;
  }

  public void setWithMetadata(boolean withMetadata) {
    this.withMetadata = withMetadata;
  }

  public boolean isWithAuditing() {
    return withAuditing;
  }

  public void setWithAuditing(boolean withAuditing) {
    this.withAuditing = withAuditing;
  }

  public List<ForeignKeys> getForeignKeys() {
    return foreignKeys;
  }

  public void setForeignKeys(List<ForeignKeys> foreignKeys) {
    this.foreignKeys = foreignKeys;
  }

  public String getCustomSnippetPath() {
    return customSnippetPath;
  }

  public void setCustomSnippetPath(String customSnippetPath) {
    this.customSnippetPath = customSnippetPath;
  }

  public List<Index> getLikeIndex() {
    return likeIndex;
  }

  public void setLikeIndex(List<Index> likeIndex) {
    this.likeIndex = likeIndex;
  }

  public List<Index> getUniqueIndex() {
    return uniqueIndex;
  }

  public void setUniqueIndex(List<Index> uniqueIndex) {
    this.uniqueIndex = uniqueIndex;
  }

  public List<Index> getIndex() {
    return index;
  }

  public void setIndex(List<Index> index) {
    this.index = index;
  }

  public String getMode() {
    return mode;
  }

  public void setMode(String mode) {
    this.mode = mode;
  }

  public List<AddFields> getAddFields() {
    return addFields;
  }

  public void setAddFields(List<AddFields> addFields) {
    this.addFields = addFields;
  }

  public List<DeleteFields> getDeleteFields() {
    return deleteFields;
  }

  public void setDeleteFields(List<DeleteFields> deleteFields) {
    this.deleteFields = deleteFields;
  }

  public AuditingSnippet getAuditingSnippet() {
    return auditingSnippet;
  }

  public void setAuditingSnippet(AuditingSnippet auditingSnippet) {
    this.auditingSnippet = auditingSnippet;
  }

  /**
   * @return name of the table that contains the audit log
   */
  public String getAuditingTableName() {
    return auditingTableName;
  }

  /**
   * @param auditingTableName name of the table that contains the audit log
   */
  public void setAuditingTableName(String auditingTableName) {
    this.auditingTableName = auditingTableName;
  }

  /**
   * @return name of the property of the audit JSON that contains the original record
   */
  public String getAuditingFieldName() {
    return auditingFieldName;
  }

  /**
   * @param auditingFieldName name of the property of the audit JSON that contains the original record
   */
  public void setAuditingFieldName(String auditingFieldName) {
    this.auditingFieldName = auditingFieldName;
  }

  /**
   * Name of the primary key field. This is no longer configurable and is always "id".
   * A basic table has these two fields: id UUID PRIMARY KEY, jsonb JSONB NOT NULL.
   */
  public String getPkColumnName() {
    return PK_COLUMN_NAME;
  }

  public List<Index> getGinIndex() {
    return ginIndex;
  }

  public void setGinIndex(List<Index> ginIndex) {
    this.ginIndex = ginIndex;
  }

  public List<Index> getFullTextIndex() {
    return fullTextIndex;
  }

  public void setFullTextIndex(List<Index> fullTextIndex) {
    this.fullTextIndex = fullTextIndex;
  }

}
