package org.folio.rest.persist.ddlgen;

import java.util.List;

/**
 * @author shale
 *
 */
public class Table {

  private String mode;
  private String tableName;
  private boolean withMetadata;
  private boolean generateId;
  private boolean withAuditing;
  private List<TableIndexes> likeIndex;
  private List<TableIndexes> uniqueIndex;
  private List<TableIndexes> ginIndex;
  private List<ForeignKeys> foreignKeys;
  private String customSnippetPath;
  private List<AddFields> addFields;
  private List<DeleteFields> deleteFields;
  private boolean populateJsonWithId;
  private double fromModuleVersion;
  private AuditingSnippet auditingSnippet;
  private String pkColumnName = "id";

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

  public boolean isGenerateId() {
    return generateId;
  }

  public void setGenerateId(boolean generateId) {
    this.generateId = generateId;
  }

  public String getCustomSnippetPath() {
    return customSnippetPath;
  }

  public void setCustomSnippetPath(String customSnippetPath) {
    this.customSnippetPath = customSnippetPath;
  }

  public List<TableIndexes> getLikeIndex() {
    return likeIndex;
  }

  public void setLikeIndex(List<TableIndexes> likeIndex) {
    this.likeIndex = likeIndex;
  }

  public List<TableIndexes> getUniqueIndex() {
    return uniqueIndex;
  }

  public void setUniqueIndex(List<TableIndexes> uniqueIndex) {
    this.uniqueIndex = uniqueIndex;
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

  public boolean isPopulateJsonWithId() {
    return populateJsonWithId;
  }

  public void setPopulateJsonWithId(boolean populateJsonWithId) {
    this.populateJsonWithId = populateJsonWithId;
  }

  public double getFromModuleVersion() {
    return fromModuleVersion;
  }

  public void setFromModuleVersion(double fromModuleVersion) {
    this.fromModuleVersion = fromModuleVersion;
  }

  public List<TableIndexes> getGinIndex() {
    return ginIndex;
  }

  public void setGinIndex(List<TableIndexes> ginIndex) {
    this.ginIndex = ginIndex;
  }

  public AuditingSnippet getAuditingSnippet() {
    return auditingSnippet;
  }

  public void setAuditingSnippet(AuditingSnippet auditingSnippet) {
    this.auditingSnippet = auditingSnippet;
  }

  public String getPkColumnName() {
    return pkColumnName;
  }

  public void setPkColumnName(String pkColumnName) {
    this.pkColumnName = pkColumnName;
  }

}
