package org.folio.rest.persist.ddlgen;

import java.util.List;

/**
 * @author shale
 *
 */
public class Table extends Versioned {

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
  private boolean populateJsonWithId;
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

  public boolean isPopulateJsonWithId() {
    return populateJsonWithId;
  }

  public void setPopulateJsonWithId(boolean populateJsonWithId) {
    this.populateJsonWithId = populateJsonWithId;
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
