package org.folio.dbschema;

import java.util.Collections;
import java.util.List;

import org.folio.dbschema.util.SqlUtil;

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
  private OptimisticLockingMode withOptimisticLocking;
  /**
   * indexes using text_pattern_ops
   * @see <a href="https://www.postgresql.org/docs/current/indexes-opclass.html">https://www.postgresql.org/docs/current/indexes-opclass.html</a>
   */
  private List<Index> likeIndex;
  /** unique btree indexes */
  private List<Index> uniqueIndex;
  /** non-unique btree indexes */
  private List<Index> index;
  /**
   * indexes using gin_trgm_ops trigram matching
   * @see <a href="https://www.postgresql.org/docs/current/pgtrgm.html#id-1.11.7.40.7">https://www.postgresql.org/docs/current/pgtrgm.html#id-1.11.7.40.7</a>
   */
  private List<Index> ginIndex;
  /**
   * indexes using tsvector full text search
   * @see <a href="https://www.postgresql.org/docs/current/textsearch-indexes.html">https://www.postgresql.org/docs/current/textsearch-indexes.html</a>
   */
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

  /**
   * @param tableName
   * @throws IllegalArgumentException on invalid tableName, see {@link SqlUtil#validateSqlIdentifier(String)}
   */
  public void setTableName(String tableName) {
    SqlUtil.validateSqlIdentifier(tableName);
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

  /**
   * Return an empty list if l is null, otherwise return l.
   */
  private <T> List<T> list(List<T> l) {
    if (l == null) {
      return Collections.emptyList();
    }
    return l;
  }

  /**
   * Set mode to "new" if null. Set fieldName using FieldName for each field.
   */
  public void setup() {
    if (getMode() == null) {
      //the only relevant mode that the templates take into account is delete
      //otherwise update and new will always create if does not exist
      //so can set to either new or update , doesnt matter, leave the option
      //in case we do need to differentiate in the future between the two
      setMode("new");
    }

    list(getDeleteFields()) .forEach(Field::setup);
    list(getAddFields())    .forEach(Field::setup);
    list(getForeignKeys())  .forEach(ForeignKeys::setup);
    list(getIndex())        .forEach(Index::setupIndex);
    list(getLikeIndex())    .forEach(Index::setupLikeIndex);
    list(getUniqueIndex())  .forEach(Index::setupUniqueIndex);
    list(getGinIndex())     .forEach(Index::setupGinIndex);
    list(getFullTextIndex()).forEach(Index::setupFullTextIndex);
    if (isWithAuditing()) {
      if (getAuditingTableName() == null) {
        throw new IllegalArgumentException(
            "auditingTableName missing for table " + getTableName() + " having \"withAuditing\": true");
      }
      if (getAuditingFieldName() == null) {
        throw new IllegalArgumentException(
            "auditingFieldName missing for table " + getTableName() + " having \"withAuditing\": true");
      }
    }
  }

  public OptimisticLockingMode getWithOptimisticLocking() {
    return withOptimisticLocking;
  }

  public void setWithOptimisticLocking(OptimisticLockingMode withOptimisticLocking) {
    this.withOptimisticLocking = withOptimisticLocking;
  }
}
