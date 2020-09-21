package org.folio.cql2pgjson.model;

import java.util.List;
import java.util.function.Function;

import org.folio.cql2pgjson.util.DbSchemaUtils;
import org.folio.dbschema.ForeignKeys;
import org.folio.dbschema.Index;
import org.folio.dbschema.Table;

/**
 * Stores index information to avoid iterating through the table's index lists repeatedly.
 */
public class DbIndex {
  /**
   * index using text_pattern_ops
   * @see <a href="https://www.postgresql.org/docs/current/indexes-opclass.html">https://www.postgresql.org/docs/current/indexes-opclass.html</a>
   */
  private final Index likeIndex;
  /** unique btree index */
  private final Index uniqueIndex;
  /** non-unique btree index */
  private final Index index;
  /**
   * index using gin_trgm_ops trigram matching
   * @see <a href="https://www.postgresql.org/docs/current/pgtrgm.html#id-1.11.7.40.7">https://www.postgresql.org/docs/current/pgtrgm.html#id-1.11.7.40.7</a>
   */
  private final Index ginIndex;
  /**
   * index using tsvector full text search
   * @see <a href="https://www.postgresql.org/docs/current/textsearch-tables.html#TEXTSEARCH-TABLES-INDEX">https://www.postgresql.org/docs/current/textsearch-tables.html#TEXTSEARCH-TABLES-INDEX</a>
   */
  private final Index fullTextIndex;
  private final ForeignKeys foreignKeys;

  /**
   * @param table  where to get the index information from
   * @param indexName  the name of the field that is indexed
   */
  public DbIndex(Table table, String indexName) {
    likeIndex     = DbSchemaUtils.getIndex(indexName, get(table, Table::getLikeIndex));
    uniqueIndex   = DbSchemaUtils.getIndex(indexName, get(table, Table::getUniqueIndex));
    index         = DbSchemaUtils.getIndex(indexName, get(table, Table::getIndex));
    ginIndex      = DbSchemaUtils.getIndex(indexName, get(table, Table::getGinIndex));
    fullTextIndex = DbSchemaUtils.getIndex(indexName, get(table, Table::getFullTextIndex));
    foreignKeys   = findForeignKey        (indexName, get(table, Table::getForeignKeys));
  }

  private static <U> U get(Table table, Function<Table, U> function) {
    if (table == null) {
      return null;
    }
    return function.apply(table);
  }

  private static ForeignKeys findForeignKey(String indexName, List<ForeignKeys> foreignKeysList) {
    if (foreignKeysList == null) {
      return null;
    }
    for (ForeignKeys foreignKeys : foreignKeysList) {
      if (indexName.equals(foreignKeys.getFieldName())) {
        return foreignKeys;
      }
    }
    return null;
  }

  /**
   * @return the index using text_pattern_ops, or null if no such index exists
   * @see <a href="https://www.postgresql.org/docs/current/indexes-opclass.html">https://www.postgresql.org/docs/current/indexes-opclass.html</a>
   */
  public Index getLikeIndex() {
    return likeIndex;
  }

  /** @return the unique btree index, or null if no such index exists */
  public Index getUniqueIndex() {
    return uniqueIndex;
  }

  /** @return the non-unique btree index, or null if no such index exists */
  public Index getIndex() {
    return index;
  }

  /**
   * @return index using gin_trgm_ops trigram matching, or null if no such index exists
   * @see <a href="https://www.postgresql.org/docs/current/pgtrgm.html#id-1.11.7.40.7">https://www.postgresql.org/docs/current/pgtrgm.html#id-1.11.7.40.7</a>
   */
  public Index getGinIndex() {
    return ginIndex;
  }

  /**
   * @return index using tsvector full text search, or null if no such index exists
   * @see <a href="https://www.postgresql.org/docs/current/textsearch-tables.html#TEXTSEARCH-TABLES-INDEX">https://www.postgresql.org/docs/current/textsearch-tables.html#TEXTSEARCH-TABLES-INDEX</a>
   */
  public Index getFullTextIndex() {
    return fullTextIndex;
  }

  /**
   * @return the foreign key information
   */
  public ForeignKeys getForeignKeys() {
    return foreignKeys;
  }

  /**
   * @return whether an index using text_pattern_ops exists
   * @see <a href="https://www.postgresql.org/docs/current/indexes-opclass.html">https://www.postgresql.org/docs/current/indexes-opclass.html</a>
   */
  public boolean hasLikeIndex() {
    return likeIndex != null;
  }

  /** @return whether a unique btree index exists */
  public boolean hasUniqueIndex() {
    return uniqueIndex != null;
  }

  /** @return whether a non-unique btree index exists */
  public boolean hasIndex() {
    return index != null;
  }

  /**
   * @return whether an index using gin_trgm_ops trigram matching exists
   * @see <a href="https://www.postgresql.org/docs/current/pgtrgm.html#id-1.11.7.40.7">https://www.postgresql.org/docs/current/pgtrgm.html#id-1.11.7.40.7</a>
   */
  public boolean hasGinIndex() {
    return ginIndex != null;
  }

  /**
   * @return whether an index using tsvector full text search exists
   * @see <a href="https://www.postgresql.org/docs/current/textsearch-indexes.html">https://www.postgresql.org/docs/current/textsearch-indexes.html</a>
   */
  public boolean hasFullTextIndex() {
    return fullTextIndex != null;
  }

  /**
   * @return whether the field is a foreign key
   */
  public boolean isForeignKey() {
    return foreignKeys != null;
  }
}
