package org.folio.dbschema;

import java.util.List;

/**
 * @author shale
 *
 */
public class ViewTable {

  private String tableName;
  private String joinOnField;
  private boolean indexUsesCaseSensitive = false; //right now cql generates everything with case insensitive
  private boolean indexUsesRemoveAccents = true;  //and remove accents, when that changes, switch back defaults
  private String prefix;

  //needed for the join table, since we can not join two tables
  //with two columns named the same, so the join table / or the root table
  //need to alias the jsonb column to another name, so that when the result set comes in
  //it will contain columns - id, jsonb, jsonb_alias
  private String jsonFieldAlias = "jsonb";

  public String getTableName() {
    return tableName;
  }
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  public String getJoinOnField() {
    return joinOnField;
  }
  public void setJoinOnField(String joinOnField) {
    this.joinOnField = joinOnField;
  }
  public String getJsonFieldAlias() {
    return jsonFieldAlias;
  }
  public void setJsonFieldAlias(String jsonFieldAlias) {
    this.jsonFieldAlias = jsonFieldAlias;
  }
  public boolean isIndexUsesCaseSensitive() {
    return indexUsesCaseSensitive;
  }
  public void setIndexUsesCaseSensitive(boolean indexUsesCaseSensitive) {
    this.indexUsesCaseSensitive = indexUsesCaseSensitive;
  }
  public boolean isIndexUsesRemoveAccents() {
    return indexUsesRemoveAccents;
  }
  public void setIndexUsesRemoveAccents(boolean indexUsesRemoveAccents) {
    this.indexUsesRemoveAccents = indexUsesRemoveAccents;
  }
  public String getPrefix() {
    return prefix;
  }
  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  void setup(List<Table> tables) {
    setPrefix(getTableName());
    Index index = getIndex(tables);
    setJoinOnField(Field.convertDotPath2PostgresNotation(getPrefix(),
      getJoinOnField(), true, index, false));
    if (index != null) {
      //when creating the join on condition, we want to create it the same way as we created the index
      //so that the index will get used, for example:
      //ON lower(f_unaccent(instance.jsonb->>'id'::text))=lower(f_unaccent(holdings_record.jsonb->>'instanceId'))
      setIndexUsesCaseSensitive(index.isCaseSensitive());
      setIndexUsesRemoveAccents(index.isRemoveAccents());
    }
  }

  Index getIndex(List<Table> tables) {
    for (Table table : tables) {
      if (! table.getTableName().equals(getTableName())) {
        continue;
      }

      String normalizedFieldName = Field.normalizeFieldName(getJoinOnField());
      Index index = getIndex(normalizedFieldName, table.getFullTextIndex());
      if (index != null) {
        return index;
      }
      index = getIndex(normalizedFieldName, table.getUniqueIndex());
      if (index != null) {
        return index;
      }
      index = getIndex(normalizedFieldName, table.getIndex());
      if (index != null) {
        return index;
      }
      return null;
    }
    return null;
  }

  Index getIndex(String normalizedFieldName, List<Index> list) {
    if (list == null) {
      return null;
    }
    for (Index index : list) {
      if (normalizedFieldName.equals(Field.normalizeFieldName(index.getFieldName())) ) {
        return index;
      }
    }
    return null;
  }
}
