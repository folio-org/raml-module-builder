package org.folio.dbschema;

/**
 * @author shale
 *
 */
public class Field {

  //path to field, jsonb->'aaa'->>'bbb'
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected String fieldPath;
  //name of field - used to generate user friendly name, ex. bbb,
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected String fieldName;
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected TableOperation tOps = TableOperation.ADD;

  public String getFieldName() {
    return fieldName;
  }

  /**
   * Set the field name, for example setFieldName("status.name").
   * @throws IllegalArgumentException if fieldName length exceeds 49.
   */
  public void setFieldName(String fieldName) {
    if (fieldName.length() > 49) {
      throw new IllegalArgumentException("Maximum fieldName length is 49: " + fieldName);
    }
    this.fieldName = fieldName;
  }

  public TableOperation gettOps() {
    return tOps;
  }

  public void settOps(TableOperation tOps) {
    this.tOps = tOps;
  }

  public String getFieldPath() {
    return fieldPath;
  }

  public void setFieldPath(String fieldPath) {
    this.fieldPath = fieldPath;
  }

  /**
   * Set fieldPath using fieldName
   */
  public void setup() {
    setFieldPath(convertDotPath2PostgresMutateNotation(getFieldName()));
  }

  static String convertDotPath2PostgresMutateNotation(String path){
    String []pathParts = path.split("\\.");
    StringBuilder sb = new StringBuilder("'{");
    for (int j = 0; j < pathParts.length; j++) {
      sb.append(pathParts[j]);
      if(j != pathParts.length-1){
        sb.append(",");
      }
    }
    return sb.append("}'").toString();
  }

  /**
   * Convert JSON dot path to PostgreSQL notation. By default string type index will be
   * wrapped with lower/f_unaccent functions except full text index <code>(isFtIndex = true)</code>.
   * Full text index uses to_tsvector to normalize token, so no need of lower/f_unaccent.
   */
  static String convertDotPath2PostgresNotation(String prefix,
    String path, boolean stringType, Index index, boolean isFullText){
    //when an index is on multiple columns, this will be defined something like "username,type"
    //so split on command and build a path for each and then combine
    String []requestIndexPath = path.split(",");
    StringBuilder finalClause = new StringBuilder();
    for (int i = 0; i < requestIndexPath.length; i++) {
      if (finalClause.length() > 0) {
        if (isFullText) {
          finalClause.append(" || ' ' || ");
        } else {
          finalClause.append(" , ");
        }
      }
      //generate index based on paths - note that all indexes will be with a -> to allow
      //postgres to treat the different data types differently and not ->> which would be all
      //strings
      String []pathParts = requestIndexPath[i].trim().split("\\.");
      String prefixString = "jsonb";
      if(prefix != null) {
        prefixString = prefix +".jsonb";
      }
      StringBuilder sb = new StringBuilder(prefixString);
      for (int j = 0; j < pathParts.length; j++) {
        if (j == pathParts.length-1 && stringType) {
          sb.append("->>");
        } else{
          sb.append("->");
        }
        sb.append("'").append(pathParts[j]).append("'");
      }
      boolean added = false;
      if (index != null && stringType) {
        if (index.isRemoveAccents()) {
          sb.insert(0, "f_unaccent(").append(")");
          added = true;
        }
        if (!index.isCaseSensitive()) {
          sb.insert(0, "lower(").append(")");
          added = true;
        }
      }
      if (!added) {
        //need to wrap path expression in () if lower / unaccent isnt
        //appended to the path
        sb.insert(0, "(").append(")");
      }
      finalClause.append(sb.toString());
    }
    return finalClause.toString();
  }

  static String normalizeFieldName(String path) {
    return path.replace('.', '_').replace(',', '_').replace(" ", "");
  }
}
