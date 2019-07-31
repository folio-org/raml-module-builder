package org.folio.cql2pgjson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.folio.cql2pgjson.exception.CQLFeatureUnsupportedException;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.cql2pgjson.exception.ServerChoiceIndexesException;
import org.folio.cql2pgjson.model.CqlAccents;
import org.folio.cql2pgjson.model.CqlCase;
import org.folio.cql2pgjson.model.CqlModifiers;
import org.folio.cql2pgjson.model.CqlSort;
import org.folio.cql2pgjson.model.CqlTermFormat;
import org.folio.cql2pgjson.model.DbIndex;
import org.folio.cql2pgjson.model.IndexTextAndJsonValues;
import org.folio.cql2pgjson.model.SqlSelect;
import org.folio.cql2pgjson.util.Cql2SqlUtil;
import org.folio.cql2pgjson.util.DbSchemaUtils;
import org.folio.rest.persist.ddlgen.ForeignKeys;
import org.folio.rest.persist.ddlgen.Index;
import org.folio.rest.persist.ddlgen.Schema;
import org.folio.rest.persist.ddlgen.Table;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.util.ResourceUtil;
import org.z3950.zing.cql.CQLAndNode;
import org.z3950.zing.cql.CQLBooleanNode;
import org.z3950.zing.cql.CQLNode;
import org.z3950.zing.cql.CQLNotNode;
import org.z3950.zing.cql.CQLOrNode;
import org.z3950.zing.cql.CQLParseException;
import org.z3950.zing.cql.CQLParser;
import org.z3950.zing.cql.CQLSortNode;
import org.z3950.zing.cql.CQLTermNode;
import org.z3950.zing.cql.Modifier;
import org.z3950.zing.cql.ModifierSet;

/**
 * Convert a CQL query into a PostgreSQL JSONB SQL query.
 * <p>
 * Contextual Query Language (CQL) Specification:
 * <a href="https://www.loc.gov/standards/sru/cql/spec.html">https://www.loc.gov/standards/sru/cql/spec.html</a>
 * <p>
 * JSONB in PostgreSQL:
 * <a href="https://www.postgresql.org/docs/current/static/datatype-json.html">https://www.postgresql.org/docs/current/static/datatype-json.html</a>
 */
public class CQL2PgJSON {

  /**
   * Name of the JSON field, may include schema and table name (e.g. tenant1.user_table.json).
   * Must conform to SQL identifier requirements (characters, not a keyword), or properly
   * quoted using double quotes.
   */
  private static Logger logger = Logger.getLogger(CQL2PgJSON.class.getName());

  /** name of the primary key column */
  private static final String PK_COLUMN_NAME = "id";

  private final Pattern uuidPattern = Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

  private String jsonField = null;
  private List<String> jsonFields = null;

  // leverage RMB and consider to merge cql2pgjson into RMB
  private Schema dbSchema;
  private Table dbTable;

  /**
   * Default index names to be used for cql.serverChoice.
   * May be empty, but not null. Must not contain null, names must not contain double quote or single quote.
   */
  private List<String> serverChoiceIndexes = Collections.emptyList();

  /**
   * Create an instance for the specified schema.
   *
   * @param field Name of the JSON field, may include schema and table name (e.g. tenant1.user_table.json).
   *   Must conform to SQL identifier requirements (characters, not a keyword), or properly
   *   quoted using double quotes.
   * @throws FieldException provided field is not valid
   */
  public CQL2PgJSON(String field) throws FieldException {
    doInit(field, null);
    initDbTable();
  }

  /**
   * Create an instance for the specified schema.
   *
   * @param field Name of the JSON field, may include schema and table name (e.g. tenant1.user_table.json).
   *   Must conform to SQL identifier requirements (characters, not a keyword), or properly
   *   quoted using double quotes.
   * @param serverChoiceIndexes       List of field names, may be empty, must not contain null,
   *                                  names must not contain double quote or single quote.
   * @throws FieldException (subclass of CQL2PgJSONException) - provided field is not valid
   * @throws ServerChoiceIndexesException (subclass of CQL2PgJSONException) - provided serverChoiceIndexes is not valid
   */
  public CQL2PgJSON(String field, List<String> serverChoiceIndexes) throws FieldException, ServerChoiceIndexesException {
    this(field);
    setServerChoiceIndexes(serverChoiceIndexes);
  }

  /**
   * Create an instance for the specified list of schemas. If only one field name is provided, queries will
   * default to the handling of single field queries.
   *
   * @param fields Field names of the JSON fields, may include schema and table name (e.g. tenant1.user_table.json).
   *  Must conform to SQL identifier requirements (characters, not a keyword), or properly quoted using double quotes.
   *  The first field name on the list will be the default field for terms in queries that don't specify a json field.
   * @throws FieldException (subclass of CQL2PgJSONException) - provided field is not valid
   */
  public CQL2PgJSON(List<String> fields) throws FieldException {
    loadDbSchema(null);
    if (fields == null || fields.isEmpty())
      throw new FieldException( "fields list must not be empty" );
    this.jsonFields = new ArrayList<>();
    for (String field : fields) {
      this.jsonFields.add(trimNotEmpty(field));
    }
    if (this.jsonFields.size() == 1) {
      this.jsonField = this.jsonFields.get(0);
    }
    initDbTable();
  }
  /**
   * Create an instance for the specified list of schemas. If only one field name is provided, queries will
   * default to the handling of single field queries.
   *
   * @param fields Field names of the JSON fields, may include schema and table name (e.g. tenant1.user_table.json).
   *  Must conform to SQL identifier requirements (characters, not a keyword), or properly quoted using double quotes.
   *  The first field name on the list will be the default field for terms in queries that don't specify a json field.
   * @param viewTarget the table that this is actually targetting since views hide the fact of the original table.  This is used to determine how to find indexes and other table specific info thats lost. 
   * @throws FieldException (subclass of CQL2PgJSONException) - provided field is not valid
   */
  public CQL2PgJSON(List<String> fields, String viewTarget) throws FieldException {
    loadDbSchema(null);
    if (fields == null || fields.isEmpty())
      throw new FieldException( "fields list must not be empty" );
    this.jsonFields = new ArrayList<>();
    for (String field : fields) {
      this.jsonFields.add(trimNotEmpty(field));
    }
    if (viewTarget!= null) {
      this.jsonField = viewTarget;
    }
    initDbTable();
  }
  /**
   * Create an instance for the specified list of schemas. If only one field name is provided, queries will
   * default to the handling of single field queries.
   *
   * @param fields Field names of the JSON fields, may include schema and table name (e.g. tenant1.user_table.json).
   *  Must conform to SQL identifier requirements (characters, not a keyword), or properly quoted using double quotes.
   *  The first field name on the list will be the default field for terms in queries that don't specify a json field.
   * @param serverChoiceIndexes  List of field names, may be empty, must not contain null,
   *                             names must not contain double quote or single quote and must identify the jsonb
   *                             field to which they apply. (e.g. "group_jsonb.patronGroup.group" )
   * @throws FieldException (subclass of CQL2PgJSONException) - provided field is not valid
   * @throws ServerChoiceIndexesException (subclass of CQL2PgJSONException) - provided serverChoiceIndexes is not valid
   */
  public CQL2PgJSON(List<String> fields, List<String> serverChoiceIndexes)
      throws ServerChoiceIndexesException, FieldException {
    this(fields);
    setServerChoiceIndexes(serverChoiceIndexes);
  }

  public String getjsonField() {
    return jsonField;
  }

  public Schema getDbSchema() {
    return dbSchema;
  }

  public void setDbSchemaPath(String dbSchemaPath) {
    loadDbSchema(dbSchemaPath);
    initDbTable();
  }

  private void loadDbSchema(String schemaPath) {
    try {
      if (schemaPath == null) {
        schemaPath = "templates/db_scripts/schema.json";
      }
      String dbJson = ResourceUtil.asString(schemaPath, CQL2PgJSON.class);
      logger.log(Level.INFO, "loadDbSchema: Loaded " + schemaPath + " OK");
      dbSchema = ObjectMapperTool.getMapper().readValue(dbJson, org.folio.rest.persist.ddlgen.Schema.class);
    } catch (IOException ex) {
      logger.log(Level.SEVERE, "No schema.json found", ex);
    }
  }

  private void initDbTable() {
    if (dbSchema.getTables() != null) {
      if (jsonField == null) {
        logger.log(Level.SEVERE, "loadDbSchema(): No primary table name, can not load");
        return;
      }
      // Remove the json blob field name, usually ".jsonb", but in tests also
      // ".user_data" etc.
      String tname = this.jsonField.replaceAll("\\.[^.]+$", "");
      for (Table table : dbSchema.getTables()) {
        if (tname.equalsIgnoreCase(table.getTableName())) {
          dbTable = table;
          break;
        }
      }
      if (dbTable == null) {
        logger.log(Level.SEVERE, "loadDbSchema loadDbSchema(): Table {0} NOT FOUND", tname);
      }
    } else {
      logger.log(Level.SEVERE, "loadDbSchema loadDbSchema(): No 'tables' section found");
    }
  }

  private void doInit(String field, String dbSchemaPath) throws FieldException {
    jsonField = trimNotEmpty(field);
    loadDbSchema(dbSchemaPath);
  }

  /**
   * Set the index names (field names) for cql.serverChoice.
   * @param serverChoiceIndexes       List of field names, may be empty, must not contain null,
   *                                  names must not contain double quote or single quote.
   * @throws ServerChoiceIndexesException if serverChoiceIndexes value(s) are invalid
   */
  public void setServerChoiceIndexes(List<String> serverChoiceIndexes) throws ServerChoiceIndexesException {
    if (serverChoiceIndexes == null) {
      this.serverChoiceIndexes = Collections.emptyList();
      return;
    }
    for (String field : serverChoiceIndexes) {
      if (field == null) {
        throw new ServerChoiceIndexesException("serverChoiceFields must not contain null elements");
      }
      if (field.trim().isEmpty()) {
        throw new ServerChoiceIndexesException("serverChoiceFields must not contain empty field names");
      }
      int pos = field.indexOf('"');
      if (pos >= 0) {
        throw new ServerChoiceIndexesException("field contains double quote at position " + pos+1 + ": " + field);
      }
      pos = field.indexOf('\'');
      if (pos >= 0) {
        throw new ServerChoiceIndexesException("field contains single quote at position " + pos+1 + ": " + field);
      }
    }
    this.serverChoiceIndexes = serverChoiceIndexes;
  }

  /**
   * Return field.trim(). Throw FieldException if field is null or
   * field.trim() is empty.
   *
   * @param field  the field name to trim
   * @return trimmed field
   * @throws FieldException  if field is null or the trimmed field name is empty
   */
  private String trimNotEmpty(String field) throws FieldException {
    if (field == null) {
      throw new FieldException("a field name must not be null");
    }
    String fieldTrimmed = field.trim();
    if (fieldTrimmed.isEmpty()) {
      throw new FieldException("a field name must not be empty");
    }
    return fieldTrimmed;
  }

  /**
   * Return an SQL WHERE clause for the CQL expression.
   * @param cql  CQL expression to convert
   * @return SQL WHERE clause, without leading "WHERE ", may contain "ORDER BY" clause
   * @throws QueryValidationException  when parsing or validating cql fails
   */
  public String cql2pgJson(String cql) throws QueryValidationException {
    try {
      CQLParser parser = new CQLParser();
      CQLNode node = parser.parse(cql);
      return pg(node);
    } catch (IOException|CQLParseException e) {
      throw new QueryValidationException(e);
    }
  }

  /**
   * Convert the CQL query into a SQL query and return the WHERE and the ORDER BY clause.
   * @param cql  the query to convert
   * @return SQL query
   * @throws QueryValidationException
   */
  public SqlSelect toSql(String cql) throws QueryValidationException {
    try {
      CQLParser parser = new CQLParser();
      CQLNode node = parser.parse(cql);
      return toSql(node);
    } catch (IOException|CQLParseException e) {
      throw new QueryValidationException(e);
    }
  }

  private SqlSelect toSql(CQLNode node) throws QueryValidationException {
    if (node instanceof CQLSortNode) {
      return toSql((CQLSortNode) node);
    }
    return new SqlSelect(pg(node), null);
  }

  private String pg(CQLNode node) throws QueryValidationException {
    if (node instanceof CQLTermNode) {
      return pg((CQLTermNode) node);
    }
    if (node instanceof CQLBooleanNode) {
      return pg((CQLBooleanNode) node);
    }
    if (node instanceof CQLSortNode) {
      SqlSelect sqlSelect = toSql((CQLSortNode) node);
      return sqlSelect.getWhere() + " ORDER BY " + sqlSelect.getOrderBy();
    }
    throw createUnsupportedException(node);
  }

  private static CQLFeatureUnsupportedException createUnsupportedException(CQLNode node) {
    return new CQLFeatureUnsupportedException("Not implemented yet: " + node.getClass().getName());
  }

  /**
   * Return "lower(f_unaccent(" + term + "))".
   * @param term  String to wrap
   * @return wrapped term
   */
  private static String wrapInLowerUnaccent(String term) {
    return "lower(f_unaccent(" + term + "))";
  }

  private static String wrapInLowerUnaccent(String term, boolean lower, boolean unaccent) {
    if (lower) {
      if (unaccent) {
        return "lower(f_unaccent(" + term + "))";
      } else {
        return "lower(" + term + ")";
      }
    } else {
      if (unaccent) {
        return "f_unaccent(" + term + ")";
      } else {
        return term;
      }
    }
  }

  /**
   * Return $term, lower($term), f_unaccent($term) or lower(f_unaccent($term))
   * according to the cqlModifiers.  If undefined use CqlAccents.IGNORE_ACCENTS
   * and CqlCase.IGNORE_CASE as default.
   * @param term  the String to wrap
   * @param cqlModifiers  what functions to use
   * @return wrapped term
   */
  private static String wrapInLowerUnaccent(String term, CqlModifiers cqlModifiers) {
    return wrapInLowerUnaccent(term,
        cqlModifiers.getCqlCase() != CqlCase.RESPECT_CASE,
        cqlModifiers.getCqlAccents() != CqlAccents.RESPECT_ACCENTS);
  }

  /**
   * Return $term, lower($term), f_unaccent($term) or lower(f_unaccent($term))
   * according to the modifiers of index.
   * @param term  the String to wrap
   * @param index  where to get the modifiers from
   * @return wrapped term
   */
  private static String wrapInLowerUnaccent(String term, Index index) {
    if(index == null ) {
      return wrapInLowerUnaccent(term, true, true);
    } else {
      return wrapInLowerUnaccent(term, ! index.isCaseSensitive(), index.isRemoveAccents());
    }
  }

  private SqlSelect toSql(CQLSortNode node) throws QueryValidationException {
    StringBuilder order = new StringBuilder();
    String where = pg(node.getSubtree());

    boolean firstIndex = true;
    for (ModifierSet modifierSet : node.getSortIndexes()) {
      if (firstIndex) {
        firstIndex = false;
      } else {
        order.append(", ");
      }

      String desc = "";
      CqlModifiers modifiers = new CqlModifiers(modifierSet);
      if (modifiers.getCqlSort() == CqlSort.DESCENDING) {
        desc = " DESC";
      }  // ASC not needed, it's Postgres' default

      if (modifierSet.getBase().equals("id")) {
        order.append(PK_COLUMN_NAME).append(desc);
        continue;
      }

      IndexTextAndJsonValues vals = getIndexTextAndJsonValues(modifierSet.getBase());

      // if sort field is marked explicitly as number type
      if (modifiers.getCqlTermFormat() == CqlTermFormat.NUMBER) {
        order.append(vals.getIndexJson()).append(desc);
        continue;
      }

      // We assume that a CREATE INDEX for this has been installed.
      order.append(wrapInLowerUnaccent(vals.getIndexText(), modifiers)).append(desc);
    }
    return new SqlSelect(where, order.toString());
  }

  private static String sqlOperator(CQLBooleanNode node) throws CQLFeatureUnsupportedException {
    if (node instanceof CQLAndNode) {
      return "AND";
    }
    if (node instanceof CQLOrNode) {
      return "OR";
    }
    if (node instanceof CQLNotNode) {
      // CQL "NOT" means SQL "AND NOT", see section "7. Boolean Operators" in
      // https://www.loc.gov/standards/sru/cql/spec.html
      return "AND NOT";
    }
    throw createUnsupportedException(node);
  }

  private String pg(CQLBooleanNode node) throws QueryValidationException {
    String operator = sqlOperator(node);
    String isNotTrue = "";

    // special case for the query the UI uses most often, before the user has
    // typed in anything: title=* OR contributors*= OR identifier=*
    if ("OR".equals(operator)
      && node.getRightOperand().getClass() == CQLTermNode.class) {
      CQLTermNode r = (CQLTermNode) (node.getRightOperand());
      if ("*".equals(r.getTerm()) && "=".equals(r.getRelation().getBase())) {
        logger.log(Level.FINE, "pgFT(): Simplifying =* OR =* ");
        return pg(node.getLeftOperand());
      }
    }

    if ("AND NOT".equals(operator)) {
      operator = "AND (";
      isNotTrue = ") IS NOT TRUE";
      // NOT TRUE is (FALSE or NULL) to catch the NULL case when the field does not exist.
      // This completely inverts the right operand.
    }

    return "(" + pg(node.getLeftOperand()) + ") "
        + operator
        + " (" + pg(node.getRightOperand()) + isNotTrue + ")";
  }

  /**
   * Convert index name to SQL term of type text.
   * Examples:
   * <p>index2sqlText("json", "foo") = "json->>'foo'"
   * <p>index2sqlText("table.json", "foo.bar.baz") = "table.json->'foo'->'bar'->>'baz'"
   * @param jsonField
   * @param index name to convert
   *
   * @return SQL term
   */
  static String index2sqlText(String jsonField, String index) throws QueryValidationException {
    StringBuilder res = new StringBuilder();
    String[] comp = index.split("\\.");
    res.append(jsonField);
    for (int j = 0; j < comp.length; j++) {
      if (j < comp.length - 1) {
        res.append("->");
      } else {
        res.append("->>");
      }
      res.append("\'");
      res.append(Cql2SqlUtil.cql2string(comp[j]));
      res.append("\'");
    }
    return res.toString();
  }

  /**
   * Convert index name to SQL term of type json.
   * Example result for field=user and index=foo.bar:
   * user->'foo'->'bar'
   * @param jsonField
   * @param index name to convert
   *
   * @return SQL term
   */
  private static String index2sqlJson(String jsonField, String index) throws QueryValidationException {
    StringBuilder res = new StringBuilder();
    String[] comp = index.split("\\.");
    res.append(jsonField);
    for (int j = 0; j < comp.length; j++) {
      res.append("->");
      res.append("\'");
      res.append(Cql2SqlUtil.cql2string(comp[j]));
      res.append("\'");
    }
    return res.toString();
  }

  private IndexTextAndJsonValues getIndexTextAndJsonValues(String index)
      throws QueryValidationException {
    if (jsonFields != null && jsonFields.size() > 1) {
      return multiFieldProcessing(index);
    }
    IndexTextAndJsonValues vals = new IndexTextAndJsonValues();
    vals.setIndexJson(index2sqlJson(this.jsonField, index));
    vals.setIndexText(index2sqlText(this.jsonField, index));
    return vals;
  }

  private IndexTextAndJsonValues multiFieldProcessing( String index ) throws QueryValidationException {
    IndexTextAndJsonValues vals = new IndexTextAndJsonValues();

    // processing for case where index is prefixed with json field name
    for (String f : jsonFields) {
      if (index.startsWith(f+'.')) {
        String indexTermWithinField = index.substring(f.length()+1);
        vals.setIndexJson(index2sqlJson(f, indexTermWithinField));
        vals.setIndexText(index2sqlText(f, indexTermWithinField));
        return vals;
      }
    }

    // if no json field name prefix is found, the default field name gets applied.
    String defaultJsonField = this.jsonFields.get(0);
    vals.setIndexJson(index2sqlJson(defaultJsonField, index));
    vals.setIndexText(index2sqlText(defaultJsonField, index));
    return vals;
  }

  private String pg(CQLTermNode node) throws QueryValidationException {
    if ("cql.allRecords".equalsIgnoreCase(node.getIndex())) {
      return "true";
    }
    //determine if this table is real by checking in schema
    //determine right here whether this node deals with a foreign term on right or left side
    Table indexTable = checkForForeignLocationOfIndex(node);
    if (indexTable != null) {
      //we are doing a foreign key search
      //determine foreign key linking tables
      return subQuery(node.getIndex(), node, indexTable);
    }
    if ("cql.serverChoice".equalsIgnoreCase(node.getIndex())) {
      if (serverChoiceIndexes.isEmpty()) {
        throw new QueryValidationException("cql.serverChoice requested, but no serverChoiceIndexes defined.");
      }
      List<String> sqlPieces = new ArrayList<>();
      for (String index : serverChoiceIndexes) {
        sqlPieces.add(index2sql(index, node));
      }
      return String.join(" OR ", sqlPieces);
    }
    return index2sql(node.getIndex(), node);
  }

  //method to determine if the right side of the node is a foreign term and returns the table it is attached to is so
  private Table checkForForeignLocationOfIndex(CQLTermNode node) {
    String[] idxParts = node.getIndex().split("\\.");
    //if the table is not supplied we do not have enough information to proceed, thereby assume it is in current table
    if(idxParts.length <= 1) {
      return null;
    }
    return getForeignTable(idxParts[0]);
  }

  private Table getForeignTable(String tableName) {
    if (dbTable == null || dbTable.getTableName().equals(tableName)) {
      return null;
    }
    for (Table table : dbSchema.getTables()) {
      if (table.getTableName().equals(tableName)) {
        return table;
      }
    }
    return null;
  }

  private String subQuery(String index, CQLTermNode node, Table correlation) throws QueryValidationException {
    String [] foreignTarget = index.split("\\.");
    ForeignKeys childParentForeignKey = findForeignKey(dbTable, dbTable.getPkColumnName(), correlation);
    boolean indexInTable =  childParentForeignKey == null;
    ForeignKeys fkey = indexInTable ? findForeignKey(correlation, PK_COLUMN_NAME, dbTable) : childParentForeignKey;
    Table indexTable = indexInTable ? dbTable : correlation;
    

    if (fkey == null) {
      String msg = "subQuery: No foreignKey for table " + foreignTarget[0] + " found";
      logger.log(Level.SEVERE, msg);
      throw new QueryValidationException(msg);
    }
    if (fkey.getTargetTable() == null) {
      String msg = "subQuery: Missing target table for foreignKey field " + fkey.getFieldName();
      logger.log(Level.SEVERE, msg);
      throw new QueryValidationException(msg);
    }
    String match = foreignKeyMatch(node, indexTable, foreignTarget);
    if (indexInTable) {
      return formatParentChild(foreignTarget[0], fkey, match);
    } else {
      return formatChildParent(foreignTarget[0], fkey, match);
    }
  }
  private String foreignKeyMatch(CQLTermNode node,  Table targetTable,String [] foreignTarget)  throws QueryValidationException {
    String foreignTableJsonb = foreignTarget[0] + ".jsonb";
    
    IndexTextAndJsonValues vals = getSubQueryIndexTextAndJsonValues( foreignTableJsonb, foreignTarget );
    CqlModifiers cqlModifiers = new CqlModifiers(node);
    String indexField = foreignTarget[1];
    return indexNode(indexField,targetTable, node, vals, cqlModifiers);
  }
  private IndexTextAndJsonValues getSubQueryIndexTextAndJsonValues(String foreignTableJsonb,String [] foreignTarget ) throws QueryValidationException {
    if (jsonField == null) {
      return multiFieldProcessing(foreignTarget[1]);
    }
    IndexTextAndJsonValues vals = new IndexTextAndJsonValues();
    vals.setIndexJson(index2sqlJson(this.jsonField, foreignTarget[1]));
    vals.setIndexText(index2sqlText(foreignTableJsonb, foreignTarget[1]));
    return vals;
  }
//  private static String foreignKeyMatch(CQLTermNode node, String indexSQL, Index index) throws QueryValidationException {
//    String term = node.getTerm();
//    CqlModifiers modifiers = new CqlModifiers(node);
//    if (CqlTermFormat.NUMBER == modifiers.getCqlTermFormat()) {
//      return "(" + indexSQL + ")::NUMERIC = ('" + Cql2SqlUtil.cql2string(term) + "')::NUMERIC";
//    }
//    if (index == null) {
//      return indexSQL + " = " + "'" + Cql2SqlUtil.cql2string(term) + "'";
//    }
//
//    return wrapInLowerUnaccent(indexSQL, index) + " = " + wrapInLowerUnaccent("'" + Cql2SqlUtil.cql2string(term) + "'", index);
//  }

  private String formatChildParent(String foreignTableName, ForeignKeys fkey,
      String match) throws QueryValidationException {

    return dbTable.getTableName() + "." + PK_COLUMN_NAME + " IN "
        + " ( SELECT " + "(" + index2sqlText(foreignTableName + ".jsonb", fkey.getFieldName()) + ")::UUID"
        + " from " + foreignTableName + " WHERE " + match + ")";
  }

  private String formatParentChild(String foreignTableName, ForeignKeys fkey, String match)
      throws QueryValidationException {

    return "(" + index2sqlText(dbTable.getTableName() + ".jsonb", fkey.getFieldName()) + ")::UUID" + " IN "
        + " ( SELECT " + PK_COLUMN_NAME + " from " + foreignTableName + " WHERE " + match + ")";
  }


  /**
   * Return the ForeignKeys from targetTable where fieldName refers to field of the
   * current table.
   * @param field  field to find as foreign key
   * @param targetTable  where to search
   * @return the ForeignKeys if found, null otherwise
   */
  private static ForeignKeys findForeignKey(Table currentTable, String field, Table targetTable) {
    String fieldName = currentTable.getTableName() + field;
    if (targetTable.getForeignKeys() == null) {
      return null;
    }
    for (ForeignKeys key : targetTable.getForeignKeys()) {
      if (fieldName.equalsIgnoreCase(key.getFieldName())) {
        return key;
      }
    }

    return null;
  }

  /**
   * Normalize a term for FT searching. Escape quotes, masking, etc
   *
   * @param term
   * @return
   */
  @SuppressWarnings({
    "squid:ForLoopCounterChangedCheck",
    // Yes, we skip the occasional character in the loop by incrementing i
    "squid:S135"
  // Yes, we have a few continue statements. Unlike what SQ says,
  // refactoring the code to avoid that would make it much less
  // readable.
  })
  private static String ftTerm(String term) throws QueryValidationException {
    StringBuilder res = new StringBuilder();
    term = term.trim();
    for (int i = 0; i < term.length(); i++) {
      // CQL specials
      char c = term.charAt(i);
      switch (c) {
        case '?':
          throw new QueryValidationException("CQL: single character mask unsupported (?)");
        case '*':
          if (i == term.length() - 1) {
            res.append(":*");
            continue;
          } else {
            throw new QueryValidationException("CQL: only right truncation supported");
          }
        case '\\':
          if (i == term.length() - 1) {
            continue;
          }
          i++;
          c = term.charAt(i);
          break;
        case '^':
          throw new QueryValidationException("CQL: anchoring unsupported (^)");
        default:
        // SQ complains if there is no default case, and if there is an empty statement #!$
      }
      if (c == '\'') {
        if (res.length() > 0) {
          res.append("''"); // double up single quotes
        } // but not in the beginning of the term, won't work.
        continue;
      }
      // escape for FT
      if ("&!|()<>*:\\".indexOf(c) != -1) {
        res.append("\\");
      }
      res.append(c);
    }
    return res.toString();
  }
  /**
   * Handle a termnode that does a search on the id. We use the primary key
   * column in the query, it is clearly faster, and we use a numerical
   * comparison instead of truncation. That way PG will use the primary key,
   * which is pretty much faster. Assumes that the UUID has already been
   * validated to be in the right format.
   *
   * @param node
   * @return SQL where clause component for this term
   * @throws QueryValidationException
   */
  private String pgId(CQLTermNode node) throws QueryValidationException {
    String comparator = StringUtils.defaultString(node.getRelation().getBase());
    if (!node.getRelation().getModifiers().isEmpty()) {
      throw new QueryValidationException("CQL: Unsupported modifier "
        + node.getRelation().getModifiers().get(0).getType());
    }
    String term = node.getTerm();
    boolean equals = true;
    switch (comparator) {
    case ">":
    case "<":
    case ">=":
    case "<=":
      if (!uuidPattern.matcher(term).matches()) {
        throw new QueryValidationException("CQL: Invalid UUID after id comparator " + comparator + ": " + term);
      }
      return PK_COLUMN_NAME + comparator + "'" + term + "'";
    case "==":
    case "=":
      comparator = "=";
      break;
    case "<>":
      equals = false;
      break;
    default:
      throw new QueryValidationException("CQL: Unsupported operator '" + comparator + "' "
          + "id only supports '=', '==', and '<>' (possibly with right truncation)");
    }

    if (term.equals("") || term.equals("*")) {
      // squid:S1774 The ternary operator should not be used
      return equals ? "true" : "false";  // no need to check
      // not even for "", since id is a mandatory field, so
      // "all that have id" is the same as "all records"
    }

    if (!term.contains("*")) { // exact match
      if (!uuidPattern.matcher(term).matches()) {
        // avoid SQL injection, don't put term into comment
        return equals
            ? "false /* id == invalid UUID */"
            : "true /* id <> invalid UUID */";
      }
      return PK_COLUMN_NAME + comparator + "'" + term + "'";
    }
    String truncTerm = term.replaceFirst("\\*$", ""); // remove trailing '*'
    if (truncTerm.contains("*")) { // any remaining '*' is an error
      throw new QueryValidationException("CQL: only right truncation supported for id:  " + term);
    }
    String lo = new StringBuilder("00000000-0000-0000-0000-000000000000")
      .replace(0, truncTerm.length(), truncTerm).toString();
    String hi = new StringBuilder("ffffffff-ffff-ffff-ffff-ffffffffffff")
      .replace(0, truncTerm.length(), truncTerm).toString();
    if (!uuidPattern.matcher(lo).matches() || !uuidPattern.matcher(hi).matches()) {
      // avoid SQL injection, don't put term into comment
      return equals ? "false /* id == invalid UUID */"
                    : "true /* id <> invalid UUID */";
    }
    if (equals) {
      return "(" + PK_COLUMN_NAME + ">='" + lo + "'"
        + " and " + PK_COLUMN_NAME + "<='" + hi + "')";
    } else {
      return "(" + PK_COLUMN_NAME + "<'" + lo + "'"
          + " or " + PK_COLUMN_NAME + ">'" + hi + "')";
    }
  }

  private String lookupModifier(Index schemaIndex, String modifierName) {
    if (schemaIndex != null) {
      List<String> schemaModifiers = schemaIndex.getArrayModifiers();
      if (schemaModifiers != null) {
        for (String schemaModifier : schemaModifiers) {
          if (schemaModifier.equalsIgnoreCase(modifierName)) {
            return schemaModifier;
          }
        }
      }
      String subfield = schemaIndex.getArraySubfield();
      if (subfield != null && subfield.equalsIgnoreCase(modifierName)) {
        return subfield;
      }
    }
    return null;
  }

  private String arrayNode(String index, CQLTermNode node, CqlModifiers modifiers,
    List<Modifier> relationModifiers, Index schemaIndex, IndexTextAndJsonValues incomingvals) throws QueryValidationException {

    StringBuilder sqlAnd = new StringBuilder();
    StringBuilder sqlOr = new StringBuilder();
    modifiers.setRelationModifiers(new LinkedList<>()); // avoid recursion
    for (Modifier relationModifier : relationModifiers) {
      final String modifierName = relationModifier.getType().substring(1);
      final String modifierValue = relationModifier.getValue();
      String foundModifier = lookupModifier(schemaIndex, modifierName);
      if (foundModifier == null) {
        throw new QueryValidationException("CQL: Unsupported relation modifier "
          + relationModifier.getType());
      }
      if (modifierValue == null) {
        if (sqlOr.length() == 0) {
          sqlOr.append("(");
        } else {
          sqlOr.append(" or ");
        }
        IndexTextAndJsonValues vals = new IndexTextAndJsonValues();
        vals.setIndexText(index2sqlText("t.c", foundModifier));
        sqlOr.append(indexNode(index,this.dbTable, node, vals, modifiers));
      } else {
        final String comparator = relationModifier.getComparison();
        if (!"=".equals(comparator)) {
          throw new QueryValidationException("CQL: Unsupported comparison for relation modifier " + relationModifier.getType());
        }
        sqlAnd.append(" and ");
        sqlAnd.append(queryByFt(index2sqlText("t.c", foundModifier), modifierValue,
          comparator, schemaIndex));
      }
    }
    if (sqlOr.length() > 0) {
      sqlOr.append(")");
    } else {
      String modifiersSubfield = null;
      if (schemaIndex != null) {
        modifiersSubfield = schemaIndex.getArraySubfield();
      }
      if (modifiersSubfield == null) {
        throw new QueryValidationException("CQL: No arraySubfield defined for index " + index);
      }
      IndexTextAndJsonValues vals = new IndexTextAndJsonValues();
      vals.setIndexText(index2sqlText("t.c", modifiersSubfield));
      sqlOr.append(indexNode(index,this.dbTable, node, vals, modifiers));
    }
    String indexText = index2sqlJson(this.jsonField, index);
    return "id in (select t.id"
      + " from (select id as id, "
      + "             jsonb_array_elements(" + incomingvals.getIndexJson() + ") as c"
      + "      ) as t"
      + " where " + sqlOr.toString() + sqlAnd.toString() + ")";
  }

  /**
   * Create an SQL expression where index is applied to all matches.
   *
   * @param index index to use
   * @param node CQLTermNode to use
   *
   * @return SQL expression
   * @throws QueryValidationException
   */
  private String index2sql(String index, CQLTermNode node) throws QueryValidationException {
    IndexTextAndJsonValues vals = getIndexTextAndJsonValues(index);
    CqlModifiers cqlModifiers = new CqlModifiers(node);
    return indexNode(index, this.dbTable, node, vals, cqlModifiers);
  }

  private String indexNode(String index, Table targetTable, CQLTermNode node, IndexTextAndJsonValues vals,
    CqlModifiers modifiers) throws QueryValidationException {

    // special handling of id search (re-use existing code)
    if ("id".equals(index)) {
      return pgId(node);
    }

    DbIndex dbIndex = DbSchemaUtils.getDbIndex(dbTable, index);
    String comparator = node.getRelation().getBase().toLowerCase();

    switch (comparator) {
    case "=":
      if (CqlTermFormat.NUMBER == modifiers.getCqlTermFormat()) {
        return queryBySql(dbIndex.isOther(), vals, node, comparator, modifiers, targetTable);
      } else if (CqlAccents.IGNORE_ACCENTS == modifiers.getCqlAccents() &&
          CqlCase.IGNORE_CASE == modifiers.getCqlCase()) {
        return queryByFt(index, dbIndex.isFt(), vals, node, comparator, modifiers, targetTable);
      } else {
        return queryByLike(index, dbIndex.isGin(), vals, node, comparator, modifiers, targetTable);
      }
    case "adj":
    case "all":
    case "any":
      return queryByFt(index, dbIndex.isFt(), vals, node, comparator, modifiers, targetTable);
    case "==":
    case "<>":
      if (CqlTermFormat.STRING == modifiers.getCqlTermFormat()) {
        boolean hasIndex = dbIndex.isGin();
        if (!Cql2SqlUtil.hasCqlWildCardd(node.getTerm())) {
          hasIndex = hasIndex || dbIndex.isOther();
        }
        return queryByLike(index, hasIndex, vals, node, comparator, modifiers, targetTable);
      } else {
        return queryBySql(dbIndex.isOther(), vals, node, comparator, modifiers, targetTable);
      }
    case "<" :
    case ">" :
    case "<=" :
    case ">=" :
      return queryBySql(dbIndex.isOther(), vals, node, comparator, modifiers, targetTable);
    default:
      throw new CQLFeatureUnsupportedException("Relation " + comparator
          + " not implemented yet: " + node.toString());
    }
  }

  /**
   * Create an SQL expression using Full Text query syntax.
   *
   * @param hasIndex
   * @param vals
   * @param node
   * @param comparator
   * @param modifiers
   * @return
   * @throws QueryValidationException
   */
  private String queryByFt(String index, boolean hasFtIndex, IndexTextAndJsonValues vals, CQLTermNode node, String comparator, CqlModifiers modifiers,Table targetTable) throws QueryValidationException {
    final String indexText = vals.getIndexText();

    if (CqlAccents.RESPECT_ACCENTS == modifiers.getCqlAccents()) {
      logger.log(Level.WARNING, "Ignoring /respectAccents modifier for FT search {0}", indexText);
    }

    if (CqlCase.RESPECT_CASE == modifiers.getCqlCase()) {
      logger.log(Level.WARNING, "Ignoring /respectCase modifier for FT search {0}", indexText);
    }

    // Clean the term. Remove stand-alone ' *', not valid word.
    String term = node.getTerm().replaceAll(" +\\*", "").trim();
    Index schemaIndex = null;
    if (targetTable != null) {
      schemaIndex = DbSchemaUtils.getIndex(index, targetTable.getFullTextIndex());
    }
    String sql = queryByFt(indexText, term, comparator, schemaIndex);
    List<Modifier> relationModifiers = modifiers.getRelationModifiers();
    if (!relationModifiers.isEmpty()) {
      sql = sql + " AND " + arrayNode(index, node, modifiers, relationModifiers, schemaIndex, vals);
    }

    if (!hasFtIndex) {
      String s = String.format("%s, CQL >>> SQL: %s >>> %s", indexText, node.toCQL(), sql);
      logger.log(Level.WARNING, "Doing FT search without index for {0}", s);
    }

    return sql;
  }

  private String queryByFt(String indexText, String term, String comparator, Index schemaIndex)
    throws QueryValidationException {

    if (term.equals("*")) {
      return "true";
    }
    if (term.equals("")) {
      return indexText + " ~ ''";
    }
    // Removing of " - " is RMB-439 to unblock CIRCSTORE-138
    String[] words = term.replaceAll(" - ", " ").split("\\s+");
    for (int i = 0; i < words.length; i++) {
      words[i] = ftTerm(words[i]);
    }
    String tsTerm = "";
    switch (comparator) {
      case "=":
      case "adj":
        tsTerm = String.join("<->", words);
        break;
      case "any":
        tsTerm = String.join(" | ", words);
        break;
      case "all":
        tsTerm = String.join(" & ", words);
        break;
      default:
        throw new QueryValidationException("CQL: Unknown comparator '" + comparator + "'");
    }
    String sql = "to_tsvector('simple', " + wrapInLowerUnaccent(indexText, schemaIndex) + ") "
      + "@@ to_tsquery('simple', " + wrapInLowerUnaccent("'"+ tsTerm + "'", schemaIndex) + ")";

    logger.log(Level.FINE, "index {0} generated SQL {1}", new Object[]{indexText, sql});
    return sql;
  }

  /**
   * Create an SQL expression using LIKE query syntax.
   *
   * @param hasIndex
   * @param vals
   * @param node
   * @param comparator
   * @param modifiers
   * @return
   */
  private String queryByLike(String index, boolean hasIndex, IndexTextAndJsonValues vals, CQLTermNode node,
    String comparator, CqlModifiers modifiers, Table targetTable) throws QueryValidationException {
    
    String indexText = vals.getIndexText();
    String sql = null;

    List<Modifier> relationModifiers = modifiers.getRelationModifiers();
    if (!relationModifiers.isEmpty()) {
      final Index schemaIndex = DbSchemaUtils.getIndex(index, this.dbTable.getGinIndex());
      sql = arrayNode(index, node, modifiers, relationModifiers, schemaIndex, vals);
    } else {
    Index schemaIndex = null;
    if(targetTable != null ) {
      schemaIndex = DbSchemaUtils.getIndex(index, targetTable.getGinIndex());
    }
      String likeOperator = comparator.equals("<>") ? " NOT LIKE " : " LIKE ";
      String like = "'" + Cql2SqlUtil.cql2like(node.getTerm()) + "'";
    String indexMatch = wrapInLowerUnaccent(indexText, schemaIndex) + likeOperator + wrapInLowerUnaccent(like, schemaIndex);
      if (modifiers.getCqlAccents() == CqlAccents.IGNORE_ACCENTS && modifiers.getCqlCase() == CqlCase.IGNORE_CASE) {
        sql = indexMatch;
      } else {
        sql = indexMatch + " AND " +
        wrapInLowerUnaccent(indexText, schemaIndex) + likeOperator + wrapInLowerUnaccent(like, schemaIndex);
      }
    }

    if (!hasIndex) {
      String s = String.format("%s, CQL >>> SQL: %s >>> %s", indexText, node.toCQL(), sql);
      logger.log(Level.WARNING, "Doing LIKE search without index for {0}", s);
    }

    logger.log(Level.FINE, "index {0} generated SQL {1}", new Object[] {indexText, sql});
    return sql;
  }

  /**
   * Create an SQL expression using SQL as is syntax.
   *
   * @param hasIndex
   * @param vals
   * @param node
   * @param comparator
   * @param modifiers
   * @return
   */
  private String queryBySql(boolean hasIndex, IndexTextAndJsonValues vals, CQLTermNode node, String comparator, CqlModifiers modifiers, Table targetTable) {
    String index = vals.getIndexText();

    if (comparator.equals("==")) {
      comparator = "=";
    }
    String term = "'" + Cql2SqlUtil.cql2like(node.getTerm()) + "'";
    if (CqlTermFormat.NUMBER.equals(modifiers.getCqlTermFormat())) {
      index = "(" + index + ")::numeric";
      term = node.getTerm();
    }
    String sql = index + " " + comparator + term;

    if (!hasIndex) {
      String s = String.format("%s, CQL >>> SQL: %s >>> %s", index, node.toCQL(), sql);
      logger.log(Level.WARNING, "Doing SQL query without index for {0}", s);
    }

    logger.log(Level.FINE, "index {0} generated SQL {1}", new Object[] {index, sql});
    return sql;
  }

}
