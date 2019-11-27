package org.folio.cql2pgjson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import org.folio.cql2pgjson.model.DbFkInfo;
import org.folio.cql2pgjson.model.DbIndex;
import org.folio.cql2pgjson.model.IndexTextAndJsonValues;
import org.folio.cql2pgjson.model.SqlSelect;
import org.folio.cql2pgjson.util.Cql2PgUtil;
import org.folio.cql2pgjson.util.Cql2SqlUtil;
import org.folio.cql2pgjson.util.DbSchemaUtils;
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

  private static final String JSONB_COLUMN_NAME = "jsonb";

  private final Pattern uuidPattern = Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

  private String jsonField = null;
  private List<String> jsonFields = null;

  // leverage RMB and consider to merge cql2pgjson into RMB
  private Schema dbSchema;
  private Table dbTable;
  /** Maps an index name to its DbIndex information */
  private Map<String,DbIndex> dbIndexMap = new HashMap<>();

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
      logger.log(Level.INFO, "loadDbSchema: Loaded {0} OK", schemaPath);
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
   *
   * @deprecated use toSql instead
   */
  @Deprecated
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
    if (index == null) {
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

      String field = modifierSet.getBase();
      DbIndex dbIndex = dbIndexMap.computeIfAbsent(field, f -> DbSchemaUtils.getDbIndex(dbTable, f));
      if (dbIndex.isForeignKey() || "id".equals(field)) {
        order.append(field).append(desc);
        continue;
      }

      IndexTextAndJsonValues vals = getIndexTextAndJsonValues(field);

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

  private IndexTextAndJsonValues getIndexTextAndJsonValues(String index) {
    if (jsonFields != null && jsonFields.size() > 1) {
      return multiFieldProcessing(index);
    }
    IndexTextAndJsonValues vals = new IndexTextAndJsonValues();
    vals.setIndexJson(Cql2PgUtil.cqlNameAsSqlJson(this.jsonField, index));
    vals.setIndexText(Cql2PgUtil.cqlNameAsSqlText(this.jsonField, index));
    return vals;
  }

  private IndexTextAndJsonValues multiFieldProcessing(String index ) {
    IndexTextAndJsonValues vals = new IndexTextAndJsonValues();

    // processing for case where index is prefixed with json field name
    for (String f : jsonFields) {
      if (index.startsWith(f+'.')) {
        String indexTermWithinField = index.substring(f.length()+1);
        vals.setIndexJson(Cql2PgUtil.cqlNameAsSqlJson(f, indexTermWithinField));
        vals.setIndexText(Cql2PgUtil.cqlNameAsSqlText(f, indexTermWithinField));
        return vals;
      }
    }

    // if no json field name prefix is found, the default field name gets applied.
    String defaultJsonField = this.jsonFields.get(0);
    vals.setIndexJson(Cql2PgUtil.cqlNameAsSqlJson(defaultJsonField, index));
    vals.setIndexText(Cql2PgUtil.cqlNameAsSqlText(defaultJsonField, index));
    return vals;
  }

  private String pg(CQLTermNode node) throws QueryValidationException {
    if ("cql.allRecords".equalsIgnoreCase(node.getIndex())) {
      return "true";
    }

    // determine if index is in a foreign table
    if (dbTable != null) {
      String srcTabName = dbTable.getTableName();
      String targetTabAlias = node.getIndex().split("\\.")[0];

      // child to parent
      List<DbFkInfo> fks = DbSchemaUtils.findForeignKeysFromSourceTableToTargetAlias(dbSchema, srcTabName, targetTabAlias);
      if (!fks.isEmpty()) {
        return pgSubQuery(node, fks, true);
      }

      // parent to child
      fks = DbSchemaUtils.findForeignKeysFromSourceAliasToTargetTable(dbSchema, targetTabAlias, srcTabName);
      if (!fks.isEmpty()) {
        return pgSubQuery(node, fks, false);
      }
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

  private String pgSubQuery(CQLTermNode node, List<DbFkInfo> fks, boolean childToParent) throws QueryValidationException {
    String currentTableName = dbTable.getTableName();
    Table targetTable = null;

    StringBuilder sb = new StringBuilder();
    if (childToParent) {
      // child to parent
      targetTable = DbSchemaUtils.getTable(dbSchema, fks.get(fks.size() -1).getTargetTable());
      for (DbFkInfo fk : fks) {
        sb.append(currentTableName).append('.').append(fk.getField())
          .append(" IN  ( SELECT id FROM ").append(fk.getTargetTable()).append(" WHERE ");
        currentTableName = fk.getTargetTable();
      }
   } else {
      // parent to child
      targetTable = DbSchemaUtils.getTable(dbSchema, fks.get(0).getTable());
      for (int i = fks.size() - 1 ; i >= 0; i--) {
        DbFkInfo fk = fks.get(i);
        sb.append(currentTableName).append(".id IN  ( SELECT ").append(fks.get(i).getField())
          .append(" FROM ").append(fk.getTable()).append(" WHERE ");
        currentTableName = fk.getTable();
      }
    }
    String [] foreignTarget = node.getIndex().split("\\.", 2);
    sb.append(indexNodeForForeignTable(node, targetTable, foreignTarget));
    for (int i = 0; i < fks.size(); i++) {
      sb.append(')');
    }
    return sb.toString();
  }

  private String indexNodeForForeignTable(CQLTermNode node, Table targetTable, String[] foreignTarget)
      throws QueryValidationException {

    String foreignTableJsonb = targetTable.getTableName() + "." + JSONB_COLUMN_NAME;

    IndexTextAndJsonValues vals = new IndexTextAndJsonValues();
    vals.setIndexJson(Cql2PgUtil.cqlNameAsSqlJson(foreignTableJsonb, foreignTarget[1]));
    vals.setIndexText(Cql2PgUtil.cqlNameAsSqlText(foreignTableJsonb, foreignTarget[1]));

    CqlModifiers cqlModifiers = new CqlModifiers(node);
    String indexField = foreignTarget[1];
    return indexNode(indexField, targetTable, node, vals, cqlModifiers);
  }

  /**
   * Search a UUID field that we've extracted from the jsonb into a proper UUID
   * database table column. This is either the primary key id or a foreign key.
   * There always exists an index. Using BETWEEN lo AND hi with UUIDs is faster
   * than a string comparison with truncation.
   *
   * @param node the CQL to convert into SQL
   * @return SQL where clause component for this term
   * @throws QueryValidationException on invalid UUID format or invalid operator
   */
  private String pgId(CQLTermNode node, String columnName) throws QueryValidationException {
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
        throw new QueryValidationException(
            "CQL: Invalid UUID after '" + columnName + comparator + "': " + term);
      }
      return columnName + comparator + "'" + term + "'";
    case "==":
    case "=":
      comparator = "=";
      break;
    case "<>":
      equals = false;
      break;
    default:
      throw new QueryValidationException("CQL: Unsupported operator '" + comparator + "', "
          + "UUID " + columnName + " only supports '=', '==', and '<>' (possibly with right truncation)");
    }

    if (StringUtils.isEmpty(term)) {
      term = "*";
    }
    if ("*".equals(term) && "id".equals(columnName)) {
      return equals ? "true" : "false";  // no need to check
      // since id is a mandatory field, so
      // "all that have id" is the same as "all records"
    }

    if (!term.contains("*")) { // exact match
      if (!uuidPattern.matcher(term).matches()) {
        // avoid SQL injection, don't put term into comment
        return equals
            ? "false /* " + columnName + " == invalid UUID */"
            : "true /* "  + columnName + " <> invalid UUID */";
      }
      return columnName + comparator + "'" + term + "'";
    }
    String truncTerm = term;
    while (truncTerm.endsWith("*")) {  // remove trailing stars
      truncTerm = truncTerm.substring(0, truncTerm.length() - 1);
    }
    if (truncTerm.contains("*")) { // any remaining '*' is an error
      throw new QueryValidationException("CQL: only right truncation supported for id:  " + term);
    }
    String lo = new StringBuilder("00000000-0000-0000-0000-000000000000")
      .replace(0, truncTerm.length(), truncTerm).toString();
    String hi = new StringBuilder("ffffffff-ffff-ffff-ffff-ffffffffffff")
      .replace(0, truncTerm.length(), truncTerm).toString();
    if (!uuidPattern.matcher(lo).matches() || !uuidPattern.matcher(hi).matches()) {
      // avoid SQL injection, don't put term into comment
      return equals ? "false /* " + columnName + " == invalid UUID */"
                    : "true /* "  + columnName + " <> invalid UUID */";
    }
    return equals ? "(" + columnName +     " BETWEEN '" + lo + "' AND '" + hi + "')"
                  : "(" + columnName + " NOT BETWEEN '" + lo + "' AND '" + hi + "')";
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
    List<Modifier> relationModifiers, Index schemaIndex, IndexTextAndJsonValues incomingvals, Table targetTable) throws QueryValidationException {

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
        vals.setIndexText(Cql2PgUtil.cqlNameAsSqlText("t.c", foundModifier));
        sqlOr.append(indexNode(index, this.dbTable, node, vals, modifiers));
      } else {
        final String comparator = relationModifier.getComparison();
        if (!"=".equals(comparator)) {
          throw new QueryValidationException("CQL: Unsupported comparison for relation modifier " + relationModifier.getType());
        }
        sqlAnd.append(" and ");
        sqlAnd.append(queryByFt(Cql2PgUtil.cqlNameAsSqlText("t.c", foundModifier), modifierValue,
          comparator, schemaIndex, targetTable));
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
      vals.setIndexText(Cql2PgUtil.cqlNameAsSqlText("t.c", modifiersSubfield));
      sqlOr.append(indexNode(index, this.dbTable, node, vals, modifiers));
    }
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

    // primary key
    if ("id".equals(index)) {
      return pgId(node, index);
    }

    DbIndex dbIndex;
    if (targetTable == null || dbTable.equals(targetTable)) {
      dbIndex = dbIndexMap.computeIfAbsent(index              , i -> DbSchemaUtils.getDbIndex(dbTable, index));
    } else {  // foreign table
      dbIndex = dbIndexMap.computeIfAbsent(vals.getIndexJson(), i -> DbSchemaUtils.getDbIndex(targetTable, index));
    }

    if (dbIndex.isForeignKey()) {
      return pgId(node, index);
    }

    String comparator = node.getRelation().getBase().toLowerCase();

    switch (comparator) {
    case "=":
      if (CqlTermFormat.NUMBER == modifiers.getCqlTermFormat()) {
        return queryBySql(dbIndex.isOther(), vals, node, comparator, modifiers);
      } else {
        return queryByFt(index, dbIndex.isFt(), vals, node, comparator, modifiers, targetTable);
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
        return queryBySql(dbIndex.isOther(), vals, node, comparator, modifiers);
      }
    case "<" :
    case ">" :
    case "<=" :
    case ">=" :
      return queryBySql(dbIndex.isOther(), vals, node, comparator, modifiers);
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
  private String queryByFt(String index, boolean hasFtIndex, IndexTextAndJsonValues vals, CQLTermNode node, String comparator,
      CqlModifiers modifiers,Table targetTable) throws QueryValidationException {
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
    String sql = queryByFt(indexText, term, comparator, schemaIndex, targetTable);

    // array modifier
    List<Modifier> relationModifiers = modifiers.getRelationModifiers();
    if (!relationModifiers.isEmpty()) {
      sql += " AND " + arrayNode(index, node, modifiers, relationModifiers, schemaIndex, vals, targetTable);
    }

    if (schemaIndex != null && schemaIndex.isCaseSensitive()) {
      throw new CQLFeatureUnsupportedException("full text index does not support case sensitive: " + index);
    }

    if (!hasFtIndex) {
      String s = String.format("%s, CQL >>> SQL: %s >>> %s", indexText, node.toCQL(), sql);
      logger.log(Level.WARNING, "Doing FT search without index for {0}", s);
    }

    return sql;
  }

  String queryByFt(String indexText, String term, String comparator, Index schemaIndex, Table targettable)
    throws QueryValidationException {

    if (term.equals("*")) {
      return "true";
    }
    if (term.equals("")) {
      return indexText + " ~ ''";
    }

    boolean removeAccents = schemaIndex == null || schemaIndex.isRemoveAccents();
    StringBuilder tsTerm;
    switch (comparator) {
      case "=":
      case "adj":
        tsTerm = Cql2SqlUtil.cql2tsqueryPhrase(term, removeAccents);
        break;
      case "any":
        tsTerm = Cql2SqlUtil.cql2tsqueryOr(term, removeAccents);
        break;
      case "all":
        tsTerm = Cql2SqlUtil.cql2tsqueryAnd(term, removeAccents);
        break;
      default:
        throw new QueryValidationException("CQL: Unknown full text comparator '" + comparator + "'");
    }
    if(schemaIndex != null && schemaIndex.getMultiFieldNames() != null) {
      indexText = wrapInLowerUnaccent(schemaIndex.getFinalSqlExpression(targettable.getTableName()),  /* lower */ false, removeAccents);
    } else if(schemaIndex != null && schemaIndex.getSqlExpression() != null) {
      indexText = schemaIndex.getSqlExpression();
    } else {
      indexText = wrapInLowerUnaccent(indexText, /* lower */ false, removeAccents);
    }
    String sql = "to_tsvector('simple', " + indexText + ") "
      + "@@ " + tsTerm;

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
      sql = arrayNode(index, node, modifiers, relationModifiers, schemaIndex, vals, targetTable);
    } else {
      Index schemaIndex = null;
      if (targetTable != null) {
        schemaIndex = DbSchemaUtils.getIndex(index, targetTable.getGinIndex());
      }
      String likeOperator = comparator.equals("<>") ? " NOT LIKE " : " LIKE ";
      String term = "'" + Cql2SqlUtil.cql2like(node.getTerm()) + "'";
      String termUnmod = term;
      String indexMod;
      if(schemaIndex != null && schemaIndex.getMultiFieldNames() != null) {
        indexMod = wrapInLowerUnaccent(schemaIndex.getFinalSqlExpression(targetTable.getTableName()), schemaIndex);
      } else if(schemaIndex != null && schemaIndex.getSqlExpression() != null) {
        indexMod = schemaIndex.getSqlExpression();
      } else {
        indexMod = wrapInLowerUnaccent(indexText, schemaIndex);
      }
      indexMod = wrapIndexForLength(indexMod);
      term = wrapInLowerUnaccent(term, schemaIndex);
      sql = "CASE WHEN length(" + termUnmod + ") <= 600 then ";
      sql += indexMod +  likeOperator + wrapTermForLength(term);
      sql = appendLengthCase(likeOperator,indexText,termUnmod,comparator.equals("<>"), sql);
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
  private String queryBySql(boolean hasIndex, IndexTextAndJsonValues vals, CQLTermNode node, String comparator, CqlModifiers modifiers) {
    String index = vals.getIndexText();

    if (comparator.equals("==")) {
      comparator = "=";
    }
    String sql = "";
    String term = Cql2SqlUtil.cql2like(node.getTerm()) ;
    String termUnmod = term;
    if (CqlTermFormat.NUMBER.equals(modifiers.getCqlTermFormat())) {
      index = "(" + index + ")::numeric";
      term = node.getTerm();
    } else {
      index = wrapIndexForLength(index);
      term = wrapTermForLength(term);
      sql = "CASE WHEN length('" + termUnmod + "') <= 600 then ";
    }

    sql += index + " " + comparator + termUnmod;
    if (!CqlTermFormat.NUMBER.equals(modifiers.getCqlTermFormat())) {
      sql = appendLengthCase(comparator, index, termUnmod,false, sql);
    }

    if (!hasIndex) {
      String s = String.format("%s, CQL >>> SQL: %s >>> %s", index, node.toCQL(), sql);
      logger.log(Level.WARNING, "Doing SQL query without index for {0}", s);
    }

    logger.log(Level.FINE, "index {0} generated SQL {1}", new Object[] {index, sql});
    return sql;
  }

  private String appendLengthCase(String comparator, String index, String termUnmod,boolean not, String sql) {

    String joiner = not ? " OR " : " AND ";
    sql += " ELSE " + wrapIndexForLength(index) + " " + comparator +  wrapTermForLength(termUnmod) + joiner + index + " " + comparator + " " + termUnmod + " END";
    return sql;
  }

  private String wrapTermForLength(String term) {
   term = "left(" + term + ",600) ";
   return term;
  }

  private String wrapIndexForLength(String index) {
    index = "left(" + index + ",600)";
    return index;
  }

}
