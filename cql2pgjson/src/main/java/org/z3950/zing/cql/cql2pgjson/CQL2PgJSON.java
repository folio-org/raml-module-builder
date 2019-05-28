package org.z3950.zing.cql.cql2pgjson;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;


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

  final String uuidPattern = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$";
  final String tableNamePattern = "^[0-9a-zA-Z_-]+\\.[0-9a-zA-Z_-]+";

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

  public Table getDbTable() {
    return dbTable;
  }

  public void setDbSchemaPath(String dbSchemaPath) {
    loadDbSchema(dbSchemaPath);
    initDbTable();
  }

  private void loadDbSchema(String schemaPath) {
    try {
      String dbJson;
      if (schemaPath == null) {
        schemaPath = "templates/db_scripts/schema.json";
      }
      dbJson = ResourceUtil.asString(schemaPath, CQL2PgJSON.class);
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

  /**
   * Return $term, lower($term), f_unaccent($term) or lower(f_unaccent($term))
   * according to the cqlModifiers.  If undefined use CqlAccents.IGNORE_ACCENTS
   * and CqlCase.IGNORE_CASE as default.
   * @param term  the String to wrap
   * @param cqlModifiers  what functions to use
   * @return wrapped term
   */
  private static String wrapInLowerUnaccent(String term, CqlModifiers cqlModifiers) {
    String result = term;
    if (cqlModifiers.getCqlAccents() != CqlAccents.RESPECT_ACCENTS) {
      result = "f_unaccent(" + result + ")";
    }
    if (cqlModifiers.getCqlCase() != CqlCase.RESPECT_CASE) {
      result = "lower(" + result + ")";
    }
    return result;
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
      order.append(wrapInLowerUnaccent(vals.getIndexText())).append(desc);
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
   * Example result for field=user and index=foo.bar:
   * user->'foo'->>'bar'
   * @param jsonField
   * @param index name to convert
   *
   * @return SQL term
   */
  private static String index2sqlText(String jsonField, String index) {
    String result = jsonField + "->'" + index.replace(".", "'->'") + "'";
    int lastArrow = result.lastIndexOf("->'");
    return result.substring(0,  lastArrow) + "->>" + result.substring(lastArrow + 2);
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
  private static String index2sqlJson(String jsonField, String index) {
    return jsonField + "->'" + index.replace(".", "'->'") + "'";
  }

  private IndexTextAndJsonValues getIndexTextAndJsonValues(String index)
      throws QueryValidationException {
    if (jsonField == null) {
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
    //determine right here whether this node deals with a foreign term on right or left side
    Table t = checkForForeignLocationOfIndex(node);
    Table u =  checkForForeignLocationOfTerm(node );
    if(t != null ) {
      //we are doing a foreign key search
      //determine foreign key linking tables
      String result = subQuery(node.getIndex() , node, t);
      return result;
    } else if (u != null) {
      //we are doing a foreign key join 
      String result = subQuery(node.getIndex() , node, u);
      return result;
    }
    if ("cql.serverChoice".equalsIgnoreCase(node.getIndex())) {
      if (serverChoiceIndexes.isEmpty()) {
        throw new QueryValidationException("cql.serverChoice requested, but no serverChoiceIndexes defined.");
      }
      List<String> sqlPieces = new ArrayList<>();
      for(String index : serverChoiceIndexes) {
        sqlPieces.add(index2sql(index, node));
      }
      return String.join(" OR ", sqlPieces);
    }
    return index2sql(node.getIndex(), node);
  }

  //method to determine if the right side of the node is a foreign term
  private Table checkForForeignLocationOfTerm(CQLTermNode node) {
    String[] termParts = node.getTerm().split("\\.");
    //if the table is not supplied we do not have enough information to proceed, thereby assume it is in current table
    if(termParts.length == 1) {
      return null;
    }
    return isItForeign(termParts[0]);
  }

  //method to determine if the right side of the node is a foreign term and returns the table it is attached to is so
  private Table checkForForeignLocationOfIndex(CQLTermNode node) {
    
    String[] idxParts = node.getIndex().split("\\.");
    //if the table is not supplied we do not have enough information to proceed, thereby assume it is in current table
    if(idxParts.length == 1) {
      return null;
    }
    return isItForeign(idxParts[0] );

  }

  private Table isItForeign(String item) {
 
    List<Table> schemaTables = dbSchema.getTables();
    Iterator<Table> it = schemaTables.iterator();
    while (it.hasNext()) {
      Object e = it.next();
      if(((Table)e).getTableName().equals(item) && !((Table)e).getTableName().equals(dbTable.getTableName()) ) {
        return ((Table)e);
      }
    }
    
    return null;
  }

  private String subQuery(String index,CQLTermNode node, Table correlation ) {
    String[] idxParts = index.split("\\.");
    String[] termParts = node.getTerm().split("\\.");
    if (idxParts.length <= 1 && termParts.length <= 1 ) {
      //considering the lack of a table.field as a failure even though it could be rescued with more work
      logger.log(Level.SEVERE, "subQuery: needs at least two-part index  or term name, not ''{0}''", index);
      return null;
    } 
    boolean isTableJoin = false;
    String [] foreignTarget ;
    if(idxParts.length > termParts.length ) { 
      
      foreignTarget = idxParts;
    } else {
      foreignTarget = termParts;
      isTableJoin = true;
    }
    //TODO: replace pk column name here with possible list of fields if we allow other columns to be joined?
    ForeignKeys fkey = findForeignKey(dbTable.getPkColumnName(),correlation );
    //System.out.println("CQL2PgJSON.subQuery1FT(): Found foreignKey '" + fkey)

    if (fkey == null) {
      logger.log(Level.SEVERE, "subQuery(): No foreignKey ''{0}'' found", foreignTarget[0]);
      return null;
    }
    if (fkey.getFieldName() ==  null || fkey.getTargetTable() == null) {
      logger.log(Level.SEVERE, "subQuery: Malformed foreignKey section {0}", fkey);
      return null;
    }
    String fkField = fkey.getFieldName(); // tagId
    String fkTable = fkey.getTargetTable();  // tags
    try {

      CQL2PgJSON c = new CQL2PgJSON(foreignTarget[0] + ".jsonb");
      String term = node.getTerm();
      if (term.isEmpty()) {
        term = "\"\"";
      }
      boolean isTermConstant = term.matches(uuidPattern);
      boolean isTableTerm = term.matches(tableNamePattern);
      if(!isTermConstant && !isTableTerm) {
        logger.log(Level.SEVERE, "subQuery: term is not a constant id and not a table unable to continue {0}", fkey);
        return null;
        
      }
      String myField = index2sqlText( dbTable.getTableName() + ".jsonb", "id" );
      String targetField = index2sqlText( foreignTarget[0] + ".jsonb", foreignTarget[1] );
      String correlationJoinClause = "";
      String inKeyword = "";
      String likeClause = null;
      if(isTermConstant ) { 
        correlationJoinClause = " WHERE (" + wrapInLowerUnaccent(myField + "::text") + " = "  + wrapInLowerUnaccent(targetField + "::text") + ")";
        likeClause = isTermConstant ?  " LIKE " + wrapInLowerUnaccent(node.getTerm()) : null;
      } else { 
        inKeyword = " IN ";
      }
      String fld = index2sqlText(c.getjsonField(), foreignTarget[1]);
      StringBuffer result = new StringBuffer( inKeyword + " ( SELECT " + fld + " from " + foreignTarget[0] + correlationJoinClause + ")" );
      if(likeClause != null ) {
        result.append(likeClause);
      }
      return  result.toString();
    } catch (FieldException  e) {
      // We should not get these exceptions, as we construct a valid query above,
      // using a valid schema.
      logger.log(Level.SEVERE, "subQuery Caught an exception", e);
      return null;
    }
  }

  private ForeignKeys findForeignKey(String field, Table targetTable) {
    Iterator<ForeignKeys> it = targetTable.getForeignKeys().iterator();
    while (it.hasNext()) {
      Object e = it.next();
      
      ForeignKeys item = (ForeignKeys) e;
        String target = item.getFieldName();
        if (field.equalsIgnoreCase(target)) {
          return item;
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
      if (!term.matches(uuidPattern)) {
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
      if (!term.matches(uuidPattern)) {
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
    if (!lo.matches(uuidPattern) || !hi.matches(uuidPattern)) {
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

    // special handling of id search (re-use existing code)
    if ("id".equals(index)) {
      return pgId(node);
    }

    IndexTextAndJsonValues vals = getIndexTextAndJsonValues(index);
    DbIndex dbIndex = DbSchemaUtils.getDbIndex(dbSchema, vals.getIndexJson());

    CqlModifiers modifiers = new CqlModifiers(node);
    String comparator = node.getRelation().getBase().toLowerCase();

    switch (comparator) {
    case "=":
      if (CqlTermFormat.NUMBER == modifiers.getCqlTermFormat()) {
        return queryBySql(dbIndex.isOther(), vals, node, comparator, modifiers);
      } else if (CqlAccents.IGNORE_ACCENTS == modifiers.getCqlAccents() &&
          CqlCase.IGNORE_CASE == modifiers.getCqlCase()) {
        return queryByFt(dbIndex.isFt(), vals, node, comparator, modifiers);
      } else {
        return queryByLike(dbIndex.isGin(), vals, node, comparator, modifiers);
      }
    case "adj":
    case "all":
    case "any":
      return queryByFt(dbIndex.isFt(), vals, node, comparator, modifiers);
    case "==":
    case "<>":
      if (CqlTermFormat.STRING == modifiers.getCqlTermFormat()) {
        return queryByLike(dbIndex.isGin(), vals, node, comparator, modifiers);
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
   * @param hasFtIndex
   * @param vals
   * @param node
   * @param comparator
   * @param modifiers
   * @return
   * @throws QueryValidationException
   */
  private String queryByFt(boolean hasFtIndex, IndexTextAndJsonValues vals, CQLTermNode node, String comparator, CqlModifiers modifiers) throws QueryValidationException {

    String index = vals.getIndexText();

    if (!hasFtIndex) {
      logger.log(Level.WARNING, "Doing FT search without FT index {0}", index);
    }

    if (CqlAccents.RESPECT_ACCENTS == modifiers.getCqlAccents()) {
      logger.log(Level.WARNING, "Ignoring /respectAccents modifier for FT search {0}", index);
    }

    if (CqlCase.RESPECT_CASE == modifiers.getCqlCase()) {
      logger.log(Level.WARNING, "Ignoring /respectCase modifier for FT search {0}", index);
    }

    // Clean the term. Remove stand-alone ' *', not valid word.
    String term = node.getTerm().replaceAll(" +\\*", "").trim();
    if (term.equals("*")) {
      return "true";
    }
    if (term.equals("")) {
      return index + " ~ ''";
    }
    String[] words = term.split("\\s+");
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
    // "simple" dictionary only does lower_casing, so need f_unaccent
    String sql = "to_tsvector('simple', f_unaccent(" + index + ")) "
      + "@@ to_tsquery('simple', f_unaccent('" + tsTerm + "'))";

    logger.log(Level.FINE, "index {0} generated SQL {1}", new Object[] {index, sql});
    return sql;
  }

  /**
   * Create an SQL expression using LIKE query syntax.
   *
   * @param hasGinIndex
   * @param vals
   * @param node
   * @param comparator
   * @param modifiers
   * @return
   */
  private String queryByLike(boolean hasGinIndex, IndexTextAndJsonValues vals, CQLTermNode node, String comparator, CqlModifiers modifiers) {

    String index = vals.getIndexText();

    if (!hasGinIndex) {
      logger.log(Level.WARNING, "Doing LIKE search without GIN index for {0}", index);
    }

    String likeOperator = comparator.equals("<>") ? " NOT LIKE " : " LIKE ";
    String like = "'" + Cql2SqlUtil.cql2like(node.getTerm()) + "'";
    String indexMatch = wrapInLowerUnaccent(index) + likeOperator + wrapInLowerUnaccent(like);
    String sql = null;
    if (modifiers.getCqlAccents() == CqlAccents.IGNORE_ACCENTS && modifiers.getCqlCase() == CqlCase.IGNORE_CASE) {
      sql = indexMatch;
    } else {
      sql = indexMatch + " AND " +
        wrapInLowerUnaccent(index, modifiers) + likeOperator + wrapInLowerUnaccent(like, modifiers);
    }

    logger.log(Level.FINE, "index {0} generated SQL {1}", new Object[] {index, sql});
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

    if (!hasIndex) {
      logger.log(Level.WARNING, "Doing SQL query without index for {0}", index);
    }

    if (comparator.equals("==")) {
      comparator = "=";
    }
    String term = "'" + Cql2SqlUtil.cql2like(node.getTerm()) + "'";
    if (CqlTermFormat.NUMBER.equals(modifiers.getCqlTermFormat())) {
      index = "(" + index + ")::numeric";
      term = node.getTerm();
    }
    String sql = index + " " + comparator + term;

    logger.log(Level.FINE, "index {0} generated SQL {1}", new Object[] {index, sql});
    return sql;
  }

}
