package org.folio.rest.persist.Criteria;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.parser.JsonPathParser;

import io.vertx.core.json.JsonObject;

/**
 *
 *         Criteria c = new Criteria();
 *         c.field = "'price' -> 'po_currency' ->> 'value'";
 *         c.operation = "LIKE";
 *         c.value = "USD";
 *
 *         c.field = "'rush'";
 *         c.operation = "IS TRUE";
 *         c.value = null;
 *
 */
public class Criteria {

  /**
   * Convenience consts - can use a regular string to represent the operator
   */
  public static final String   OP_IS_NOT_NULL     = "IS NOT NULL"; //[{"field":"'ebook_url'","value":null,"op":"IS NOT NULL"}]
  public static final String   OP_IS_NULL         = "IS NULL"; //[{"field":"'ebook_url'","value":null,"op":"IS NULL"}]
  public static final String   OP_IS_TRUE         = "IS TRUE";
  public static final String   OP_IS_NOT_TRUE     = "IS NOT TRUE";
  public static final String   OP_IS_FALSE        = "IS FALSE";
  public static final String   OP_IS_NOT_FALSE    = "IS NOT FALSE";
  public static final String   OP_SIMILAR_TO      = "SIMILAR TO"; // [{"field":"'rush'","value":"ru(s|t)h","op":"SIMILAR TO"}]
  public static final String   OP_NOT_SIMILAR_TO  = "NOT SIMILAR TO";
  public static final String   OP_NOT_EQUAL       = "!=";
  public static final String   OP_EQUAL           = "="; //[{"field":"'rush'","value":"false","op":"="}]
  public static final String   OP_LIKE            = "LIKE"; //[{"field":"'po_line_status'->>'value'","value":"SENT%","op":"like"}]
  public static final String   OP_GREATER_THAN    = ">"; //non-array values only --> [{"field":"'fund_distributions'->'amount'->>'sum'","value":120,"op":">"}]
  public static final String   OP_GREATER_THAN_EQ = ">=";
  public static final String   OP_LESS_THAN       = "<";
  public static final String   OP_LESS_THAN_EQ    = "<=";
  public static final String   OP_JSON_LESS_THAN_EQ = "@>";// contains json with json "field"
                     //field price has json entry matching the value passed in [{"field":"'price'","value":{"sum": "150.0"},"op":"@>"}]
  public static final String   OP_NOT             = "NOT"; //[{"field":"'po_line_status'->>'value'","value":"fa(l|t)se","op":"SIMILAR TO"}, {"op":"NOT"}]
  public static final String   OP_OR              = "OR";
  public static final String   OP_AND             = "AND";

  private static final String  ARRAY_FROM_CLAUSE  = "jsonb_array_elements";//("jsonb->'fund_distributions'[]->'amount'->>'sum'")

  private static final Pattern STRING_PATTERN     = Pattern.compile("\"[^\"]*\"");
  private static final Pattern BOOLEAN_PATTERN    = Pattern.compile("true|false");
  private static final Pattern NUMERIC_PATTERN    = Pattern.compile("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$");
  private static final Pattern BOOLEAN_OPS        = Pattern.compile("IS TRUE|IS NOT TRUE|IS FALSE|IS NOT FALSE|IS UNKNOWN|IS NOT UNKNOWN",Pattern.CASE_INSENSITIVE);
  private static final Pattern NULL_OPS           = Pattern.compile("IS NOT NULL|IS NULL|ISNULL|NOTNULL",Pattern.CASE_INSENSITIVE);
  private static final Pattern JSON_OPS           = Pattern.compile("@>|#>|#>>|<@",Pattern.CASE_INSENSITIVE);
  private static final Pattern FIELD_PATTERN      = Pattern.compile("'[^']*'");
  private static final Pattern VALUE_PATTERN      = Pattern.compile("'\\s*?->>\\s*?'");

  private static final String  GET_JSON_FIELD     = "->";
  private static final String  GET_VALUE_FIELD    = "->>";

  private static final int  STRING_TYPE           = 1;
  private static final int  BOOLEAN_TYPE          = 2;
  private static final int  NUMERIC_TYPE          = 3;
  private static final int  NULL_TYPE             = 4;

  private static final Map<String, JsonObject> schemaCache = new HashMap<>();

  String alias                                = null;

  boolean joinON                                  = false;

  String forceCast                               = null;

  int valueType                                   = 1;

  String column;
  /**
   * field - in the format of column_name -> field -> subfield ->>
   * subfield_value for example: "'price' -> 'po_currency' ->> 'value'"
   */
  List<String>                 field = new ArrayList<>();
  Object                       value;

  String                operation;

  From                  from;
  Select                select;

  boolean               isJSONB            = true;

  boolean               isNotQuery         = false;
  boolean               isArray            = false;

  boolean               isJsonOp           = false;

  String                arrayField         = null;

  JsonObject            schema4validation  = null;

  public Criteria() {
  }

  public Criteria(String schema) throws Exception {
    schema4validation = schemaCache.get(schema);
    if(schema4validation == null){
      JsonObject jo = new JsonObject(
        IOUtils.toString(getClass().getClassLoader().getResourceAsStream(schema), "UTF-8"));
      schemaCache.put(schema, jo);
      schema4validation = jo;
    }
  }

  @Override
  public String toString() {
    return wrapClause();
  }

  private String wrapClause(){
    if(operation != null && field != null){

      if(JSON_OPS.matcher(operation).find()){
        isJsonOp = true;
      }

      populateSnippet();

      String clause = wrapField() + " " + operation + " " + wrapValue();
      if(isNotQuery){
        return "( "+ OP_NOT +" " + clause + ")";
      }
      return clause;
    }
    else if(alias != null){
      populateSnippet();
      return wrapField();
    }
    return "";
  }

  private void populateSnippet(){
    setArrayField();
    createSelectSnippet();
    createFromSnippet();
  }

  private void setArrayField(){
    if(arrayField == null && isArray){
      //isArray set - assume first field is the array field
      //the fields are supposed to be surrounded by '' - so find
      //first instance of un-escaped ''
/*      Matcher m = FIELD_PATTERN.matcher(field);
      if (m.find()) {
        //this should be the first field
        arrayField = m.group();
      }*/
      arrayField = field.get(0);
    }
    //arrayField was set manually or calculated automatically successfully then
    //split the field into two - the arrayField - already populated - and the new
    //field which will comtain only the suffix of the original field
/*    if(arrayField != null){
      int idx = -1;
      if((idx = field.indexOf(arrayField)) != -1){
        field = field.substring(idx + arrayField.length() + 1);
      }
    }*/
  }

  private void createFromSnippet(){
    if(isArray){
      from = new From();
      from.setSnippet(ARRAY_FROM_CLAUSE+"(" +
          PostgresClient.DEFAULT_JSONB_FIELD_NAME + GET_JSON_FIELD + arrayField + ")");
      from.setAsValue(field.get(0).replaceAll("^'|'$", "")); //remove ''
    }
  }

  private void createSelectSnippet(){
    if(isArray){
      select = new Select();
      //replace surrounding '' from the field name
      select.setSnippet(field.get(0).replaceAll("^'|'$", ""));
    }
  }

  private String wrapField() {
    String cast = "";
    if (isJSONB) {
      valueType = getOperationType();
      if(valueType == NUMERIC_TYPE){
        cast = "::numeric";
        return "(" + addPrefix() + field2String(false) + ")" + cast;
      }
      else if (valueType == BOOLEAN_TYPE){
        cast = "::boolean";
        return "(" + addPrefix() + field2String(false) + ")" + cast;
      }
      return "(" + addPrefix() + field2String(false) + ")";
    }
    else if(alias != null){
      if(forceCast != null){
        cast = forceCast;
      }
      return alias + "." + field2String(false) + "::\"" + cast + "\"";
    }
    return field2String(false);
  }

  private String addPrefix() {

    String prefix = PostgresClient.DEFAULT_JSONB_FIELD_NAME;

    if(alias != null){
      prefix = alias + "." + prefix;
    }

    if(from != null){
      //if from set and has a " AS " clause - use that clause as the field alias to
      //operate on
      prefix = from.getAsValue();
    }
    if(isJsonOp){
      return prefix + GET_JSON_FIELD;
    }
    //Matcher m = valuePattern.matcher(field);
    //if (m.find()) {
    if (field.size() > 1) {
      return prefix + GET_JSON_FIELD;
    } else {
      // top level field criteria
      if(isArray){
        return "";
      }
      else{
        return prefix + GET_VALUE_FIELD;
      }
    }
  }

  /**
   * Added fields in arrayList to a string - prefix and -> / ->> added in addPrefix()
   * @return
   */
  private String field2String(boolean validate){
    StringBuilder sb = new StringBuilder();
    int size = field.size();
    if(size == 1 && isArray){
      //query where claue should look like -
      return from.asValue;
    }
    if(size == 1){
      return field.get(0);
    }
    for (int i = 0; i < size; i++) {
      if(isArray){
        //skip first field if array - included in FROM clause with an AS
        if(i==0){
          i++;
        }
      }
      String fVal = field.get(i);
      if(validate){
        //remove '' from field name when using schema to
        //detect type
        fVal = fVal.substring(1, fVal.length()-1);
      }
      sb.append(fVal);
      if(i+2 == size){
        //add final arrow either -> or ->>
        if(!isJsonOp){
          if(validate){
            sb.append(".");
          }else{
            sb.append("->>");
          }
        }
        else{
          if(validate){
            sb.append(".");
          }else{
            sb.append("->");
          }
        }
      }
      else if(i+1 < size){
        //add ->
        if(validate){
          sb.append(".");
        }else{
          sb.append("->");
        }
      }
    }
    return sb.toString();
  }

  private int getOperationType() {
    String ret = "";
    if(schema4validation != null){
      String val = new JsonPathParser(schema4validation, true).
        getValueAt(field2String(true).replaceAll("->>", ".").replaceAll("->", ".")+".type").toString();
      if("string".equals(val)){
        return STRING_TYPE;
      }
      else if("boolean".equals(val)){
        return BOOLEAN_TYPE;
      }
      else if("null".equals(val)){
        return NULL_TYPE;
      }
      else{
        return NUMERIC_TYPE;
      }
    }
    else{
      if (value == null) {
        //no value passed this may be ok, try to guess value type via the operator
        if (isBooleanOp()) {
          //using a boolean operator - values are not relevant in this use case
          return BOOLEAN_TYPE;
        }
        else if(isNULLOp()){
          //using a null operator - values are not relevant in this use case
          return NULL_TYPE;
        }
        //criteria is using a regular operator to compare to a null value - that is ok
        return STRING_TYPE;
      } else {
        Matcher m = NUMERIC_PATTERN.matcher(value.toString());
        if (m.matches()) {
          return NUMERIC_TYPE;
        } else {
          m = BOOLEAN_PATTERN.matcher(value.toString());
          if (m.matches()) {
            return BOOLEAN_TYPE;
          }
        }
      }
      return STRING_TYPE;
    }
  }

  private Object wrapValue() {

    // value may be null for example - field IS NOT NULL criteria
    if (value != null && valueType == STRING_TYPE && isJSONB) {
      if(isArray){
        return " '" + value + "'";
      }else{
        if(isWrappedInQuotes((String)value)){
          return value;
        }
        else{
          return " '" + value + "'";
        }
      }
    }
    if (value == null && valueType == STRING_TYPE) {
      //null is a legit json value, example to check if a string field is null
      //- so leave it in such a case field a = null
      return null;
    }
    if (value == null){
      //use case here is value = null and type is either boolean or numeric
      //in such a case - for example - IS TRUE - remove the null so that it doesnt read
      //field IS TRUE null
      return "";
    }
    return value;
  }

  private boolean isBooleanOp() {
    if (operation != null) {
      return BOOLEAN_OPS.matcher(operation).find();
    }
    return false;
  }

  private boolean isNULLOp() {
    if (operation != null) {
      return NULL_OPS.matcher(operation).find();
    }
    return false;
  }

  private boolean isWrappedInQuotes(String value){
    try {
      if(value.charAt(0) == '\'' && value.charAt(value.length()-1) == '\''){
        return true;
      }
    } catch (Exception e) {
      return false;
    }
    return false;
  }

  public List<String> getField() {
    return field;
  }

  public Criteria addField(String field) {
    this.field.add(field);
    return this;
  }

  public Object getValue() {
    return value;
  }

  public Criteria setValue(Object value) {
    this.value = value;
    return this;
  }

  public String getOperation() {
    return operation;
  }

  /**
   * operation - in the format of "=" or "like"
   */
  public Criteria setOperation(String operation) {
    this.operation = operation;
    return this;
  }

  /** prefix the criteria with an alias - for example,
   * if the query does something like FROM table1 t1
   * then we need to prefix the jsonb for example like
   * t1.jsonb */
  public Criteria setAlias(String alias){
    this.alias = alias;
    return this;
  }

  public String getAlias(){
    return this.alias;
  }

  public boolean isJSONB() {
    return isJSONB;
  }

  /**
   * set this to false if not a jsonb field criteria
   * For example: the _id is not in the jsonb object hence would be false if criteria based on _id
   */
  public Criteria setJSONB(boolean isJSONB) {
    this.isJSONB = isJSONB;
    return this;
  }

  public boolean isNotQuery() {
    return isNotQuery;
  }

  public void setNotQuery(boolean isNotQuery) {
    this.isNotQuery = isNotQuery;
  }

  public boolean isArray() {
    return isArray;
  }

  public void setArray(boolean isArray) {
    this.isArray = isArray;
  }

  public String getArrayField() {
    return arrayField;
  }

  /**
   * arrays in jsonb are handled differently, need to open up the array
   * and then compare the value in each slot to the requested value, therefore
   * needs special handling. need to set the isArray to true and indicate which part of the field
   * in the requested field path is the array.<br>
   * for example: 'a'->'b'->'c' - is the path - if 'a' is an array (list) of items - then set this
   * criteria isArray to true and set the arrayField as 'a' - if no arrayField is set then the
   * first field will be extracted and used as the array field
   */
  public void setArrayField(String arrayField) {
    this.arrayField = arrayField;
  }

  public boolean isJoinON() {
    return joinON;
  }

  /**
   * if set to true, this criteria is being used to create a criteria to
   * compare to another criteria to join on
   * in this generate an ON instead of a WHERE
   * */
  public Criteria setJoinON(boolean joinON) {
    this.joinON = joinON;
    return this;
  }

  public String getForceCast() {
    return forceCast;
  }

  /**
   * force cast is needed when using the criteria as part of a
   * join statement - for example, in a case where we compare the id
   * of records (not a jsonb field) to another table where the id is in
   * a jsonb field (as a number, varchar (uuid), etc.) then we need
   * to force cast the id from the non-jsonb field to the appropriate type
   * so that the comparison will work
   * generate the "varchar" part of the statement below:
   *  ON groups._id::"varchar" = (join_table.jsonb->>'groupId')
   *  in the above forceCast = varchar
   */
  public Criteria setForceCast(String forceCast) {
    this.forceCast = forceCast;
    return this;
  }

  public From getFrom() {
    return from;
  }

  public Select getSelect() {
    return select;
  }

}
