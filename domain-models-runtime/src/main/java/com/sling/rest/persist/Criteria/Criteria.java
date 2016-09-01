package com.sling.rest.persist.Criteria;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sling.rest.persist.PostgresClient;

/**
 * @author shale
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
  public static final String   OP_JSON_LESS_THAN_EQ    = "@>";// contains json with json "field" --> [{"field":"'price'","value":{"sum": "150.0"},"op":"@>"}]
  public static final String   OP_NOT             = "NOT"; //[{"field":"'po_line_status'->>'value'","value":"fa(l|t)se","op":"SIMILAR TO"}, {"op":"NOT"}]
  
  private static final String  ARRAY_FROM_CLAUSE  = "jsonb_array_elements";//("jsonb->'fund_distributions'[]->'amount'->>'sum'")
  
  private static final Pattern STRING_PATTERN     = Pattern.compile("\"[^\"]*\"");
  private static final Pattern BOOLEAN_PATTERN    = Pattern.compile("true|false");
  private static final Pattern NUMERIC_PATTERN    = Pattern.compile("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$");
  private static final Pattern BOOLEAN_OPS        = Pattern.compile("IS TRUE|IS NOT TRUE|IS FALSE|IS NOT FALSE|IS UNKNOWN|IS NOT UNKNOWN",Pattern.CASE_INSENSITIVE);
  private static final Pattern NULL_OPS           = Pattern.compile("IS NOT NULL|IS NULL|ISNULL|NOTNULL",Pattern.CASE_INSENSITIVE);
  private static final Pattern JSON_OPS           = Pattern.compile("@>|#>|#>>|<@",Pattern.CASE_INSENSITIVE);
  private static final Pattern FIELD_PATTERN      = Pattern.compile("'[^']*'");

  private static final Pattern valuePattern       = Pattern.compile("'\\s*?->>\\s*?'");

  private static final String  GET_JSON_FIELD     = "->";
  private static final String  GET_VALUE_FIELD    = "->>";

  String                       column;
  /**
   * field - in the format of column_name -> field -> subfield ->>
   * subfield_value for example: "'price' -> 'po_currency' ->> 'value'"
   */
  List<String>                 field = new ArrayList<String>();
  Object                       value;
  /**
   * operation - in the format of "=" or "like"
   */
  String                operation;

  From                  from; 
  Select                select;   
  /**
   * set this to false if not a jsonb field criteria
   */
  boolean               isJSONB            = true;
  
  boolean               isNotQuery         = false;
  boolean               isArray            = false;
  
  boolean               isJsonOp           = false;
  /**
   * arrays in jsonb are handled differently, need to open up the array
   * and then compare the value in each slot to the requested value, therefore
   * needs special handling. need to set the isArray to true and indicate which part of the field
   * in the requested field path is the array
   * for example: 'a'->'b'->'c' - is the path - if 'a' is an array (list) of items - then set this
   * criteria isArray to true and set the arrayField as 'a' - if no arrayField is set then the 
   * first field will be extracted and used 
   */
  String                arrayField         = null;
  
  static final int             STRING_TYPE        = 1;
  static final int             BOOLEAN_TYPE       = 2;
  static final int             NUMERIC_TYPE       = 3;
  static final int             NULL_TYPE          = 4;
  int                          valueType          = 1;
  
  
  @Override
  public String toString() {
    return wrapClause();
  }

  private String wrapClause(){
    if(operation != null && field != null){
      
      if(JSON_OPS.matcher(operation).find()){
        isJsonOp = true;
      }
      
      setArrayField();
      createSelectSnippet();
      createFromSnippet();
      
      String clause = wrapField() + " " + operation + " " + wrapValue();
      if(isNotQuery){
        return "( "+ OP_NOT +" " + clause + ")";
      }
      return clause;
    }
    return "";
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
    if (isJSONB) {
      String cast = "";
      valueType = getOperationType();
      if(valueType == NUMERIC_TYPE){
        cast = "::numeric";
        return "(" + addPrefix() + field2String() + ")" + cast;
      }
      else if (valueType == BOOLEAN_TYPE){
        cast = "::boolean";
        return "(" + addPrefix() + field2String() + ")" + cast;
      }
      return "(" + addPrefix() + field2String() + ")";
    }
    return field2String();
  }
  
  private String addPrefix() {
    
    String prefix = PostgresClient.DEFAULT_JSONB_FIELD_NAME;
    
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
  private String field2String(){
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
      sb.append(field.get(i));
      if(i+2 == size){
        //add final arrow either -> or ->>
        if(!isJsonOp){
          sb.append("->>");
        }
        else{
          sb.append("->");   
        }
      }
      else if(i+1 < size){
        //add ->
        sb.append("->");
      }
    }
    return sb.toString();
  }
  
  private int getOperationType() {
    String ret = "";
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

  private Object wrapValue() {
    // value may be null for example - field IS NOT NULL criteria
    if (value != null && valueType == STRING_TYPE && isJSONB) {
      if(isArray){
        return " '\"" + value + "\"'";
      }else{
        return " '" + value + "'";
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

  public void setValue(Object value) {
    this.value = value;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public boolean isJSONB() {
    return isJSONB;
  }

  public void setJSONB(boolean isJSONB) {
    this.isJSONB = isJSONB;
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

  public void setArrayField(String arrayField) {
    this.arrayField = arrayField;
  }
  
}
