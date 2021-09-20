package org.folio.rest.persist.Criteria;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.folio.rest.persist.PostgresClient;

/**
 *
 * <pre>
 * {@code
 * new Criteria().addField("'personal'").addField("'lastName'")
 *   .setOperation("=").setVal("O'Kapi").toString()
 * }</pre>
 * returns "(jsonb->'personal'->>'lastName') =  'O''Kapi'".
 *
 * <pre>
 * {@code
 * new Criteria().addField("'rush'").setOperation("IS TRUE").setVal(null).toString()
 * }</pre>
 * returns "(jsonb->'rush') IS TRUE  ".
 *
 * <p>Note: For best performance do not use
 * {@code new Criteria().addField("'id'").setOperation("=").setVal(someId)} because it
 * accesses {@code table.jsonb->>'id'}.
 * Use PostgresClient.getById or PgUtil.getById (or any of the other primary key id taking methods)
 * instead, or use
 * <pre>
 * {@code new Criteria().addField("id").setJSONB(false).setOperation("=").setVal(someId)}
 * </pre>
 * to access the primary key field {@code table.id}.
 */
public class Criteria {

  /**
   * Convenience consts - can use a regular string to represent the operator
   */
  private static final String OP_NOT = "NOT"; //[{"field":"'po_line_status'->>'value'","value":"fa(l|t)se","op":"SIMILAR TO"}, {"op":"NOT"}]
  private static final String ARRAY_FROM_CLAUSE = "jsonb_array_elements";//("jsonb->'fund_distributions'[]->'amount'->>'sum'")
  private static final String GET_JSON_FIELD = "->";
  private static final String GET_VALUE_FIELD = "->>";
  private static final Pattern JSON_OPS = Pattern.compile("@>|#>|#>>|<@", Pattern.CASE_INSENSITIVE);

  String alias = null;

  boolean joinON = false;

  String column;
  /**
   * field - in the format of column_name -> field -> subfield ->>
   * subfield_value for example: "'price' -> 'po_currency' ->> 'value'"
   */
  List<String> field = new ArrayList<>();
  String val;

  String operation;

  From from;
  Select select;

  boolean isJSONB = true;

  boolean isNotQuery = false;
  boolean isArray = false;

  boolean isJsonOp = false;

  String arrayField = null;

  /**
   * Returns the SQL string.
   */
  @Override
  public String toString() {
    return wrapClause();
  }

  private String wrapClause() {
    if (operation != null && !field.isEmpty()) {

      if (JSON_OPS.matcher(operation).find()) {
        isJsonOp = true;
      }

      populateSnippet();

      String clause = wrapField() + " " + operation + " " + wrapVal();
      if (isNotQuery()) {
        return "( " + OP_NOT + " " + clause + ")";
      }
      return clause;
    } else if (alias != null) {
      populateSnippet();
      return wrapField();
    }
    return "";
  }

  private void populateSnippet() {
    setArrayField();
    createSelectSnippet();
    createFromSnippet();
  }

  private void setArrayField() {
    if (arrayField == null && isArray) {
      arrayField = field.get(0);
    }
  }

  private void createFromSnippet() {
    if (isArray()) {
      from = new From();
      from.setSnippet(ARRAY_FROM_CLAUSE + "("
        + PostgresClient.DEFAULT_JSONB_FIELD_NAME + GET_JSON_FIELD + arrayField + ")");
      from.setAsValue(field.get(0).replace("'", "")); //remove ''
    }
  }

  private void createSelectSnippet() {
    if (isArray()) {
      select = new Select();
      //replace surrounding '' from the field name
      select.setSnippet(field.get(0).replace("'", ""));
    }
  }

  private String wrapField() {
    if (isJSONB) {
      return "(" + addPrefix() + field2String() + ")";
    } else if (alias != null) {
      return alias + "." + field2String() + "::\"\"";
    }
    return field2String();
  }

  private String addPrefix() {

    String prefix = PostgresClient.DEFAULT_JSONB_FIELD_NAME;

    if (alias != null) {
      prefix = alias + "." + prefix;
    }

    if (from != null) {
      //if from set and has a " AS " clause - use that clause as the field alias to
      //operate on
      prefix = from.getAsValue();
    }
    if (isJsonOp) {
      return prefix + GET_JSON_FIELD;
    }
    //Matcher m = valuePattern.matcher(field);
    //if (m.find()) {
    if (field.size() > 1) {
      return prefix + GET_JSON_FIELD;
    } else {
      // top level field criteria
      if (isArray()) {
        return "";
      } else {
        return prefix + GET_VALUE_FIELD;
      }
    }
  }

  /**
   * Added fields in arrayList to a string - prefix and -> / ->> added in
   * addPrefix()
   *
   * @return
   */
  private String field2String() {
    StringBuilder sb = new StringBuilder();
    int size = field.size();
    if (size == 1 && isArray()) {
      //query where claue should look like -
      return from.asValue;
    }
    if (size == 1) {
      return field.get(0);
    }
    for (int i = 0; i < size; i++) {
      if (isArray()) {
        //skip first field if array - included in FROM clause with an AS
        if (i == 0) {
          i++;
        }
      }
      sb.append(field.get(i));
      if (i + 2 == size) {
        //add final arrow either -> or ->>
        if (!isJsonOp) {
          sb.append("->>");
        } else {
          sb.append("->");
        }
      } else if (i + 1 < size) {
        //add ->
        sb.append("->");
      }
    }
    return sb.toString();
  }

  /**
   * Wrap val into single quotes, mask any containing single quote by duplicating it.
   * Return empty string of val is null.
   */
  private String wrapVal() {
    if (val == null) {
      // val may be null if unused, for example when operator = "IS NOT NULL"
      return "";
    }
    return "'" + val.replace("'", "''") + "'";
  }

  public Criteria addField(String field) {
    this.field.add(field);
    return this;
  }

  public String getVal() {
    return val;
  }

  /**
   * Set the string value, use null if unused. It will always wrap val into single quotes
   * and duplicate any containing single quotes to create a proper SQL string constant.
   */
  public Criteria setVal(String val) {
    this.val = val;
    return this;
  }

  /**
   * operation - in the format of "=" or "like"
   */
  public Criteria setOperation(String operation) {
    this.operation = operation;
    return this;
  }

  /**
   * prefix the criteria with an alias - for example, if the query does
   * something like FROM table1 t1 then we need to prefix the jsonb for example
   * like t1.jsonb
   */
  public Criteria setAlias(String alias) {
    this.alias = alias;
    return this;
  }

  public String getAlias() {
    return this.alias;
  }

  /**
   * set this to false if not a jsonb field criteria. For example: the _id is
   * not in the jsonb object hence would be false if criteria based on _id
   */
  public Criteria setJSONB(boolean isJSONB) {
    this.isJSONB = isJSONB;
    return this;
  }

  public boolean isNotQuery() {
    return isNotQuery;
  }

  public Criteria setNotQuery(boolean isNotQuery) {
    this.isNotQuery = isNotQuery;
    return this;
  }

  public boolean isArray() {
    return isArray;
  }

  public Criteria setArray(boolean isArray) {
    this.isArray = isArray;
    return this;
  }

  public From getFrom() {
    return from;
  }

  public Select getSelect() {
    return select;
  }

}
