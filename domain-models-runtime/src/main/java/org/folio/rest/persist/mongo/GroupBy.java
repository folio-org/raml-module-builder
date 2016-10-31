package org.folio.rest.persist.mongo;

import io.vertx.core.json.JsonObject;

/**
 *
 */
public class GroupBy {

  private static final String GROUP_BY_ID         = "_id";
  private static final String GROUP_BY_IDENTIFIER = "$group";
  private static final String SUM_IDENTIFIER      = "$sum";
  private static final String AVG_IDENTIFIER      = "$avg";
  private static final String MAX_IDENTIFIER      = "$max";
  private static final String MIN_IDENTIFIER      = "$min";
  private static final String COUNT_FIELD         = "count";
  private static final String PUSH                = "$push";
  private static final String PREFIX              = "$";

  private String idFieldName = null;

  private JsonObject groupBy = new JsonObject();
  private JsonObject groupByRoot = new JsonObject();

  public GroupBy(){
    groupBy.put(GROUP_BY_IDENTIFIER, new JsonObject());
    groupByRoot = groupBy.getJsonObject(GROUP_BY_IDENTIFIER);
    groupByRoot.put(GROUP_BY_ID, new JsonObject());
    this.idFieldName = GROUP_BY_ID;
  }

  /**
   * you can specify an _id value of null to calculate accumulated values for all the input documents as a whole.<br>
   * Will return one group for all documents.
   * @param idField
   */
  public GroupBy(String idField){
    groupBy.put(GROUP_BY_IDENTIFIER, new JsonObject());
    groupByRoot = groupBy.getJsonObject(GROUP_BY_IDENTIFIER);
    if(idField == null){
      groupByRoot.put(GROUP_BY_ID, "null");
    }
    else{
      groupByRoot.put(idField, new JsonObject());
    }
    this.idFieldName = idField;
  }

  /**
   * Add a field to group by - multiple fields can be added
   * @param field
   * @return
   */
  public GroupBy addGroupByField(String field){
    JsonObject idField = groupBy.getJsonObject(GROUP_BY_IDENTIFIER).getJsonObject(this.idFieldName);
    idField.put(field, PREFIX+field);
    return this;
  }

  public GroupBy addGroupByField(String alias, String field){
    JsonObject idField = groupBy.getJsonObject(GROUP_BY_IDENTIFIER).getJsonObject(this.idFieldName);
    idField.put(alias, PREFIX+field);
    return this;
  }

  /**
   * Use this for pivoting data, group by a field and return all values from documents for different field. For example<br>
   * pivot data in a books collection to have titles grouped by authors.
   * for example: field = authors, valueAlias = bookTitles , valueField = titles
   * @param field
   * @param valueAlias
   * @param valueField
   * @return
   */
  public GroupBy addGroupByField(String field, String valueAlias, String valueField){
    JsonObject idField = groupBy.getJsonObject(GROUP_BY_IDENTIFIER).getJsonObject(this.idFieldName);
    idField.put(field, PREFIX+field);
    JsonObject job = new JsonObject();
    job.put(PUSH, PREFIX+valueField);
    groupByRoot.put(valueAlias, job);
    return this;
  }



  /**
   * The field passed in must be of type date
   * for example:
   *  _id : { month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } },
   *  will create groups with the month, day and year as the triplet for each group
   *  This can be achieved by call addGroupByField three times
   *  with alias month, field_containing_iso_date, enum indicating which part of the date to use
   * @param alias
   * @param field
   * @param date - use this part of the date (year, month, day, etc...) to group by
   * @return
   */
  public GroupBy addGroupByField(String alias, String field, DateEnum date){
    JsonObject idField = groupBy.getJsonObject(GROUP_BY_IDENTIFIER).getJsonObject(this.idFieldName);
    JsonObject job = new JsonObject();
    job.put(date.getValue(), PREFIX+field);
    idField.put(alias, job);
    return this;
  }

  /**
   * The field passed in must be of type date
   * for example:
   *  _id : { month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } },
   *  will create groups with the month, day and year as the triplet for each group
   *  This can be achieved by call addGroupByField three times
   *  with alias month, field_containing_iso_date, enum indicating which part of the date to use
   * @param field
   * @param date - use this part of the date (year, month, day, etc...) to group by
   * @return
   */
  public GroupBy addGroupByField(String field, DateEnum date){
    JsonObject idField = groupBy.getJsonObject(GROUP_BY_IDENTIFIER).getJsonObject(this.idFieldName);
    JsonObject job = new JsonObject();
    job.put(date.getValue(), PREFIX+field);
    idField.put(field, job);
    return this;
  }

  /**
   * return count of items in each group
   * @param alias name of the label for this value
   * @return
   */
  public GroupBy addCount(String alias){
    JsonObject job = new JsonObject();
    job.put(SUM_IDENTIFIER, 1);
    groupByRoot.put(alias, job);
    return this;
  }

  /**
   *
   * @return return count of items in each group
   */
  public GroupBy addCount(){
    JsonObject job = new JsonObject();
    job.put(SUM_IDENTIFIER, 1);
    groupByRoot.put(COUNT_FIELD, job);
    return this;
  }

  /**
   * Returns an average of numerical values. Ignores non-numeric values.
   * @param alias label name of result field
   * @param field field to calculate avg for
   * @return
   */
  public GroupBy addAVGForField(String alias, String field){
    JsonObject job = new JsonObject();
    job.put(AVG_IDENTIFIER, PREFIX+field);
    groupByRoot.put(alias, job);
    return this;
  }

  /**
   * Returns an average of numerical values. Ignores non-numeric values.
   * @param field field to calculate avg for
   * @return
   */
  public GroupBy addAVGForField(String field){
    JsonObject job = new JsonObject();
    job.put(AVG_IDENTIFIER, PREFIX+field);
    groupByRoot.put(field, job);
    return this;
  }

  /**
   * Returns the highest expression value for each group.
   * @param alias label for result
   * @param field to calculate max value for each group
   * @return
   */
  public GroupBy addMAXForField(String alias, String field){
    JsonObject job = new JsonObject();
    job.put(MAX_IDENTIFIER, PREFIX+field);
    groupByRoot.put(alias, job);
    return this;
  }

  /**
   * Returns the highest expression value for each group.
   * @param field to calculate max value for each group
   * @return
   */
  public GroupBy addMAXForField(String field){
    JsonObject job = new JsonObject();
    job.put(MAX_IDENTIFIER, PREFIX+field);
    groupByRoot.put(field, job);
    return this;
  }

  /**
   *
   * @param field
   * @param expression a json object - for example <br> { $multiply: [ "$price", "$quantity" ] }
   *  <br> this would indicate to multiply the price and quantity fields. The max would be calculated on
   *  the result.
   * @return
   */
  public GroupBy addMAXForField(String alias, JsonObject expression){
    JsonObject job = new JsonObject();
    job.put(MAX_IDENTIFIER, expression);
    groupByRoot.put(alias, job);
    return this;
  }

  /**
   * Returns the highest expression value for each group.
   * @param field to calculate min value for each group
   * @return
   */
  public GroupBy addMINForField(String alias, String field){
    JsonObject job = new JsonObject();
    job.put(MIN_IDENTIFIER, PREFIX+field);
    groupByRoot.put(alias, job);
    return this;
  }

  /**
   * Returns the highest expression value for each group.
   * @param field to calculate min value for each group
   * @return
   */
  public GroupBy addMINForField(String field){
    JsonObject job = new JsonObject();
    job.put(MIN_IDENTIFIER, PREFIX+field);
    groupByRoot.put(field, job);
    return this;
  }


  public GroupBy addMINForField(String alias, JsonObject expression){
    JsonObject job = new JsonObject();
    job.put(MIN_IDENTIFIER, expression);
    groupByRoot.put(alias, job);
    return this;
  }

  /**
   * generic ability to add a key - json expression to the group by expression
   * @param field
   * @param expression
   * @return
   */
  public GroupBy addConstraint(String field, JsonObject expression){
    groupByRoot.put(field, expression);
    return this;
  }

  /**
   * generic ability to add a key - string value to the group by expression
   * @param field
   * @param value
   * @return
   */
  public GroupBy addConstraint(String field, String value){
    groupByRoot.put(field, value);
    return this;
  }

  @Override
  public String toString(){
    return groupBy.encodePrettily();
  }

  public JsonObject toJson(){
    return groupBy;
  }

}
