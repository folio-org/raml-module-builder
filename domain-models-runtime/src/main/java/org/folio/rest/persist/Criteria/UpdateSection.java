package org.folio.rest.persist.Criteria;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.primitives.Primitives;


public class UpdateSection {

  ArrayList<String> fieldHierarchy = new ArrayList<>();
  Object value;
  private final Pattern fieldPattern = Pattern.compile("[a-zA-Z0-9_]+");

  /**
   * set the field to update - the field are listed in the generated objects
   * (can be seen from javadocs as well - "@JsonPropertyOrder(value={"sum", "currency"})"
   * Note that the root object should not be included here
   * @param field - in case the field is embedded - add multiple fields in the correct order
   * for example:
   *    Updating the rush section in a Money object - where 'rush' is a field found in the 'status' section (which is a top level field)
   *    addField("status").addField("rush"):
   * this gets mapped for use by the jsonb_set function and produces a '{status, rush}' path entry for the function to use
   * @return
   */
  public UpdateSection addField(String field){
    if (! fieldPattern.matcher(field).matches()) {
      throw new IllegalArgumentException("Field name with illegal character: '" + field + "'");
    }
    fieldHierarchy.add(field);
    return this;
  }

  /**
   * @param o - the value to use to replace the existing value as denoted by the field to update (fieldHierarchy)
   * can be a wrapped primitive Integer / String / etc...
   * can be a JsonObject
   * this depends on what value is to be set in the specified field
   * can be for example a string:
   *    "SOMETHING_NEW"
   *   or a json (in which case a jsonobject should be passed in)
   *   {"value":"SOMETHING_NEW4","desc":"sent to vendor"}
   *
   */
  public void setValue(Object o){
    value = o;
  }

  public String getFieldsString() {

    return "'{" + Joiner.on(", ").join(fieldHierarchy) +"}'";

  }

  /**
   * Return the object o set by setValue(o) encoded as a JSON string. Return null if o is null
   * or if o is neither a JsonObject nor a String nor a primitive type.
   * @return the JSON string
   */
  public String getValue() {
    if (value == null) {
      return null;
    }
    if (value instanceof JsonObject) {
      return ((JsonObject) value).encode();
    }
    if (value instanceof String || value.getClass().isPrimitive() || Primitives.isWrapperType(value.getClass())) {
      return Json.encode(value);
    }
    return null;
  }
}
