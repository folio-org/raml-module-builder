package org.folio.rest.tools.plugins;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.folio.rest.annotations.ElementsNotNull;
import org.folio.rest.annotations.ElementsPattern;
import org.jsonschema2pojo.GenerationConfig;
import org.jsonschema2pojo.Jackson2Annotator;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.sun.codemodel.JAnnotationUse;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JFieldVar;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author shale
 * used to annotate custom json fields,
 * see https://github.com/folio-org/raml-module-builder#json-schema-fields
 *
 * this is called since we set the generator configuration with the following (in GenerateRunner.java):
 *     Map<String, String> config = new HashMap<>();
 *     config.put("customAnnotator", "ramltojaxrs.resources.CustomTypeAnnotator");
 *     configuration.setJsonMapperConfiguration(config);
 */
public class CustomTypeAnnotator extends Jackson2Annotator {

  private static final Logger log = Logger.getLogger(CustomTypeAnnotator.class.getName());

  private static final String[] DEFAULT_CUSTOM_FIELD = new String[]
    {"{\"fieldname\":\"readonly\",\"fieldvalue\":true,\"annotation\":{\"type\":\"javax.validation.constraints.Null\"}}"};

  private static String [] schemaCustomFields = DEFAULT_CUSTOM_FIELD;

  private static final String REGEXP  = "regexp";
  private static final String TYPE    = "type";
  private static final String ITEMS   = "items";
  private static final String NOT     = "not";
  private static final String NULL    = "null";
  private static final String ARRAY   = "array";
  private static final String STRING  = "string";
  private static final String PATTERN = "pattern";

  private Table<String, Object, JsonObject> annotationLookUp = HashBasedTable.create();

  private Map<String, JsonObject> fields2annotate = new HashMap<>();

  public CustomTypeAnnotator(GenerationConfig generationConfig) {
    //called once for each json schema defined
    super(generationConfig);
    //load into a table the custom json schema fields
    for (int j = 0; j < schemaCustomFields.length; j++) {
      JsonObject jo = new JsonObject(schemaCustomFields[j]);
      String fieldName = jo.getString("fieldname");
      Object fieldValue = jo.getValue("fieldvalue");
      JsonObject annotation = jo.getJsonObject("annotation");
      if(annotationLookUp.get(fieldName, fieldValue) == null){
        annotationLookUp.put(fieldName , fieldValue, annotation);
        log.info("Loading custom field " + fieldName + " with value " + fieldValue + " with annotation " + annotation.encode());
      }
    }
  }

  @Override
  public void propertyInclusion(JDefinedClass clazz, JsonNode schema) {
    super.propertyInclusion(clazz, schema);
    //check if the schema we are currently processing is using any of the
    //custom json schema fields
    JsonNode root = schema.get("properties");
    if(root != null){
      root.fields().forEachRemaining( entry -> {
        String fieldName = entry.getKey();
        JsonNode fieldProps = entry.getValue();
        if(fieldProps != null){
          fieldProps.fields().forEachRemaining( prop -> {
            JsonObject annotationToUse = getValue(prop.getKey(), prop.getValue());
            if(annotationToUse != null){
              fields2annotate.put(fieldName, annotationToUse);
              log.info(clazz.name() + " is using " + fieldName);
            }
          });
        }
      });
    }
  }

  @Override
  public void propertyField(JFieldVar field, JDefinedClass clazz, String propertyName, JsonNode propertyNode) {
    super.propertyField(field, clazz, propertyName, propertyNode);

    // Optionally annotates arrays with ElementsNotNull and ElementsPattern
    if(isArray(propertyNode)) {
      if(isItemsNotNull(propertyNode)) {
        field.annotate(ElementsNotNull.class);
      }
      Optional<String> pattern = getPattern(propertyNode);
      if(pattern.isPresent()) {
        field.annotate(ElementsPattern.class).param(REGEXP, pattern.get());
      }
    }

    JsonObject annotation = fields2annotate.get(propertyName);
    if(annotation != null){
      String annotationType = annotation.getString("type");
      JsonArray annotationMembers = annotation.getJsonArray("values");
      log.info("Attempting to annotate " + propertyName +
        " with " + annotationType);
      JClass annClazz = null;
      try {
        annClazz = new JCodeModel().ref(Class.forName(annotationType));
      } catch (ClassNotFoundException e) {
        log.log(Level.SEVERE, "annotation of type " + annotationType + " which is used on field "
            + propertyName + " can not be found (class not found)......");
        throw new RuntimeException(e);
      }
      //annotate the field with the requested annotation
      JAnnotationUse ann = field.annotate(annClazz);
      if(annotationMembers != null){
        //add members to the annotation if they exist
        //for example for Size, min and max
        int memberCount = annotationMembers.size();
        for (int i = 0; i < memberCount; i++) {
          //a member is something like {"max", 5}
          JsonObject member = annotationMembers.getJsonObject(i);
          member.getMap().entrySet().forEach( entry -> {
            String memberKey = entry.getKey();
            Object memberValue = entry.getValue();
            //find the type of the member value so we can create it correctly
            String valueType = memberValue.getClass().getName();
            if(valueType.toLowerCase().endsWith("string")){
              ann.param(memberKey, (String)memberValue);
            }
            else if(valueType.toLowerCase().endsWith("integer")){
              ann.param(memberKey, (Integer)memberValue);
            }
            else if(valueType.toLowerCase().endsWith("boolean")){
              ann.param(memberKey, (Boolean)memberValue);
            }
            else if(valueType.toLowerCase().endsWith("double")){
              ann.param(memberKey, (Double)memberValue);
            }
          });
        }
      }
    }
  }

  /**
   * Set the JSON schemas of custom fields.
   * @param customFields the semicolon separated schemas, or null for default custom fields.
   */
  public static void setCustomFields(String customFields) {
    if (customFields != null) {
      schemaCustomFields = customFields.split(";");
    }
  }

  public static void setCustomFields(String[] customFields) {
    if (customFields.length == 1 && "".equals(customFields[0])) {
      return;  // { "" } indicates to use the default custom fields
    }
    schemaCustomFields = customFields;
  }

  private JsonObject getValue(String key, JsonNode value){
    if(value.isTextual()){
      return annotationLookUp.get(key, value.asText());
    }
    else if(value.isBoolean()){
      return annotationLookUp.get(key, value.asBoolean());
    }
    else if(value.isDouble()){
      return annotationLookUp.get(key, value.asDouble());
    }
    else if(value.isIntegralNumber() || value.isInt()){
      return annotationLookUp.get(key, value.asInt());
    }
    else{
      return null;
    }
  }

  private boolean isItemsNotNull(JsonNode propertyNode) {
    if (propertyNode.has(ITEMS)) {
      JsonNode itemNode = propertyNode.get(ITEMS);
      if (itemNode.has(NOT)) {
        JsonNode notNode = itemNode.get(NOT);
        if(notNode.has(TYPE) && NULL.equals(notNode.get(TYPE).asText())) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean isArray(final JsonNode propertyNode) {
    return propertyNode.has(TYPE) && ARRAY.equals(propertyNode.get(TYPE).asText());
  }

  private Optional<String> getPattern(final JsonNode propertyNode) {
    if (propertyNode.has(ITEMS)) {
      JsonNode itemNode = propertyNode.get(ITEMS);
      if (itemNode.has(TYPE) && STRING.equals(itemNode.get(TYPE).asText()) && itemNode.has(PATTERN)) {
        return Optional.of(itemNode.get(PATTERN).asText());
      }
    }
    return Optional.empty();
  }

}
