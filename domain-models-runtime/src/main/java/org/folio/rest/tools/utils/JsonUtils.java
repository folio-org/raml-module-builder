package org.folio.rest.tools.utils;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author shale
 *
 */
public class JsonUtils {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Logger log = LogManager.getLogger(JsonUtils.class);


  public static String entity2String(Object entity){
    String obj = null;
    if(entity != null){
      if(entity instanceof JsonObject){
        //json object
        obj = ((JsonObject) entity).encode();
      }
      else if(entity instanceof List<?>){
        obj =  new JsonArray((List)entity).encode();
      }
      else{
        try {
          //pojo
          obj = MAPPER.writeValueAsString(entity);
        } catch (JsonProcessingException e) {
          log.error(e.getMessage() , e);
        }
      }
    }
    return obj;
  }

  /**
   * transform a pojo to a mongo json query by flattening out the object
   * for example a list of parameters with a key in the list called 'key' will be transformed into
   * parameters.key
   * @param entity pojo to transform into a query
   * @param removeFieldsPrefixes fields starting with this prefix will not be included in the returned json object
   * @return
   */
  public static JsonObject entity2Json(Object entity, String[] removeFieldsPrefixes){

    if(entity == null){
      return null;
    }

    //Create a regex of excluded entries in the json so that the json created from the pojo does not
    //include them
    Pattern excludes = null;

    if(removeFieldsPrefixes != null){
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < removeFieldsPrefixes.length; i++) {
        sb.append( removeFieldsPrefixes[i].replace(".", "\\.") ).append(".*");
        if(i+1<removeFieldsPrefixes.length){
          sb.append("|");
        }
      }
      excludes = Pattern.compile(sb.toString(), Pattern.CASE_INSENSITIVE);
    }

    try {
      JsonObject result = new JsonObject( MAPPER.writeValueAsString(entity) );
      JsonObject processed = new JsonObject();
      entity2JsonInternal(result, "", processed, excludes);
      return processed;
    } catch (JsonProcessingException e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static void entity2JsonInternal(Object obj, String key, JsonObject result, Pattern excludes){
    String prefix = key;
    Consumer<Map.Entry<String,Object>> consumer = entry -> {
      String key1 = entry.getKey();
      Object value1 = entry.getValue();
      String newPrefix = new StringBuilder(prefix).append(key1).append(".").toString();
      if(value1 instanceof JsonObject){
        entity2JsonInternal(value1, newPrefix, result, excludes);
      }
      else if (value1 instanceof JsonArray){
        int size = ((JsonArray)value1).size();
        for(int i=0; i<size; i++){
          Object val = ((JsonArray)value1).getValue(i);
          entity2JsonInternal(val, newPrefix, result, excludes);
        }
      }
      else{
        String path = prefix+key1;
        if(excludes == null || !excludes.matcher(path).find()){
          result.put(path, value1);
        }
      }
    };
    ((Iterable)obj).forEach(consumer);
  }

  /**
   * transform a pojo to a mongo json query by flattening out the object
   * for example a list of parameters with a key in the list called 'key' will be transformed into
   * parameters.key
   * @param entity
   * @return
   */
  public static JsonObject entity2Json(Object entity){
    return entity2Json(entity, null);
  }

  public static JsonObject entity2JsonNoLastModified(Object entity){
    JsonObject q = entity2Json(entity, new String[]{"last_modified"});
    return q;
  }

}
