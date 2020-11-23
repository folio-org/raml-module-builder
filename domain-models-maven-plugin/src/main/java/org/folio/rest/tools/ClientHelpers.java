package org.folio.rest.tools;

import io.vertx.core.json.JsonObject;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author shale
 *
 */
public class ClientHelpers {

  private static final Logger log = Logger.getLogger(ClientHelpers.class.getName());
  private static ObjectMapper mapper = new ObjectMapper();

  public static String pojo2json(Object entity) throws Exception {
    if (entity != null) {
      if (entity instanceof JsonObject) {
        return ((JsonObject) entity).encode();
      } else {
        try {
          return mapper.writeValueAsString(entity);
        } catch (JsonProcessingException e) {
          log.log(Level.SEVERE, e.getMessage(), e);
          throw e;
        }
      }
    }
    throw new NullPointerException("Entity can not be null");
  }

}
