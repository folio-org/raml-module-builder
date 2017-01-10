package org.folio.rest.tools;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author shale
 *
 */
public class ClientHelpers {

  private static final Logger log = LoggerFactory.getLogger(ClientHelpers.class);
  private static ObjectMapper mapper = new ObjectMapper();

  public static String pojo2json(Object entity) throws Exception {
    if (entity != null) {
      if (entity instanceof JsonObject) {
        return ((JsonObject) entity).encode();
      } else {
        try {
          return mapper.writeValueAsString(entity);
        } catch (JsonProcessingException e) {
          log.error(e);
        }
      }
    }
    throw new Exception("Entity can not be null");
  }

}
