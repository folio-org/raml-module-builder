package org.folio.rest.tools;

import io.vertx.core.json.JsonObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author shale
 *
 */
public class ClientHelpers {

  private static final Logger log = LogManager.getLogger(ClientHelpers.class);
  private static ObjectMapper mapper = new ObjectMapper();

  public static String pojo2json(Object entity)  {
    if (entity != null) {
      if (entity instanceof JsonObject) {
        return ((JsonObject) entity).encode();
      } else {
        try {
          return mapper.writeValueAsString(entity);
        } catch (JsonProcessingException e) {
          log.error(e.getMessage(), e);
          throw new IllegalArgumentException(e.getMessage());
        }
      }
    }
    throw new IllegalArgumentException("entity can not be null");
  }

}
