package org.folio.dbschema;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Date;

import org.folio.dbschema.util.DateDeserializer;
import org.folio.dbschema.util.DateSerializer;

/**
 * @author shale
 *
 */
public final class ObjectMapperTool {
  private static final ObjectMapper DEFAULT_MAPPER = createDefaultMapper();
  private static final ObjectMapper MAPPER = createDefaultMapper();

  private ObjectMapperTool() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  public static ObjectMapper getDefaultMapper() {
    return DEFAULT_MAPPER;
  }

  public static ObjectMapper getMapper() {
    return MAPPER;
  }

  public static <M, D extends JsonDeserializer<M>> void registerDeserializer(Class<M> clazz, D deserializer) {
    SimpleModule module = new SimpleModule();
    module.addDeserializer(clazz, deserializer);
    MAPPER.registerModule(module);
  }

  /**
   * Map (deserialize) JSON String to java type instance.
   *
   * @param content JSON content
   * @param valueType Resulting type.
   * @param <T> Type
   * @return instance of type.
   */
  public static <T> T readValue(String content, Class<T> valueType) {
    try {
      return getMapper().readValue(content, valueType);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Map (serialize) java type instance to JSON String.
   *
   * @param o java class value
   * @return serialized JSON String
   */
  public static <T> String valueAsString(T o) {
    try {
      return getMapper().writeValueAsString(o);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static ObjectMapper createDefaultMapper() {
    var module = new SimpleModule();
    module.addSerializer(Date.class, new DateSerializer(Date.class));
    module.addDeserializer(Date.class, new DateDeserializer(Date.class));
    var mapper = new ObjectMapper();
    mapper.registerModule(module);
    return mapper;
  }

}
