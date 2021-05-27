package org.folio.dbschema;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * @author shale
 *
 */
public final class ObjectMapperTool {
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    DEFAULT_MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

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
   * Map JSON to type.
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
}
