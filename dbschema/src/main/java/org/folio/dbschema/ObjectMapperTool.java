package org.folio.dbschema;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Date;

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

  private static ObjectMapper createDefaultMapper() {
    var module = new SimpleModule();
    module.addSerializer(Date.class, new DateSerializer(Date.class));
    module.addDeserializer(Date.class, new DateDeserializer(Date.class));
    var mapper = new ObjectMapper();
    mapper.registerModule(module);
    return mapper;
  }

  public static class DateSerializer extends StdSerializer<Date> {

    public DateSerializer(Class<Date> type) {
      super(type);
    }

    @Override
    public void serialize(Date value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
      var s = provider.getConfig().getDateFormat().format(value);
      // remove preceding + that Jackson's default Date formatter creates
      // for year 0 dates like "+0000-01-01T00:00:00.000+00:00"
      if (s.startsWith("+")) {
        s = s.substring(1);
      }
      jgen.writeString(s);
    }
  }

  public static class DateDeserializer extends StdDeserializer<Date> {

    public DateDeserializer(Class<?> c) {
      super(c);
    }

    @Override
    public Date deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {

      var token = parser.currentToken();
      if (JsonToken.VALUE_STRING != token) {
        throw context.wrongTokenException(parser, Date.class, JsonToken.VALUE_STRING,
            "expected string containing a date");
      }
      var v = parser.getValueAsString();
      // remove preceding + that Jackson's default Date formatter have created
      // for year 0 dates like "+0000-01-01T00:00:00.000+00:00"
      if (v.startsWith("+")) {
        v = v.substring(1);
      }
      return context.parseDate(v);
    }
  }

}
