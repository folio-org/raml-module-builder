package org.folio.dbschema.util;

import java.io.IOException;
import java.util.Date;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class DateDeserializer extends StdDeserializer<Date> {

  public DateDeserializer(Class<?> c) {
    super(c);
  }

  @SuppressWarnings({
    "java:S1151", // "switch case" clauses should not have too many lines of code -
    // false positive because the total size of the switch statement is small
    "java:S2143", // "java.time" classes should be used for dates and times -
    // cannot change legacy code both in RAML tooling and in RMB
  })
  @Override
  public Date deserialize(JsonParser parser, DeserializationContext context)
      throws IOException {

    return switch (parser.currentToken()) {
      case VALUE_STRING -> {
        var v = parser.getValueAsString();
        if (v.isEmpty()) {
          yield null;
        }
        // remove preceding + that Jackson's default Date formatter have created
        // for year 0 dates like "+0000-01-01T00:00:00.000+00:00"
        if (v.startsWith("+")) {
          v = v.substring(1);
        }
        yield context.parseDate(v);
      }
      // non-RMB serializers use Date::getTime to serialize a Date
      case VALUE_NUMBER_INT -> new Date(parser.getValueAsLong());
      default -> throw context.wrongTokenException(parser, Date.class, JsonToken.VALUE_STRING,
          "expected string containing a date");
    };
  }
}

