package org.folio.dbschema.util;

import java.io.IOException;
import java.util.Date;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class DateSerializer extends StdSerializer<Date> {

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
