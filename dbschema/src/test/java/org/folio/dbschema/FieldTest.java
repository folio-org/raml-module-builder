package org.folio.dbschema;

import static org.junit.jupiter.api.Assertions.*;

import org.folio.dbschema.Field;
import org.junit.jupiter.api.Test;

class FieldTest {
  @Test
  @SuppressWarnings("squid:S2699")  // suppress "add at least on assertion to this test case"
  void fieldName49CharsLong() {
    new Field().setFieldName("a234567890123456789012345678901234567890123456789");
    // assert that no exception has been thrown
  }

  @Test
  void fieldNameTooLong() {
    assertThrows(IllegalArgumentException.class, () ->
      new Field().setFieldName("a2345678901234567890123456789012345678901234567890"));
  }
}
