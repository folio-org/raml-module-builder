package org.folio.rest.tools.plugins;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.folio.rest.tools.plugins.CustomTypeAnnotator.getCustomFields;

import org.junit.jupiter.api.Test;

class CustomTypeAnnotatorTest {

  @Test
  void setCustomFieldsNullString() {
    CustomTypeAnnotator.setCustomFields((String)null);
    assertThat(getCustomFields()[0], containsString("\"fieldname\":\"readonly\""));
  }

  @Test
  void setCustomFieldsString() {
    CustomTypeAnnotator.setCustomFields("\"fieldname\":\"foo\";\"fieldname\":\"bar\"");
    assertThat(getCustomFields()[0], containsString("\"fieldname\":\"foo\""));
    assertThat(getCustomFields()[1], containsString("\"fieldname\":\"bar\""));
  }

  @Test
  void setCustomFieldsEmptyArray() {
    String[] array = {};
    CustomTypeAnnotator.setCustomFields(array);
    assertThat(getCustomFields()[0], containsString("\"fieldname\":\"readonly\""));
  }

  @Test
  void setCustomFieldsEmptyStringArray() {
    String[] array = {""};
    CustomTypeAnnotator.setCustomFields(array);
    assertThat(getCustomFields()[0], containsString("\"fieldname\":\"readonly\""));
  }

  @Test
  void setCustomFieldsArray1() {
    String[] array = {"\"fieldname\":\"one\""};
    CustomTypeAnnotator.setCustomFields(array);
    assertThat(getCustomFields()[0], containsString("\"fieldname\":\"one\""));
  }

  @Test
  void setCustomFieldsArray2() {
    String[] array = {"\"fieldname\":\"foo\"", "\"fieldname\":\"bar\""};
    CustomTypeAnnotator.setCustomFields(array);
    assertThat(getCustomFields()[0], containsString("\"fieldname\":\"foo\""));
    assertThat(getCustomFields()[1], containsString("\"fieldname\":\"bar\""));
  }


}
