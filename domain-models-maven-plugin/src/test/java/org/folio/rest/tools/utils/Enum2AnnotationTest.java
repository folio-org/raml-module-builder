package org.folio.rest.tools.utils;

import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class Enum2AnnotationTest implements WithAssertions {

  @ParameterizedTest
  @CsvSource({
    "PATTERN,      javax.validation.constraints.Pattern",
    "pattern,      javax.validation.constraints.Pattern",
    "MIN,          javax.validation.constraints.Min",
    "MAX,          javax.validation.constraints.Max",
    "REQUIRED,     javax.validation.constraints.NotNull",
    "DEFAULTVALUE, javax.ws.rs.DefaultValue",
    "SIZE,         javax.validation.constraints.Size",
    "MINNI, ",
  })
  void getAnnotation(String anno, String clazz) {
    assertThat(Enum2Annotation.getAnnotation(anno)).isEqualTo(clazz);
  }

  @ParameterizedTest
  @CsvSource({
    "GET,    true",
    "get,    true",
    "POST,   true",
    "PUT,    true",
    "DELETE, true",
    "GETTY,  false",
    "getty,  false",
    ",       false",
  })
  void isVerbEnum(String verb, boolean expected) {
    assertThat(Enum2Annotation.isVerbEnum(verb)).isEqualTo(expected);
  }

}
