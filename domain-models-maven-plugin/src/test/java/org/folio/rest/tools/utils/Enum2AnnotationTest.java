package org.folio.rest.tools.utils;

import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class Enum2AnnotationTest implements WithAssertions {

  @ParameterizedTest
  @CsvSource({
    "PATTERN,      jakarta.validation.constraints.Pattern",
    "pattern,      jakarta.validation.constraints.Pattern",
    "MIN,          jakarta.validation.constraints.Min",
    "MAX,          jakarta.validation.constraints.Max",
    "REQUIRED,     jakarta.validation.constraints.NotNull",
    "DEFAULTVALUE, javax.ws.rs.DefaultValue",
    "SIZE,         jakarta.validation.constraints.Size",
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
