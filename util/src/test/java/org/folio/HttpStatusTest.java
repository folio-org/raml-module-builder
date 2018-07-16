package org.folio;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class HttpStatusTest {
  @ParameterizedTest
  @CsvSource({
    "200, HTTP_OK",
    "201, HTTP_CREATED",
    "202, HTTP_ACCEPTED",
    "501, HTTP_NOT_IMPLEMENTED",
  })
  void existingStatus(int code, String name) {
    HttpStatus status = HttpStatus.get(code);
    assertThat(status.name(), is(name));
    assertThat(status.toInt(), is(code));
  }

  @Test
  void getUndefined() {
    assertThrows(IllegalArgumentException.class, () -> HttpStatus.get(299));
  }

  @Test
  void codeIsUnique() {
    for (HttpStatus status : HttpStatus.values()) {
      HttpStatus statusByCode = HttpStatus.get(status.toInt());
      assertThat(status.name(), status, is(statusByCode));
    }
  }
}
