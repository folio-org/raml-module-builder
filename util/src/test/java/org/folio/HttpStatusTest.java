package org.folio;

import static org.junit.Assert.assertEquals;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class HttpStatusTest {
  @Test
  @Parameters({
    "200, HTTP_ACCEPTED",
    "201, HTTP_CREATED",
    "501, HTTP_NOT_IMPLEMENTED",
  })
  public void existingStatus(int code, String name) {
    HttpStatus status = HttpStatus.get(code);
    assertEquals(name, status.name());
    assertEquals(code, status.toInt());
  }

  @Test(expected = IllegalArgumentException.class)
  public void getUndefined() {
    HttpStatus.get(299);
  }

  @Test
  public void codeIsUnique() {
    for (HttpStatus status : HttpStatus.values()) {
      HttpStatus statusByCode = HttpStatus.get(status.toInt());
      assertEquals(status.name(), status, statusByCode);
    }
  }
}
