
package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.vertx.pgclient.PgException;
import org.folio.rest.testing.UtilityClassTester;
import org.junit.Test;

public class PgExceptionUtilTest {
  @Test
  public void nullThrowable() {
    assertThat(PgExceptionUtil.badRequestMessage(null), is(nullValue()));
  }

  @Test
  public void throwable() {
    assertThat(PgExceptionUtil.badRequestMessage(new Throwable()), is(nullValue()));
  }

  @Test
  public void databaseException() {
    assertThat(PgExceptionUtil.badRequestMessage(new PgException("", null, "", "")), is(nullValue()));
  }

  @Test
  public void noField() {
    assertThat(PgExceptionUtil.badRequestMessage(new PgException("", null, "", "")), is(nullValue()));
  }

  @Test
  public void foreignKeyViolation() {
    assertString("23503", "some detail", "the message", "the message: some detail");
  }

  @Test
  public void invalidTextRepresentation() {
    assertString("22P02", "detail", "message", "message");
  }

  @Test
  public void otherError() {
    assertString("XX000", "internal error", "crash", null);
  }

  @Test
  public void utilityClass() {
    UtilityClassTester.assertUtilityClass(PgExceptionUtil.class);
  }

  @Test
  public void isForeignKeyViolation() {
    assertThat(PgExceptionUtil.isForeignKeyViolation(new PgException("", null, "23503", "")), is(true));
    assertThat(PgExceptionUtil.isForeignKeyViolation(new PgException("", null, "22P02", "")), is(false));
    assertThat(PgExceptionUtil.isForeignKeyViolation(new PgException("", null, "", "")), is(false));
    assertThat(PgExceptionUtil.isForeignKeyViolation(null), is(false));
  }

  @Test
  public void isUniqueViolation() {
    assertThat(PgExceptionUtil.isUniqueViolation(new PgException("", null, "23505", "")), is(true));
    assertThat(PgExceptionUtil.isUniqueViolation(new PgException("", null, "23503", "")), is(false));
  }

  @Test
  public void isInvalidTextRepresentationn() {
    assertThat(PgExceptionUtil.isInvalidTextRepresentation(new PgException("", null, "22P02", "")), is(true));
    assertThat(PgExceptionUtil.isInvalidTextRepresentation(new PgException("", null, "23503", "")), is(false));
  }

  private void assertString(String sqlstate, String detail, String message, String expected) {
    String actual = PgExceptionUtil.badRequestMessage(new PgException(message, null, sqlstate, detail));
    assertThat(actual, is(expected));
  }

}
