
package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import io.vertx.pgclient.PgException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.folio.okapi.testing.UtilityClassTester;
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

  @Test
  public void testBadUUID() {
    try {
      UUID.fromString("bad-uid");
    } catch (Exception ex) {
      String actual = PgExceptionUtil.badRequestMessage(ex);
      assertThat(actual, is("Invalid UUID string: bad-uid"));
    }
  }

  @Test
  public void getMessageOther() {
    assertThat(PgExceptionUtil.getMessage(new RuntimeException("foo")), is("foo"));
  }

  @Test
  public void getMessagePgException() {
    PgException e = new PgException("my message", "my severity", "00742", "error sits in front of the screen");
    assertThat(PgExceptionUtil.getMessage(e),
        is("ErrorMessage(fields=[(Severity, my severity), (SQLSTATE, 00742), " +
            "(Message, my message), (Detail, error sits in front of the screen)])"));
  }

  @Test
  public void testCreatePgExceptionFromMap() {
    Map<Character, String> fields1 = new HashMap<>();
    fields1.put('M', "valueM");
    fields1.put('S', "valueS");
    fields1.put('D', "ValueD");
    fields1.put('C', "ValueC");

    Exception e = PgExceptionUtil.createPgExceptionFromMap(fields1);
    Map<Character, String> fields2 = PgExceptionUtil.getBadRequestFields(e);

    for (Character k : fields1.keySet()) {
      assertThat(fields1.get(k), is(fields2.get(k)));
    }
  }

}
