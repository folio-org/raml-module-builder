
package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.folio.rest.testing.UtilityClassTester;
import org.junit.Test;

import com.github.jasync.sql.db.exceptions.DatabaseException;
import com.github.jasync.sql.db.postgresql.exceptions.GenericDatabaseException;
import com.github.jasync.sql.db.postgresql.messages.backend.ErrorMessage;

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
    assertThat(PgExceptionUtil.badRequestMessage(new DatabaseException("")), is(nullValue()));
  }

  @Test
  public void noField() {
    @SuppressWarnings("unchecked")
    ErrorMessage m = new ErrorMessage(Collections.EMPTY_MAP);
    assertThat(PgExceptionUtil.badRequestMessage(new GenericDatabaseException(m)), is(nullValue()));
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
    assertThat(PgExceptionUtil.isForeignKeyViolation(genericDatabaseException('C', "23503")), is(true));
    assertThat(PgExceptionUtil.isForeignKeyViolation(genericDatabaseException('C', "22P02")), is(false));
    assertThat(PgExceptionUtil.isForeignKeyViolation(genericDatabaseException()), is(false));
    assertThat(PgExceptionUtil.isForeignKeyViolation(null), is(false));
  }

  @Test
  public void isUniqueViolation() {
    assertThat(PgExceptionUtil.isUniqueViolation(genericDatabaseException('C', "23505")), is(true));
    assertThat(PgExceptionUtil.isUniqueViolation(genericDatabaseException('C', "23503")), is(false));
  }

  @Test
  public void isInvalidTextRepresentationn() {
    assertThat(PgExceptionUtil.isInvalidTextRepresentation(genericDatabaseException('C', "22P02")), is(true));
    assertThat(PgExceptionUtil.isInvalidTextRepresentation(genericDatabaseException('C', "23503")), is(false));
  }

  /**
   * @return GenericDatabaseException with ErrorMessage with the key value pairs listed in arguments, for example
   *   {@code genericDatabaseException('C', sqlstate, 'D', detail, 'M', message)}
   */
  static GenericDatabaseException genericDatabaseException(Object ... arguments) {
    Map<Character, String> map = new HashMap<>();
    for (int i=0; i<arguments.length; i+=2) {
      map.put((Character) arguments[i], (String) arguments[i+1]);
    }
    return new GenericDatabaseException(new ErrorMessage(map));
  }

  private void assertString(String sqlstate, String detail, String message, String expected) {
    String actual = PgExceptionUtil.badRequestMessage(genericDatabaseException('C', sqlstate, 'D', detail, 'M', message));
    assertThat(actual, is(expected));
  }

}
