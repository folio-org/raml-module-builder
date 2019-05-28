package org.folio.rest.persist;

import java.util.Map;

import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException;
import com.github.mauricio.async.db.postgresql.messages.backend.ErrorMessage;

import scala.collection.JavaConverters;

public final class PgExceptionUtil {
  // https://www.postgresql.org/docs/current/static/errcodes-appendix.html
  private static final String FOREIGN_KEY_VIOLATION = "23503";
  private static final String UNIQUE_VIOLATION = "23505";
  private static final String INVALID_TEXT_REPRESENTATION = "22P02";

  private PgExceptionUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Return the value for key in the
   * {@link com.github.mauricio.async.db.postgresql.messages.backend.ErrorMessage ErrorMessage} map of the
   * {@link com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException GenericDatabaseException}.
   * @param throwable a Throwable or null
   * @param key the {@link com.github.mauricio.async.db.postgresql.messages.backend.ErrorMessage ErrorMessage} key
   * @return the value if throwable is a
   *   {@link com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException GenericDatabaseException}
   *   and the key exists, null otherwise.
   */
  public static String get(Throwable throwable, Character key) {
    Map<Object,String> fields = getBadRequestFields(throwable);
    if (fields == null) {
      return null;
    }
    return fields.get(key);
  }

  /**
   * Check for foreign key violation.
   * @param throwable any Throwable or null
   * @return true if throwable is a
   *   {@link com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException GenericDatabaseException}
   *   containing an
   *   {@link com.github.mauricio.async.db.postgresql.messages.backend.ErrorMessage ErrorMessage}
   *   that reports a foreign key violation, false otherwise.
   */
  public static boolean isForeignKeyViolation(Throwable throwable) {
    return FOREIGN_KEY_VIOLATION.equals(get(throwable, 'C'));
  }

  /**
   * Check for unique violation.
   * @param throwable any Throwable or null
   * @return true if throwable is a
   *   {@link com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException GenericDatabaseException}
   *   containing an
   *   {@link com.github.mauricio.async.db.postgresql.messages.backend.ErrorMessage ErrorMessage}
   *   that reports a unique violation, false otherwise.
   */
  public static boolean isUniqueViolation(Throwable throwable) {
    return UNIQUE_VIOLATION.equals(get(throwable, 'C'));
  }

  /**
   * Check for invalid text representation.
   * @param throwable any Throwable or null
   * @return true if throwable is a
   *   {@link com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException GenericDatabaseException}
   *   containing an
   *   {@link com.github.mauricio.async.db.postgresql.messages.backend.ErrorMessage ErrorMessage}
   *   that reports an invalid text representation, false otherwise.
   */
  public static boolean isInvalidTextRepresentation(Throwable throwable) {
    return INVALID_TEXT_REPRESENTATION.equals(get(throwable, 'C'));
  }

  /**
   * If this throwable is an Exception thrown because of some PostgreSQL data
   * restriction (foreign key violation, invalid uuid, duplicate key) then
   * return some detail text of that Exception, otherwise return null.
   *
   * @param throwable - where to read the text from
   * @return detail text of the violation, or null if some other Exception
   */
  public static String badRequestMessage(Throwable throwable) {
    if (!(throwable instanceof GenericDatabaseException)) {
      return null;
    }

    ErrorMessage errorMessage = ((GenericDatabaseException) throwable).errorMessage();
    Map<Object,String> fields = JavaConverters.mapAsJavaMapConverter(errorMessage.fields()).asJava();
    String sqlstate = fields.get('C');
    if (sqlstate == null) {
      return null;
    }

    String detail = fields.getOrDefault('D', "");
    String message = fields.getOrDefault('M', "");
    switch (sqlstate) {
    case FOREIGN_KEY_VIOLATION:
      // insert or update on table "item" violates foreign key constraint "item_permanentloantypeid_fkey":
      // Key (permanentloantypeid)=(5573df18-043f-4228-b108-483fd3a0cb57) is not present in table "loan_type".
    case UNIQUE_VIOLATION:
      // duplicate key value violates unique constraint "loan_type_unique_idx":
      // Key ((jsonb ->> 'name'::text))=(Can circulate) already exists.
      return message + ": " + detail;
    case INVALID_TEXT_REPRESENTATION:  // invalid input syntax for uuid: "1234"
      return message;
    default:
      return null;
    }
  }

  public static Map<Object,String> getBadRequestFields(Throwable throwable) {
    if (!(throwable instanceof GenericDatabaseException)) {
      return null;
    }

    ErrorMessage errorMessage = ((GenericDatabaseException) throwable).errorMessage();
    return JavaConverters.mapAsJavaMapConverter(errorMessage.fields()).asJava();
  }
}
