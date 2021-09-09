package org.folio.rest.persist;

import io.vertx.pgclient.PgException;

import java.util.HashMap;
import java.util.Map;

public final class PgExceptionUtil {
  // https://www.postgresql.org/docs/current/static/errcodes-appendix.html
  static final String FOREIGN_KEY_VIOLATION = "23503";
  static final String UNIQUE_VIOLATION = "23505";
  static final String INVALID_TEXT_REPRESENTATION = "22P02";
  static final String VERSION_CONFLICT = "23F09";

  private PgExceptionUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Return the value for key in the
   *   {@link io.vertx.pgclient.PgException PgException}
   * @param throwable a Throwable or null
   * @param key
   * @return the value if throwable is a
   *   {@link io.vertx.pgclient.PgException PgException}
   *   and the key exists, null otherwise.
   */
  public static String get(Throwable throwable, Character key) {
    Map<Character,String> fields = getBadRequestFields(throwable);
    if (fields == null) {
      return null;
    }
    return fields.get(key);
  }

  /**
   * Check for foreign key violation.
   * @param throwable any Throwable or null
   * @return true if throwable is a
   *   {@link io.vertx.pgclient.PgException PgException}
   *   that reports a foreign key violation, false otherwise.
   */
  public static boolean isForeignKeyViolation(Throwable throwable) {
    return FOREIGN_KEY_VIOLATION.equals(get(throwable, 'C'));
  }

  /**
   * Check for unique violation.
   * @param throwable any Throwable or null
   * @return true if throwable is a
   *   {@link io.vertx.pgclient.PgException PgException}
   *   that reports a unique violation, false otherwise.
   */
  public static boolean isUniqueViolation(Throwable throwable) {
    return UNIQUE_VIOLATION.equals(get(throwable, 'C'));
  }

  /**
   * Check for invalid text representation.
   * @param throwable any Throwable or null
   * @return true if throwable is a
   *   {@link io.vertx.pgclient.PgException PgException}
   *   that reports an invalid text representation, false otherwise.
   */
  public static boolean isInvalidTextRepresentation(Throwable throwable) {
    return INVALID_TEXT_REPRESENTATION.equals(get(throwable, 'C'));
  }

  /**
   * Check for optimistic locking version conflict.
   * @param throwable any Throwable or null
   * @return true if throwable is a
   *   {@link io.vertx.pgclient.PgException PgException}
   *   that reports a version conflict, false otherwise.
   */
  public static boolean isVersionConflict(Throwable throwable) {
    return VERSION_CONFLICT.equals(get(throwable, 'C'));
  }

  /**
   * If this throwable is an Exception thrown because of some PostgreSQL data
   * restriction (foreign key violation, invalid uuid, duplicate key) or a user error
   * Eg invalid UUID, then return some detail text of that Exception, otherwise return null.
   *
   * @param throwable - where to read the text from
   * @return detail text of the violation if user error, or null if some other Exception (server error)
   */
  public static String badRequestMessage(Throwable throwable) {
    if (throwable instanceof IllegalArgumentException) {
      return throwable.getMessage();
    }
    Map<Character,String> fields = getBadRequestFields(throwable);
    if (fields == null) {
      return null;
    }
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

  /**
   * Compose a message of all PgException fields.
   *
   * <p>Example output:
   *
   * <p>{@code ErrorMessage(fields=[(Severity, ERROR), (SQLSTATE, 23505),
   *     (Message, duplicate key value violates unique constraint "t_text_key"),
   *     (Detail, Key (text)=(a) already exists.)]}
   *
   * <p>This is similar to {@link com.github.jasync.sql.db.postgresql.exceptions.GenericDatabaseException#getMessage()
   * GenericDatabaseException#getMessage()} that returns
   *
   * <p>{@code ErrorMessage(fields=[(Severity, ERROR), (V, ERROR), (SQLSTATE, 23505),
   *     (Message, duplicate key value violates unique constraint "t_text_key"),
   *     (Detail, Key (text)=(a) already exists.), (s, public), (t, t), (n, t_text_key),
   *     (File, nbtinsert.c), (Line, 427), (Routine, _bt_check_unique)])}
   *
   * <p>Use this method where PgException.getMessage() returning the message field only is not sufficient.
   *
   * @return all PgException fields if throwable is a PgException, throwable.getMessage() otherwise
   */
  public static String getMessage(Throwable throwable) {
    if (!(throwable instanceof PgException)) {
      return throwable.getMessage();
    }
    PgException e = (PgException) throwable;
    return "ErrorMessage(fields=["
        + "(Severity, " + e.getSeverity() + "), "
        + "(SQLSTATE, " + e.getCode() + "), "
        + "(Message, " + e.getErrorMessage() + "), "
        + "(Detail, " + e.getDetail() + ")])";
  }

  public static Map<Character, String> getBadRequestFields(Throwable throwable) {
    if (!(throwable instanceof PgException)) {
      return null;
    }
    Map<Character, String> map = new HashMap<>();
    map.put('M', ((PgException) throwable).getErrorMessage());

    map.put('D', ((PgException) throwable).getDetail());

    map.put('S', ((PgException) throwable).getSeverity());
    map.put('C', ((PgException) throwable).getCode());

    return map;
  }

  /**
   * Constructor for PgException similar to the old postgres driver
   * @param map map of message, detail, code
   * @return
   */
  public static PgException createPgExceptionFromMap(Map<Character, String> map) {
    return new PgException(map.get('M'), map.get('S'), map.get('C'), map.get('D'));
  }
}
