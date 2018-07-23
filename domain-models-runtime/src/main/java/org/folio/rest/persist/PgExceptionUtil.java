package org.folio.rest.persist;

import java.util.Map;

import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException;
import com.github.mauricio.async.db.postgresql.messages.backend.ErrorMessage;

import scala.collection.JavaConverters;

public final class PgExceptionUtil {
  private PgExceptionUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
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

    // https://www.postgresql.org/docs/current/static/errcodes-appendix.html
    final String foreignKeyViolation = "23503";
    final String uniqueViolation = "23505";
    final String invalidTextRepresentation = "22P02";
    String detail = fields.getOrDefault('D', "");
    String message = fields.getOrDefault('M', "");
    switch (sqlstate) {
    case foreignKeyViolation:
      // insert or update on table "item" violates foreign key constraint "item_permanentloantypeid_fkey":
      // Key (permanentloantypeid)=(5573df18-043f-4228-b108-483fd3a0cb57) is not present in table "loan_type".
    case uniqueViolation:
      // duplicate key value violates unique constraint "loan_type_unique_idx":
      // Key ((jsonb ->> 'name'::text))=(Can circulate) already exists.
      return message + ": " + detail;
    case invalidTextRepresentation:  // invalid input syntax for uuid: "1234"
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
    Map<Object,String> fields = JavaConverters.mapAsJavaMapConverter(errorMessage.fields()).asJava();

    return fields;
  }
}
