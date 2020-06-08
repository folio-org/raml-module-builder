package org.folio.cql2pgjson.util;

import java.util.regex.Pattern;

public final class SqlUtil {
  private static final Pattern SQL_IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]{0,48}$");

  private SqlUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Validate as an SQL identifier. Additional restrictions: ASCII only (no accents, no umlauts),
   * no dollar sign $, the maximum length is 49 characters to be within PostgreSQL's limit of 63
   * when appending _idx_unique for index names or creating the audit trigger function names
   * audit_${table.tableName}_changes.
   * @param identifier
   * @throws IllegalArgumentException if identifier does not match regexp ^[a-zA-Z_][a-zA-Z0-9_]{0,48}$,
   *     see <a href="https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS">
   *     https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS</a>
   */
  public static void validateSqlIdentifier(String identifier) {
    if (! SQL_IDENTIFIER.matcher(identifier).matches()) {
      throw new IllegalArgumentException("SQL identifier must match regexp "
          + SQL_IDENTIFIER.pattern() + " but it is " + identifier);
    }
  }
}
