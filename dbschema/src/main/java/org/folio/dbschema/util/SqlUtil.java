package org.folio.dbschema.util;

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

  public static final class Cql2PgUtil {
    private Cql2PgUtil() {
      throw new UnsupportedOperationException("Cannot instantiate utility class.");
    }

    /**
     * Convert cqlName to an SQL JSON expression with the given table.
     *
     * <p>Examples:
     *
     * <p>cqlNameAsSqlJson("tab.jsonb", "a", result) = "tab.jsonb->'a'"
     *
     * <p>cqlNameAsSqlJson("tab.jsonb", "a.b.c", result) = "tab.jsonb->'a'->'b'->'c'"
     *
     * @param field the PostgreSQL table field
     * @param cqlName the CQL name to convert
     * @return the SQL JSON expression
     */
    public static String cqlNameAsSqlJson(CharSequence field, CharSequence cqlName) {
      StringBuilder result = new StringBuilder();
      appendCqlNameAsSqlJson(field, cqlName, 0, cqlName.length(), result);
      return result.toString();
    }

    /**
     * Convert cqlName to an SQL JSON expression with the given table and append it to result.
     *
     * <p>Examples:
     *
     * <p>appendCqlNameAsSqlJson("tab.jsonb", "a", result) appends "tab.jsonb->'a'"
     *
     * <p>appendCqlNameAsSqlJson("tab.jsonb", "a.b.c", result) appends "tab.jsonb->'a'->'b'->'c'"
     *
     * @param field the PostgreSQL table field
     * @param cqlName the CQL name to convert
     * @param result where to append
     */
    public static void appendCqlNameAsSqlJson(CharSequence field, CharSequence cqlName, StringBuilder result) {
      appendCqlNameAsSqlJson(field, cqlName, 0, cqlName.length(), result);
    }

    /**
     * Convert cqlName to an SQL JSON expression with the given table and append it to result.
     *
     * <p>cqlName = cqlTerm.subSequence(start, end)
     *
     * <p>The same as appendCqlNameAsSqlJson(tableLoc, cqlTerm.subSequence(start, end), result), but more efficient.
     *
     * <p>Examples:
     *
     * <p>appendCqlNameAsSqlJson("tab.jsonb", "a=x AND b=y", 8, 9, result) appends "tab.jsonb->'b'"
     *
     * <p>appendCqlNameAsSqlJson("tab.jsonb", "a=x AND b.c.d=y", 8, 13, result) appends "tab.jsonb->'b'->'c'->'d'"
     *
     * @param field the PostgreSQL table field
     * @param cqlTerm the CQL term with the cqlName
     * @param start start position of the cqlName within cqlTerm
     * @param end end position of the cqlName within cqlTerm
     * @param result where to append
     */
    public static void appendCqlNameAsSqlJson(
        CharSequence field, CharSequence cqlTerm, int start, int end, StringBuilder result) {
      // This implementation directly copies the characters without creating any temporary
      // String by avoiding String creating methods like String.split or CharSequence.subsequence.
      // This saves heap space and is much faster.
      result.append(field).append("->'");
      for (int pos = start; pos < end; pos++) {
        char c = cqlTerm.charAt(pos);
        switch (c) {
        case '.':
          result.append("'->'");
          break;
        case '\'':
          result.append("''");
          break;
        default:
          result.append(c);
        }
      }
      result.append("'");
    }

    /**
     * Convert cqlName to an SQL expression that extracts a text value from a JSON of the given table.
     *
     * <p>Examples:
     *
     * <p>appendCqlNameAsSqlText("tab.jsonb", "b", result) = "tab.jsonb->>'b'"
     *
     * <p>appendCqlNameAsSqlText("tab.jsonb", "b.c.d", result) = "tab.jsonb->'b'->'c'->>'d'"
     *
     * @param field the PostgreSQL table field
     * @param cqlName the CQL name of the text field
     * @return the SQL expression
     */
    public static String cqlNameAsSqlText(CharSequence field, CharSequence cqlName) {
      StringBuilder result = new StringBuilder();
      appendCqlNameAsSqlText(field, cqlName, 0, cqlName.length(), result);
      return result.toString();
    }

    /**
     * Convert cqlName to an SQL expression that extracts a text value from a JSON of the given table and append it to result.
     *
     * <p>Examples:
     *
     * <p>appendCqlNameAsSqlText("tab.jsonb", "b", result) appends "tab.jsonb->>'b'"
     *
     * <p>appendCqlNameAsSqlText("tab.jsonb", "b.c.d", result) appends "tab.jsonb->'b'->'c'->>'d'"
     *
     * @param field the PostgreSQL table field
     * @param cqlName the CQL name of the text field
     * @param result where to append
     */
    public static void appendCqlNameAsSqlText(CharSequence field, CharSequence cqlName, StringBuilder result) {
      appendCqlNameAsSqlText(field, cqlName, 0, cqlName.length(), result);
    }

    /**
     * Convert cqlName to an SQL expression that extracts a text value from a JSON of the given table and append it to result.
     *
     * <p>cqlName = cqlTerm.subSequence(start, end)
     *
     * <p>This method has the same result as appendCqlNameAsSqlText(tableLoc, cqlTerm.subSequence(start, end), result)
     * but is more efficient.
     *
     * <p>Examples:
     *
     * <p>appendCqlNameAsSqlText("tab.jsonb", "a=x AND b=y", 8, 9, result) appends "tab.jsonb->>'b'"
     *
     * <p>appendCqlNameAsSqlText("tab.jsonb", "a=x AND b.c.d=y", 8, 13, result) appends "tab.jsonb->'b'->'c'->>'d'"
     *
     * @param field the PostgreSQL table field
     * @param cqlTerm the CQL term with the cqlName
     * @param start start position of the cqlName within cqlTerm
     * @param end end position of the cqlName within cqlTerm
     * @param result where to append
     */
    public static void appendCqlNameAsSqlText(
        CharSequence field, CharSequence cqlTerm, int start, int end, StringBuilder result) {
      // This implementation directly copies the characters without creating any temporary
      // String by avoiding String creating methods like String.split or CharSequence.subsequence.
      // This saves heap space and is much faster.
      result.append(field);
      int lastDot = -1;
      for (int pos = end - 1; pos >= start; pos--) {
        if (cqlTerm.charAt(pos) == '.') {
          lastDot = pos;
          break;
        }
      }
      if (lastDot == -1) {
        result.append("->>'");
      } else {
        result.append("->'");
      }
      for (int pos = start; pos < end; pos++) {
        char c = cqlTerm.charAt(pos);
        switch (c) {
        case '.':
          if (pos < lastDot) {
            result.append("'->'");
          } else {
            result.append("'->>'");
          }
          break;
        case '\'':
          result.append("''");
          break;
        default:
          result.append(c);
        }
      }
      result.append("'");
    }

    /**
     * Duplicate any single quote within s and wrap the result into single quotes.
     * This creates an SQL string constant in single quotes.
     *
     * <p>Examples:
     *
     * <p><code>quoted("") = "''"</code>
     *
     * <p><code>quoted("a") = "'a'"</code>
     *
     * <p><code>quoted("It's cool", result) = "'It''s cool'"</code>
     *
     * @param s the source text, must not be null
     * @return the quoted string
     */
    public static String quoted(CharSequence s) {
      StringBuilder result = new StringBuilder();
      appendQuoted(s, 0, s.length(), result);
      return result.toString();
    }

    /**
     * Duplicate any single quote within s, wrap the result into single quotes, and append it to s.
     * This creates an SQL string constant in single quotes.
     *
     * <p>Examples:
     *
     * <p><code>appendQuoted("", result)</code> appends <code>''</code>
     *
     * <p><code>appendQuoted("a", result)</code> appends <code>'a'</code>
     *
     * <p><code>appendQuoted("It's cool", result)</code> appends <code>'It''s cool'</code>
     *
     * @param s the source text, must not be null
     * @param result where to append
     */
    public static void appendQuoted(CharSequence s, StringBuilder result) {
      appendQuoted(s, 0, s.length(), result);
    }

    /**
     * Take the substring, duplicate any single quote within it, wrap the result it into single quotes,
     * and append it to s. Substring is s.subsquence(start, end). This creates an SQL string constant in single quotes.
     *
     * <p>This is the same as <code>appendQuoted(s.subsequence(start, end), result)</code> but more efficient.
     *
     * <p>Examples:
     *
     * <p><code>appendQuoted(null, 5, 5, result)</code> appends <code>''</code>
     *
     * <p><code>appendQuoted("a b c", 2, 3, result)</code> appends <code>'b'</code>
     *
     * <p><code>appendQuoted("4 Rock'n'roll!", 2, 13, result)</code> appends <code>'Rock''n''roll'</code>
     *
     * @param s the source text to take the subsequence from, must not be null if start &lt; end
     * @param start the start of the substring within s
     * @param end the end of the substring within s
     * @param result where to append
     */
    public static void appendQuoted(CharSequence s, int start, int end, StringBuilder result) {
      // This implementation directly copies the characters without creating any temporary
      // String by avoiding String creating methods like String.split or CharSequence.subsequence.
      // This saves heap space and is much faster.
      result.append('\'');
      for (int i = start; i < end; i++) {
        final char c = s.charAt(i);
        if (c == '\'') {
          result.append("''");
        } else {
          result.append(c);
        }
      }
      result.append('\'');
    }

    public static String wrapInLowerUnaccent(String term, boolean lower, boolean unaccent) {
      if (lower) {
        if (unaccent) {
          return "lower(f_unaccent(" + term + "))";
        } else {
          return "lower(" + term + ")";
        }
      } else {
        if (unaccent) {
          return "f_unaccent(" + term + ")";
        } else {
          return term;
        }
      }
    }
  }
}
