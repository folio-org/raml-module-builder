package org.folio.cql2pgjson.util;

import java.util.regex.Pattern;

/**
 * Functions that convert some CQL string the equivalent SQL string.
 */
public final class Cql2SqlUtil {
  /**
   * Postgres number, see spec at
   * <a href="https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS-NUMERIC">
   * https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS-NUMERIC</a>
   */
  private static final Pattern POSTGRES_NUMBER_REGEXP = Pattern.compile(
        "[+-]?"
      + "(?:"
      +   "\\d+"
      +   "|\\d+\\.\\d*"
      +   "|\\.\\d+"
      + ")"
      + "(?:[eE][+-]?\\d+)?"
      );

  private Cql2SqlUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }
  /**
   * Convert a CQL string to an SQL LIKE string.
   * CQL escapes * ? ^ \ and SQL LIKE escapes \ % _.
   *
   * @param s  CQL string without leading or trailing double quote
   * @return SQL LIKE string without leading or trailing single quote
   */
  @SuppressWarnings("squid:S3776")  // suppress "Cognitive Complexity of methods should not be too high"
  public static String quotedString(String s) {
    StringBuilder like = new StringBuilder();
    /** true if the previous character is an escaping backslash */
    boolean backslash = false;
    for (char c : s.toCharArray()) {
      switch (c) {
      case '\\':
        if (backslash) {
          like.append("\\\\");
          backslash = false;
        } else {
          backslash = true;
        }
        break;
      case '?':
        if (backslash) {
          like.append("\\?");
          backslash = false;
        } else {
          like.append('_');
        }
        break;
      case '*':
        if (backslash) {
          like.append("\\*");
          backslash = false;
        } else {
          like.append('%');
        }
        break;
      case '\'':   // a single quote '
        // postgres requires to double a ' inside a ' terminated string.
        like.append("''");
        backslash = false;
        break;
      default:
        like.append(c);
        backslash = false;
        break;
      }
    }

    if (backslash) {
      // a single backslash at the end is an error but we handle it gracefully matching one.
      like.append("\\\\");
    }

    return like.toString();
  }
  /**
   * Convert a CQL string to an SQL LIKE string.
   * CQL escapes * ? ^ \ and SQL LIKE escapes \ % _.
   *
   * @param s  CQL string without leading or trailing double quote
   * @return SQL LIKE string without leading or trailing single quote
   */
  @SuppressWarnings("squid:S3776")  // suppress "Cognitive Complexity of methods should not be too high"
  public static String cql2like(String s) {
    StringBuilder like = new StringBuilder();
    /** true if the previous character is an escaping backslash */
    boolean backslash = false;
    for (char c : s.toCharArray()) {
      switch (c) {
      case '\\':
        if (backslash) {
          like.append("\\\\");
          backslash = false;
        } else {
          backslash = true;
        }
        break;
      case '%':
      case '_':
        like.append('\\').append(c);  // mask LIKE character
        backslash = false;
        break;
      case '?':
        if (backslash) {
          like.append("\\?");
          backslash = false;
        } else {
          like.append('_');
        }
        break;
      case '*':
        if (backslash) {
          like.append("\\*");
          backslash = false;
        } else {
          like.append('%');
        }
        break;
      case '\'':   // a single quote '
        // postgres requires to double a ' inside a ' terminated string.
        like.append("''");
        backslash = false;
        break;
      default:
        like.append(c);
        backslash = false;
        break;
      }
    }

    if (backslash) {
      // a single backslash at the end is an error but we handle it gracefully matching one.
      like.append("\\\\");
    }

    return like.toString();
  }

  /**
   * Convert a CQL string to an SQL regexp string for the ~ operator.
   *
   * @param s  CQL string without leading or trailing double quote
   * @return SQL regexp string without leading and trailing single quote
   */
  @SuppressWarnings("squid:S3776")  // suppress "Cognitive Complexity of methods should not be too high"
  public static String cql2regexp(String s) {
    StringBuilder regexp = new StringBuilder();
    /** true if the previous character is an escaping backslash */
    boolean backslash = false;
    for (char c : s.toCharArray()) {
      switch (c) {
      case '\\':
        if (backslash) {
          regexp.append("\\\\");
          backslash = false;
        } else {
          backslash = true;
        }
        break;
      case '.':
      case '+':
      case '(':
      case ')':
      case '{':
      case '}':
      case '[':
      case ']':
      case '$':
        // Mask any character that is special in regexp. See list at
        // https://www.postgresql.org/docs/current/static/functions-matching.html#POSIX-SYNTAX-DETAILS
        regexp.append('\\').append(c);
        backslash = false;
        break;
      case '?':
        if (backslash) {
          regexp.append("\\?");
          backslash = false;
        } else {
          regexp.append('.');
        }
        break;
      case '*':
        if (backslash) {
          regexp.append("\\*");
          backslash = false;
        } else {
          regexp.append(".*");
        }
        break;
      case '\'':   // a single quote '
        // postgres requires to double a ' inside a ' terminated string.
        regexp.append("''");
        backslash = false;
        break;
      case '^':    // start of string or end of string
        if (backslash) {
          regexp.append("\\^");
          backslash = false;
        } else {
          regexp.append("(^|$)");
        }
        break;
      default:
        regexp.append(c);
        backslash = false;
        break;
      }
    }

    if (backslash) {
      // a single backslash at the end is an error but we handle it gracefully matching one.
      regexp.append("\\\\");
    }

    return regexp.toString();
  }

  /**
   * Test if s for sure is a syntactically correct SQL number.
   * <p>
   * Postgres also parses 1e but that may change in future.
   * @param s  String to test
   * @return true  if s is a Postgres number, false if not or unknown
   */
  public static boolean isPostgresNumber(String s) {
    return POSTGRES_NUMBER_REGEXP.matcher(s).matches();
  }
}
