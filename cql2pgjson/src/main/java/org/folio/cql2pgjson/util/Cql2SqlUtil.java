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

  public static String cql2like(String s) {
    return cql2like(s, false);
  }
  /**
   * Convert a CQL string to an SQL LIKE string.
   * CQL escapes * ? ^ \ and SQL LIKE escapes \ % _.
   *
   * @param s  CQL string without leading or trailing double quote
   * @return SQL LIKE string without leading or trailing single quote
   */
  @SuppressWarnings("squid:S3776")  // suppress "Cognitive Complexity of methods should not be too high"
  public static String cql2like(String s, boolean quoted) {
    StringBuilder like = new StringBuilder();
    /** true if the previous character is an escaping backslash */
    boolean backslash = false;
    for (char c : s.toCharArray()) {
      switch (c) {
      case '\\':
        backslash = escapeBackSlash(like, backslash);
        break;
      case '%':
      case '_':
        if(!quoted) {
          backslash = escapePercentAndUnderscore(like, c);
        }
        break;
      case '?':
        backslash = escapeQuestionMark(like, backslash);
        break;
      case '*':
        backslash = escapeStar(like, backslash);
        break;
      case '\'':   // a single quote '
        backslash = escapeSingleQuote(like);
        break;
      default:
        backslash = defaultAppend(like, c);
        break;
      }
    }

    if (backslash) {
      // a single backslash at the end is an error but we handle it gracefully matching one.
      like.append("\\\\");
    }

    return like.toString();
  }
  private static boolean defaultAppend(StringBuilder like, char c) {
    boolean backslash;
    like.append(c);
    backslash = false;
    return backslash;
  }
  private static boolean escapeSingleQuote(StringBuilder like) {
    boolean backslash;
    // postgres requires to double a ' inside a ' terminated string.
    like.append("''");
    backslash = false;
    return backslash;
  }
  private static boolean escapeStar(StringBuilder like, boolean backslash) {
    if (backslash) {
      like.append("\\*");
      backslash = false;
    } else {
      like.append('%');
    }
    return backslash;
  }
  private static boolean escapeQuestionMark(StringBuilder like, boolean backslash) {
    if (backslash) {
      like.append("\\?");
      backslash = false;
    } else {
      like.append('_');
    }
    return backslash;
  }
  private static boolean escapePercentAndUnderscore(StringBuilder like, char c) {
    boolean backslash;
    like.append('\\').append(c);  // mask LIKE character
    backslash = false;
    return backslash;
  }
  private static boolean escapeBackSlash(StringBuilder like, boolean backslash) {
    if (backslash) {
      like.append("\\\\");
      backslash = false;
    } else {
      backslash = true;
    }
    return backslash;
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
        backslash = escapeBackSlash(regexp, backslash);
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
        backslash = escapePercentAndUnderscore(regexp, c);
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
        backslash = escapeSingleQuote(regexp);
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
        backslash = defaultAppend(regexp, c);
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
