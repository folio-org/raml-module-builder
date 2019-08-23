package org.folio.cql2pgjson.util;

import java.util.regex.Pattern;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.rest.persist.ddlgen.Index;

/**
 * Functions that convert some CQL string the equivalent SQL string.
 */
public final class Cql2SqlUtil {

  private static final String ESC_SLASH = Pattern.quote("\\\\");
  private static final String ESC_STAR = Pattern.quote("\\*");
  private static final String ESC_QUEST = Pattern.quote("\\?");

  /**
   * Postgres number, see spec at
   * <a href="https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS-NUMERIC">
   * https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS-NUMERIC</a>
   */
  private static final Pattern POSTGRES_NUMBER_REGEXP = Pattern.compile(
    "[+-]?"
    + "(?:"
    + "\\d+"
    + "|\\d+\\.\\d*"
    + "|\\.\\d+"
    + ")"
    + "(?:[eE][+-]?\\d+)?"
  );

  private Cql2SqlUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Convert a CQL string to an SQL LIKE string. Signal the underlying function
   * that this is not a quoted String case.
   *
   * @param s CQL string without leading or trailing double quote
   * @return SQL LIKE string without leading or trailing single quote
   */
  @SuppressWarnings("squid:S3776")  // suppress "Cognitive Complexity of methods should not be too high"
  public static String cql2like(String s) {
    StringBuilder like = new StringBuilder();
    boolean backslash = false;  // previous character is backslash
    for (char c : s.toCharArray()) {
      switch (c) {
        case '\\':
          if (backslash) {
            like.append("\\\\");
            backslash = false;
            continue;
          }
          break;
        case '%':
        case '_':
          like.append('\\').append(c);  // mask LIKE character
          break;
        case '?':
          if (backslash) {
            like.append("\\?");
          } else {
            like.append('_');
          }
          break;
        case '*':
          if (backslash) {
            like.append("\\*");
          } else {
            like.append('%');
          }
          break;
        case '\'':   // a single quote '
          // postgres requires to double a ' inside a ' terminated string.
          like.append("''");
          break;
        default:
          like.append(c);
          break;
      }
      backslash = c == '\\';
    }

    if (backslash) {
      // a single backslash at the end is an error but we handle it gracefully matching one.
      like.append("\\\\");
    }

    return like.toString();
  }

  /**
   * Convert a CQL string to an SQL string. and SQL string constant escapes only
   * the single quote ' by doubling it.
   *
   * @param cql CQL string without leading or trailing double quote
   * @return SQL string without leading or trailing single quote
   */
  public static String cql2string(String cql) throws QueryValidationException {
    StringBuilder s = new StringBuilder();
    boolean backslash = false; // previous character is escaping backslash
    for (char c : cql.toCharArray()) {
      switch (c) {
        case '\\':
          if (backslash) {
            s.append("\\");
            backslash = false;
            continue;
          }
          break;
        case '^':
        case '?':
        case '*':
          if (backslash) {
            s.append(c);
          } else {
            throw new QueryValidationException("CQL operator " + c + " unsupported");
          }
          break;
        case '\'':   // a single quote '
          // postgres requires to double a ' inside a ' terminated string.
          s.append("''");
          break;
        default:
          s.append(c);
          break;
      }
      backslash = c == '\\';
    }
    if (backslash) {
      throw new QueryValidationException("Unterminated \\-character");
    }
    return s.toString();
  }

  /**
   * Convert a CQL string to an SQL regexp string for the ~ operator.
   *
   * @param s CQL string without leading or trailing double quote
   * @return SQL regexp string without leading and trailing single quote
   */
  @SuppressWarnings("squid:S3776")  // suppress "Cognitive Complexity of methods should not be too high"
  public static String cql2regexp(String s) {
    StringBuilder regexp = new StringBuilder();
    boolean backslash = false;  // previous character is backslash
    for (char c : s.toCharArray()) {
      switch (c) {
        case '\\':
          if (backslash) {
            regexp.append("\\\\");
            backslash = false;
            continue;
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
          break;
        case '?':
          if (backslash) {
            regexp.append("\\?");
          } else {
            regexp.append('.');
          }
          break;
        case '*':
          if (backslash) {
            regexp.append("\\*");
          } else {
            regexp.append(".*");
          }
          break;
        case '\'':   // a single quote '
          // postgres requires to double a ' inside a ' terminated string.
          regexp.append("''");
          break;
        case '^':    // start of string or end of string
          if (backslash) {
            regexp.append("\\^");
          } else {
            regexp.append("(^|$)");
          }
          break;
        default:
          regexp.append(c);
          break;
      }
      backslash = c == '\\';
    }

    if (backslash) {
      // a single backslash at the end is an error but we handle it gracefully matching one.
      regexp.append("\\\\");
    }

    return regexp.toString();
  }

  /**
   * Convert a CQL string to an SQL tsquery where each word matches in any order.
   *
   * @param s CQL string without leading or trailing double quote
   * @param removeAccents whether to wrap all words in f_unaccent().
   * @return SQL term
   * @throws QueryValidationException if s contains an unmasked wildcard question mark
   */
  @SuppressWarnings("squid:S3776")  // suppress "Cognitive Complexity of methods should not be too high"
  public static StringBuilder cql2tsqueryAnd(String s, boolean removeAccents) throws QueryValidationException {
    // We cannot use plainto_tsquery and phraseto_tsquery because they do not support right truncation.
    // to_tsquery supports right truncation but spaces must be replaced by some operator.

    StringBuilder t = new StringBuilder();
    t.append(removeAccents ? "(to_tsquery('simple', f_unaccent('''"
                           : "(to_tsquery('simple', ('''");
    /** whether the previous character is a masking backslash */
    boolean backslash = false;
    /** whether the previous character is a wildcard star for right truncation */
    boolean star = false;

    final int length = s.length();
    for (int i=0; i<length; i++) {
      char c = s.charAt(i);
      switch (c) {
      case '\\':
        if (backslash) {
          t.append("\\\\");
        }
        backslash = ! backslash;
        star = false;
        break;
      case '*':
        if (backslash) {
          t.append(c);
          backslash = false;
        } else {
          if (i+1 < length && s.charAt(i+1) != ' ') {
            throw new QueryValidationException(
                "* right truncation wildcard must be followed by space or end of string, but found " + c);
          }
          star = true;
        }
        break;
      case '?':
        if (! backslash) {
          throw new QueryValidationException("? wildcard not allowed in full text query string");
        }
        t.append(c);
        backslash = false;
        break;
      case ' ':
        if (i>0 && s.charAt(i-1) == ' ') {
          // skip space if previous character already was a space
          break;
        }
        if (star) {
          t.append("'':*')) ");
          star = false;
        } else {
          t.append("''')) ");
        }
        t.append(removeAccents ? "&& to_tsquery('simple', f_unaccent('''"
                               : "&& to_tsquery('simple', ('''");
        backslash = false;
        break;
      case '&':   // replace & so that we can replace all tsquery & by <-> for phrase search
      case '\'':  // replace single quote to avoid single quote masking
        t.append(',');
        backslash = false;
        break;
      default:
        t.append(c);
        backslash = false;
        break;
      }
    }
    if (star) {
      t.append("'':*')))");
    } else {
      t.append("''')))");
    }
    return t;
  }

  /**
   * Convert a CQL string to an SQL tsquery where at least one word matches.
   *
   * @param s CQL string without leading or trailing double quote
   * @param removeAccents whether to wrap all words in f_unaccent().
   * @return SQL term
   * @throws QueryValidationException if s contains an unmasked wildcard question mark
   */
  public static StringBuilder cql2tsqueryOr(String s, boolean removeAccents) throws QueryValidationException {
    // implementation idea: Replace the tsquery AND operator & by the tsquery OR operator |

    // to_tsquery('simple', '''abc,xyz''')
    // = 'abc' & 'xyz'

    // replace(to_tsquery('simple', '''abc,xyz''')::text, '&', '|')::tsquery
    // =  'abc' | 'xyz'

    StringBuilder t = cql2tsqueryAnd(s, removeAccents);
    t.insert(0, "replace(");
    t.append("::text, '&', '|')::tsquery");
    return t;
  }

  /**
   * Convert a CQL string to an SQL tsquery where the words matches consecutively and in that order (phrase search).
   *
   * @param s CQL string without leading or trailing double quote
   * @param removeAccents whether to wrap all words in f_unaccent().
   * @return SQL term
   * @throws QueryValidationException if s contains an unmasked wildcard question mark
   */
  public static StringBuilder cql2tsqueryPhrase(String s, boolean removeAccents) throws QueryValidationException {
    // implementation idea: Replace the tsquery AND operator & by the tsquery phrase operator <->

    // to_tsquery('simple', '''Vigneras, Louis-André''')
    // = 'vigneras' & 'louis-andré' & 'louis' & 'andré'

    // replace(to_tsquery('simple', '''Vigneras, Louis-André''')::text, '&', '<->')::tsquery
    // =  'vigneras' <-> 'louis-andré' <-> 'louis' <-> 'andré'

    StringBuilder t = cql2tsqueryAnd(s, removeAccents);
    t.insert(0, "replace(");
    t.append("::text, '&', '<->')::tsquery");
    return t;
  }

  /**
   * Test if s for sure is a syntactically correct SQL number.
   * <p>
   * Postgres also parses 1e but that may change in future.
   *
   * @param s String to test
   * @return true if s is a Postgres number, false if not or unknown
   */
  public static boolean isPostgresNumber(String s) {
    return POSTGRES_NUMBER_REGEXP.matcher(s).matches();
  }

  /**
   * Test if term has CQL wildcard characters: * or ?
   *
   * @param term
   * @return
   */
  public static boolean hasCqlWildCardd(String term) {
    String s = ("" + term).replaceAll(ESC_SLASH, "").replaceAll(ESC_STAR, "").replaceAll(ESC_QUEST, "");
    return s.contains("*") || s.contains("?");
  }

}
