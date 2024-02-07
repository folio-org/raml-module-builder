package org.folio.cql2pgjson.util;

import java.util.regex.Pattern;
import org.folio.cql2pgjson.exception.QueryValidationException;

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
  @SuppressWarnings("squid:S3776")  // Suppress "Cognitive Complexity of methods should not be too high"
  // because the cognitive complexity metric threshold is a rule of thumb, here it's a false positive:
  // Splitting this method decreases readability.
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
   * Convert a CQL string into a single quoted SQL string suitable for to_tsquery and append it to t.
   *
   * @param s CQL string without leading or trailing double quote
   * @throws QueryValidationException if s contains an unmasked wildcard question mark or an unmasked
   *                                  star that is not at the of the string or before a space
   */
  @SuppressWarnings("squid:S3776")  // suppress "Cognitive Complexity of methods should not be too high"
  public static void appendCql2tsquery(StringBuilder t, CharSequence s) throws QueryValidationException {
    t.append("'");
    /** whether the previous character is a masking backslash */
    boolean backslash = false;

    final int length = s.length();
    for (int i=0; i<length; i++) {
      char c = s.charAt(i);
      switch (c) {
      case '\\':
        if (backslash) {
          t.append("\\\\");
        }
        backslash = ! backslash;
        break;
      case '*':
        if (! backslash && i + 1 < length && s.charAt(i + 1) != ' ') {
          throw new QueryValidationException(
              "* right truncation wildcard must be followed by space or end of string, but found " + s.charAt(i + 1));
        }
        t.append('*');
        break;
      case '?':
        if (! backslash) {
          throw new QueryValidationException("? wildcard not allowed in full text query string");
        }
        t.append(c);
        backslash = false;
        break;
      case '\'':
        t.append("''");
        backslash = false;
        break;
      default:
        t.append(c);
        backslash = false;
        break;
      }
    }
    t.append("'");
  }

  /**
   * @return true if s has " " or "\\ " (one backslash, one space) at pos, false otherwise.
   */
  private static boolean isSpace(String s, int pos) {
    return (s.length() > pos && s.charAt(pos) == ' ')
        || (s.length() > pos + 1 && s.charAt(pos) == '\\' && s.charAt(pos + 1) == ' ');
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
    int i = 0;
    // skip space and backslashed space
    for ( ; i<length; i++) {
      char c = s.charAt(i);
      if (c == ' ') {
        // skip space
        backslash = false;
      } else if (c == '\\') {
        backslash = true;
      } else {
        break;
      }
    }
    for ( ; i<length; i++) {
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
        backslash = false;
        if (isSpace(s, i + 1)) {
          // skip until the last space
          break;
        }
        if (i + 1 == length) {
          // don't create new empty word at the end
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
        break;
      case '&':   // Replace & by , so that we can replace all tsquery & by <-> for phrase search.
        // Replace regular single quote and all other characters that f_unaccent converts into
        // a regular single quote. This avoids masking the single quote (sql injection).
        // How to find these characters:
        // select * from (select chr(generate_series(1,       55295))) x(s) where f_unaccent(s) LIKE '%''%'
        // select * from (select chr(generate_series(57344, 1114111))) x(s) where f_unaccent(s) LIKE '%''%'
        // Or search on
        // https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=contrib/unaccent/unaccent.rules;hb=HEAD
      case '\'':  // a regular single quote
      case '‘':
      case '’':
      case '‛':
      case '′':
      case '＇':
        t.append(',');  // replace by comma to avoid masking and sql injection
        backslash = false;
        break;
      case 'ŉ':  // f_unaccent('ŉ') = regular single quote + n, see comment above
        t.append(",n");  // replace single quote by comma to avoid masking and sql injection
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
   * Test if term contains a CQL wildcard character (* or ?) that is not masked by \
   *
   * @param term the non-null String
   * @return true if found, false otherwise
   * @deprecated use {@link #hasCqlWildCard(CharSequence)} instead
   */
  @Deprecated
  public static boolean hasCqlWildCardd(String term) {
    return hasCqlWildCard(term);
  }

  /**
   * Test if term contains a CQL wildcard character (* or ?) that is not masked by \
   *
   * @param term the non-null CharSequence
   * @return true if found, false otherwise
   */
  public static boolean hasCqlWildCard(final CharSequence term) {
    final int length = term.length();
    for (int i = 0; i < length; i++) {
      final char c = term.charAt(i);
      if (c == '\\') {
        i++;  // skip the character that the backslash masks
        continue;
      }
      if (c == '*' || c == '?') {
        return true;
      }
    }
    return false;
  }
}
