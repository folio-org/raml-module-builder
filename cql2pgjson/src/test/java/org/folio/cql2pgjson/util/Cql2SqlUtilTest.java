package org.folio.cql2pgjson.util;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.folio.cql2pgjson.DatabaseTestBase;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.okapi.testing.UtilityClassTester;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class Cql2SqlUtilTest extends DatabaseTestBase {

  @BeforeClass
  public static void runOnceBeforeClass() throws Exception {
    setupDatabase();
    runGeneralFunctions();
  }

  @AfterClass
  public static void runOnceAfterClass() {
    closeDatabase();
  }

  @Test
  public void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(Cql2SqlUtil.class);
  }

  /**
   * Return the first two words. Words are non-space character sequences
   * separated by spaces.
   * <p>
   * Returns "" for any of the two words if no more word is found.
   * <p>
   * <
   * pre>
   * split("foo bar ") = { "foo", "bar" }; split(" ") = { "", "" };
   * </pre>
   *
   * @param s string to split
   * @return two words
   */
  private String[] split(String s) {
    String[] chunks = s.split(" +");
    String left = "";
    if (chunks.length >= 1) {
      left = chunks[0];
    }
    String right = "";
    if (chunks.length >= 2) {
      right = chunks[1];
    }
    return new String[]{left, right};
  }

  private List<List<String>> params(String... strings) {
    List<List<String>> params = new ArrayList<>();
    for (String s : strings) {
      params.add(Arrays.asList(split(s)));
    }
    return params;
  }

  public Object cql2likeParams() {
    return params(
      "           ",
      "'     ''   ",
      "a     a    ",
      "*     %    ",
      "?     _    ",
      "\\    \\\\ ",
      "\\*   \\*  ",
      "\\?   \\?  ",
      "\\%   \\%  ",
      "%     \\%  ",
      "_     \\_  ",
      "\\_   \\_  ",
      "\\'   ''   ",
      "\\\\  \\\\ "
    );
  }

  @Test
  @Parameters(method = "cql2likeParams")
  public void cql2like(String cql, String sql) {
    assertThat(Cql2SqlUtil.cql2like(cql), is(sql));
  }

  public Object cql2stringParams() {
    return params(
      "           ",
      "'     ''   ",
      "''    ''''",
      "a''b  a''''b",
      "a     a    ",
      "^     exception",
      "*     exception",
      "?     exception",
      "\\    exception",
      "\\x   x    ",
      "\\*   *    ",
      "\\?   ?    ",
      "\\'   ''   ",
      "\\\\  \\   "
    );
  }

  @Test
  @Parameters(method = "cql2stringParams")
  public void cql2string(String cql, String sql) {
    boolean caught = false;
    try {
      assertThat(Cql2SqlUtil.cql2string(cql), is(sql));
    } catch (QueryValidationException ex) {
      caught = true;
    }
    assertThat(sql.equals("exception"), is(caught));
  }

  public Object cql2regexpParams() {
    return params(
      "           ",
      "'     ''   ",
      "a     a    ",
      "*     .*   ",
      "?     .    ",
      "^     (^|$)",
      "\\    \\\\ ",
      "\\*   \\*  ",
      "\\?   \\?  ",
      "\\^   \\^  ",
      "\\%   %    ",
      "\\_   _    ",
      "(     \\(  ",
      "\\(   \\(  ",
      ")     \\)  ",
      "\\)   \\)  ",
      "{     \\{  ",
      "\\{   \\{  ",
      "}     \\}  ",
      "\\}   \\}  ",
      "[     \\[  ",
      "\\[   \\[  ",
      "]     \\]  ",
      "\\]   \\]  ",
      "$     \\$  ",
      "\\$   \\$  ",
      "\\'   ''   ",
      "\\\\  \\\\ "
    );
  }

  @Test
  @Parameters(method = "cql2regexpParams")
  public void cql2regexp(String cql, String sql) {
    assertThat(Cql2SqlUtil.cql2regexp(cql), is(sql));
  }

  @Test(expected = QueryValidationException.class)
  public void appendCql2tsqueryQuestionmark() throws QueryValidationException {
    Cql2SqlUtil.appendCql2tsquery(new StringBuilder(), "abc d?f");
  }

  @Test(expected = QueryValidationException.class)
  public void appendCql2tsqueryInnerStar() throws QueryValidationException {
    Cql2SqlUtil.appendCql2tsquery(new StringBuilder(), "abc d*f");
  }

  @Test
  public void appendCql2tsqueryMasking() throws QueryValidationException {
    StringBuilder sb = new StringBuilder();
    Cql2SqlUtil.appendCql2tsquery(sb, "abc d\\*f g\\?i j\\kl");
    assertThat(sb.toString(), is("'abc d*f g?i jkl'"));
  }

  private String selectTsvector(String field, boolean removeAccents) {
    return removeAccents ? "SELECT get_tsvector(f_unaccent('" + field.replace("'", "''") + "')) @@ "
                         : "SELECT get_tsvector('" + field.replace("'", "''") + "') @@ ";
  }

  private void assertCql2tsqueryAnd(String field, String query, boolean removeAccents, String result) {
    try {
      String sql = selectTsvector(field, removeAccents) + Cql2SqlUtil.cql2tsqueryAnd(query, removeAccents);
      assertThat(sql, firstColumn(sql), contains(result));
    } catch (QueryValidationException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertCql2tsqueryOr(String field, String query, boolean removeAccents, String result) {
    try {
      String sql = selectTsvector(field, removeAccents) + Cql2SqlUtil.cql2tsqueryOr(query, removeAccents);
      assertThat(sql, firstColumn(sql), contains(result));
    } catch (QueryValidationException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertCql2tsqueryPhrase(String field, String query, boolean removeAccents, String result) {
    try {
      String sql = selectTsvector(field, removeAccents) + Cql2SqlUtil.cql2tsqueryPhrase(query, removeAccents);
      assertThat(sql, firstColumn(sql), contains(result));
    } catch (QueryValidationException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertCql2tsqueryAnd(String field, String query, String result) {
    assertCql2tsqueryAnd(field, query, false, result);
  }

  private void assertCql2tsqueryOr(String field, String query, String result) {
    assertCql2tsqueryOr(field, query, false, result);
  }

  private void assertCql2tsqueryPhrase(String field, String query, String result) {
    assertCql2tsqueryPhrase(field, query, false, result);
  }

  List<String> cql2tsqueryParams() {
    return Arrays.asList(
        "abc",
        "abc*",
        " abc ",
        "  abc  ",
        "   abc   ",
        "\\ abc\\ ",
        "\\ \\ abc\\ \\ ",
        "\\ \\ \\ abc\\ \\ \\ ",
        "\\  abc\\  ",
        " \\ abc \\ ",
        "abc  xyz",
        "abc   xyz",
        "a b c",
        "abc def ghi",
        "abc* def* ghi*",  // 'abc':* & 'def':* & 'ghi':*
                           // minus in words, RMB-438
        "abc-def",         // 'abc-def' & 'abc' & 'def'
        "abc--def",        // 'abc' & 'def'
        "abc---def",       // 'abc' & 'def'
        "abc-def-ghi",     // 'abc-def-ghi' & 'abc' & 'def' & 'ghi'
        "abc-def-ghi*",    // 'abc-def-ghi':* & 'abc':* & 'def':* & 'ghi':*
        "abc-def ghi-jkl", // 'abc-def' & 'abc' & 'def' & 'ghi-jkl' & 'ghi' & 'jkl'
        "-abc def",        // 'abc' & 'def'
        "abc- def",        // 'abc' & 'def'
        "abc -def",        // 'abc' & 'def'
        "abc def-",        // 'abc' & 'def'
        "abc - def",       // minus as a word, RMB-439
        "abc/def",         // 'abc/def'
        "abc//def",        // 'abc' & '/def'
        "abc///def",       // 'abc' & '/def'
        "abc\\?def",       // masked ? wildcard
        "abc\\?\\?def",
        "abc\\*def",       // masked * wildcard
        "abc\\*\\*def",
        "abc\\\\def",      // masked \ backslash
        "abc<->def"        // quoting of <-> phrase operator
    );
  }

  @Test
  @Parameters(method = "cql2tsqueryParams")
  public void cql2tsqueryAnd(String term) {
    assertCql2tsqueryAnd(term, term, "t");
  }

  @Test
  @Parameters(method = "cql2tsqueryParams")
  public void cql2tsqueryOr(String term) {
    assertCql2tsqueryOr(term, term, "t");
  }

  @Test
  @Parameters(method = "cql2tsqueryParams")
  public void cql2tsqueryPhrase(String term) {
    assertCql2tsqueryPhrase(term, term, "t");
  }

  @Test
  @Parameters({
    // single quote masking, RMB-432
    "abc'def",
    "abc''def",
    "abc'''def",
    // f_unaccent converts these other single quotes into a regular single quote, RMB-537
    "abc‘def",
    "abc‘‘def",
    "abc‘‘‘def",
    "abcŉdef",  // f_unaccent('ŉ') = regular single quote + n
    "abcŉŉdef",
    "abcŉŉŉdef",
    "abc’def",
    "abc’’def",
    "abc’’’def",
    "abc‛def",
    "abc‛‛def",
    "abc‛‛‛def",
    "abc′def",
    "abc′′def",
    "abc′′′def",
    "abc＇def",
    "abc＇＇def",
    "abc＇＇＇def",
  })
  public void cql2tsquerySingleQuote(String term) {
    assertCql2tsqueryAnd   (term, term, true, "t");
    assertCql2tsqueryOr    (term, term, true, "t");
    assertCql2tsqueryPhrase(term, term, true, "t");
  }

  @Test
  @Parameters({
    "abc,         abc,         t",
    "abc xyz,     abc xyz,     t",
    "abc,         xyz,         f",
    "abc-xyz,     xyz-abc,     f",
    "abc-xyz-qqq, abc-xyz,     f",
    "abc-xyz-qqq, xyz-qqq,     f",
    "abc-def,     xyz-abc-def, f",
    "abcdef,      abc*,        t",
    "abc-def,     abc-de*,     t",
    "abc-def,     abc-defg*,   f",
    "abc-def,     ab-def*,     f",
    "0cc175b9-c0f1-b6a8-31c3-99e269772661, 0cc175b9-c0f1-b6a8-31c3-99e269772661, t",
    "0cc175b9-c0f1-b6a8-31c3-99e269772661, 0cc175b9-c0f1-31c3-b6a8-99e269772661, f",
    "0cc175b9-c0f1-b6a8-31c3-99e269772661, 0cc175b9-31c3-b6a8-c0f1-99e269772661, f",
    "0cc175b9-c0f1-b6a8-31c3-99e269772661, 0cc175b9-31c3-c0f1-b6a8-99e269772661, f",
    "0cc175b9-c0f1-b6a8-31c3-99e269772661, 0cc175b9-b6a8-c0f1-31c3-99e269772661, f",
    "0cc175b9-c0f1-b6a8-31c3-99e269772661, 0cc175b9-b6a8-31c3-c0f1-99e269772661, f",
  })
  public void cql2tsqueryAndPhrase(String field, String query, String result) {
    assertCql2tsqueryAnd(field, query, result);
    assertCql2tsqueryPhrase(field, query, result);
  }

  @Test
  @Parameters({
    "ábc,         âbc",
    "ábc-xöz,     âbc-xôz",
  })
  public void cql2tsqueryAccents(String field, String query) {
    assertCql2tsqueryAnd(field, query, false, "f");
    assertCql2tsqueryAnd(field, query, true, "t");
    assertCql2tsqueryOr(field, query, false, "f");
    assertCql2tsqueryOr(field, query, true, "t");
    assertCql2tsqueryPhrase(field, query, false, "f");
    assertCql2tsqueryPhrase(field, query, true, "t");
  }

  private String [][] cql2tsqueryAndParams() {
    // cannot use JUnitParams splitting, it splits at |
    return new String [][] {
      { "abc",         "abc xyz",        "f" },
      { "abc xyz",     "abc",            "t" },
      { "abc xyz qqq", "ab* xy* qq*",    "t" },
      { "abc xyz qqq", "ab* xz* qq*",    "f" },
      { "abc xyz qqq", "ab* xy* qq*",    "t" },
      { "abc xyz qqq", "ab* xy  qq*",    "f" },
      { "abc",         "abc|xyz",        "f" },  // check | masking, this is the tsquery OR operator
      { "xyz",         "abc|xyz",        "f" },
      { "abc",         "abc||xyz",       "f" },
      { "abc",         "abc | xyz",      "f" },
      { "abc",         "abc || xyz",     "f" },
      { "abc",         "abc<->abc",      "t" },  // check <-> masking, this is the tsquery PHRASE operator
      { "abc",         "abc<-><->abc",   "t" },
      { "abc",         "abc <-> abc",    "t" },
      { "abc",         "abc <-><-> abc", "t" }
    };
  }

  @Test
  @Parameters(method = "cql2tsqueryAndParams")
  public void cql2tsqueryAnd(String field, String query, String result) {
    assertCql2tsqueryAnd(field, query, result);
  }

  @Test
  @Parameters({
    "abc,         abc,            t",
    "abc,         xyz,            f",
    "abc,         abc xyz,        t",
    "abc,         abc=xyz,        t",
    "abc xyz,     abc,            t",
    "xyz abc,     abc,            t",
    "abc qqq,     ab* xy*,        t",
    "qqq xyz,     ab* xy*,        t",
    "abc qqq,     xy* ab*,        t",
    "qqq xyz,     xy* ab*,        t",
    "abc,         ac*,            f",
    "abc,         abc&xyz,        t",  // check & masking, this is the tsquery AND operator
    "xyz,         abc&xyz,        t",
    "abc,         abc&&xyz,       t",
    "abc,         abc & xyz,      t",
    "abc,         abc && xyz,     t",
    "abc,         abc<->xyz,      t",  // check <-> masking, this is the tsquery PHRASE operator
    "xyz,         abc<->xyz,      t",
    "abc,         abc<-><->xyz,   t",
    "abc,         abc <-> xyz,    t",
    "abc,         abc <-><-> xyz, t",
  })
  public void cql2tsqueryOr(String field, String query, String result) {
    assertCql2tsqueryOr(field, query, result);
  }

  private String [][] cql2tsqueryPhraseParams() {
    // cannot use JUnitParams splitting, it splits at |
    return new String [][] {
      { "abc",             "abc xyz",         "f" },
      { "abc xyz",         "abc",             "t" },
      { "xyz abc",         "abc",             "t" },
      { "abc xyz",         "ab* xy*",         "t" },
      { "qqq abc xyz sss", "ab* xy*",         "t" },
      { "qqq abc xyz sss", "ab* ss*",         "f" },
      { "abc-def uvw-xyz", "abc-de* uvw-xy*", "t" },
      { "ab-def uv-xyz",   "abc-de* uvw-xy*", "f" },
      { "abc xyz",         "xyz|abc",         "f" },  // check | masking, this is the tsquery OR operator
      { "abc xyz",         "xyz||abc",        "f" },
      { "abc xyz",         "xyz | abc",       "f" },
      { "abc xyz",         "xyz || abc",      "f" },
      { "abc xyz",         "xyz&abc",         "f" },  // check & masking, this is the tsquery AND operator
      { "abc xyz",         "xyz&&abc",        "f" },
      { "abc xyz",         "xyz & abc",       "f" },
      { "abc xyz",         "xyz && abc",      "f" },
      { "12345678-9999-0000-1234-012345678901", "12345678-9999-0000-1234-012345678901", "t" },
      { "12345678-9999-0000-1234-012345678901", "12345678-1234-0000-9999-012345678901", "f" }
    };
  }

  @Test
  @Parameters(method = "cql2tsqueryPhraseParams")
  public void cql2tsqueryPhrase(String field, String query, String result) {
    assertCql2tsqueryPhrase(field, query, result);
  }

  List<String> cql2tsqueryExceptionParams() {
    return Arrays.asList(
      "?",
      "? abc",
      "?abc",
      "ab?c",
      "abc?",
      "abc ?",
      "*.",
      "*. abc",
      "*abc",
      "ab*c",
      "abc *."
    );
  }

  @Test(expected = QueryValidationException.class)
  @Parameters(method = "cql2tsqueryExceptionParams")
  public void cql2tsqueryAndException(String s) throws QueryValidationException {
    Cql2SqlUtil.cql2tsqueryAnd(s, true);
  }

  @Test(expected = QueryValidationException.class)
  @Parameters(method = "cql2tsqueryExceptionParams")
  public void cql2tsqueryOrException(String s) throws QueryValidationException {
    Cql2SqlUtil.cql2tsqueryOr(s, true);
  }

  @Test(expected = QueryValidationException.class)
  @Parameters(method = "cql2tsqueryExceptionParams")
  public void cql2tsqueryPhraseException(String s) throws QueryValidationException {
    Cql2SqlUtil.cql2tsqueryPhrase(s, true);
  }

  @Test
  @Parameters({
    // 6 examples from
    // https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS-NUMERIC
    "42",
    "3.5",
    "4.",
    ".001",
    "5e2",
    "1.925e-3",
    "0",
    "00",
    "1",
    "9",
    "10",
    "+1",
    "+0",
    "-1",
    "-0",
    "01",
    "001",
    "0099",
    "123.456e789",
    "-123.456e-789",
    "+123.456e+789",})
  public void isPostgresNumber(String term) {
    assertThat(Cql2SqlUtil.isPostgresNumber(term), is(true));
  }

  @Test
  @Parameters({
    "e",
    ".",
    ".e2",
    "1e2e",
    "1e", // this one is against the SQL spec but Postgres can parse it (assumes 1e0 resulting in 1)
  })
  public void isNotPostgresNumber(String term) {
    assertThat(Cql2SqlUtil.isPostgresNumber(term), is(false));
  }

  @Test
  @Parameters({       // without double backslash required for Java String
    "?",
    "*",
    "abc?",
    "abc*",
    "abc?*",
    "abc\\\\*",       // abc\\*
    "abc\\\\?",       // abc\\?
    "abc\\\\*\\\\?",  // abc\\*\\?
    "abc\\*\\\\?",    // abc\*\\?
    "abc\\\\*\\?",    // abc\\*\?
    "abc\\*\\?*",     // abc\*\?*
    "abc\\*\\??",     // abc\*\??
  })
  public void hasCqlWildCard(String term) {
    assertThat(Cql2SqlUtil.hasCqlWildCardd(term), is(true));
  }

  @Test
  @Parameters({        // without double backslash required for Java String
    "\\?",             // \?
    "\\*",             // \*
    "abc",             // abc
    "abc\\?",          // abc\?
    "abc\\*",          // abc\*
    "abc\\\\\\?",      // abc\\\?
    "abc\\\\\\*",      // abc\\\*
    "abc\\\\\\\\\\?",  // abc\\\\\?
    "abc\\\\\\\\\\*",  // abc\\\\\*
    "abc\\*\\?",       // abc\*\?
  })
  public void hasNoCqlWildCard(String term) {
    assertThat(Cql2SqlUtil.hasCqlWildCardd(term), is(false));
  }
}
