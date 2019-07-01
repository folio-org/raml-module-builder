package org.folio.cql2pgjson.util;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.rest.testing.UtilityClassTester;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class Cql2SqlUtilTest {

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
}
