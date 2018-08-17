package io.vertx.core.logging;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FolioLog4j2LogDelegateTest {
  private static PrintStream oldOut;
  private static ByteArrayOutputStream stream = new ByteArrayOutputStream();
  private Logger log = new Logger(new FolioLog4j2LogDelegate(getClass().getName()));

  @BeforeAll
  static void beforeAll() {
    oldOut = System.out;
    System.setOut(new PrintStream(stream));
  }

  @AfterAll
  static void afterAll() {
    System.setOut(oldOut);
  }

  private void init(Level level) {
    stream.reset();
    Configurator.setLevel(getClass().getName(), level);
  }

  private String result() {
    try {
      stream.flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return stream.toString();
  }

  public static int countMatches(String haystack, String needle) {
    int count = 0;
    int i = 0;
    while (true) {
      i = haystack.indexOf(needle, i);
      if (i == -1) {
        break;
      }
      count++;
      i += needle.length();
    }
    return count;
  }

  private void allString(Level level, int expected) {
    init(level);
    log.fatal("foo");
    log.error("foo");
    log.warn("foo");
    log.info("foo");
    log.debug("foo");
    log.trace("foo");
    String result = result();
    assertThat(result, countMatches(result, "foo"), is(expected));
  }

  private void allException(Level level, int expected) {
    init(level);
    Exception e = new RuntimeException("foo");
    log.fatal(e);
    log.error(e);
    log.warn(e);
    log.info(e);
    log.debug(e);
    log.trace(e);
    String result = result();
    assertThat("number of messages in " + result,
        countMatches(result, "io.vertx.core.logging.FolioLog4j2LogDelegateTest - foo"),
        is(expected));
    assertThat("number of stacktraces in " + result,
        countMatches(result, ".allException(FolioLog4j2LogDelegateTest.java"),
        is(expected));
  }

  @Test
  void testString() {
    allString(Level.FATAL, 1);
    allString(Level.ERROR, 2);
    allString(Level.WARN,  3);
    allString(Level.INFO,  4);
    allString(Level.DEBUG, 5);
    allString(Level.TRACE, 6);
  }

  @Test
  void testException() {
    allException(Level.FATAL, 1);
    allException(Level.ERROR, 2);
    allException(Level.WARN,  3);
    allException(Level.INFO,  4);
    allException(Level.DEBUG, 5);
    allException(Level.TRACE, 6);
  }
}
