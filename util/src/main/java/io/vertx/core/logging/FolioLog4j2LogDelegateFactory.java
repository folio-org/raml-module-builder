package io.vertx.core.logging;

import io.vertx.core.spi.logging.LogDelegate;
import io.vertx.core.spi.logging.LogDelegateFactory;

/**
 * A logger from this factory extends the logger from Log4j2LogDelegateFactory
 * so that an logger invocation with a single argument that is an instance of Throwable
 * logs both the Thowable's message and stacktrace.
 * <p>
 * This is in contrast to Log4j2LogDelegateFactory where the logger prints the message only,
 * no stacktrace.
 * <p>
 * Example for an Exception e: logger.error(e) is the same as logger.error(e.getMessage, e).
 * Use logger.error(e.toString()) or logger.error(e.getMessage()) to suppress the stacktrace.
 */
public class FolioLog4j2LogDelegateFactory implements LogDelegateFactory {
  @Override
  public LogDelegate createDelegate(final String name) {
     return new FolioLog4j2LogDelegate(name);
  }
}
