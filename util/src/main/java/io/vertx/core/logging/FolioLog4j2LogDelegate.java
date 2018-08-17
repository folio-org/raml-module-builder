package io.vertx.core.logging;

import io.vertx.core.logging.Log4j2LogDelegate;

/**
 * Extends Log4j2LogDelegate so that an logger invocation with a single
 * argument that is an instance of Throwable logs both the Thowable's message
 * and stacktrace.
 * <p>
 * This is in contrast to Log4j2LogDelegate where a logger prints the message only, no stacktrace.
 * <p>
 * Example for an Exception e: logger.error(e) is the same as logger.error(e.getMessage, e).
 * Use logger.error(e.toString()) or logger.error(e.getMessage()) to suppress the stacktrace.
 * <p>
 * Configure for a single logger:
 * <pre>
 * {@code
 * Logger log = new Logger(new FolioLog4j2LogDelegate(getClass().getName()));
 * }
 * </pre>
 * <p>
 * Configure globally:
 * <pre>
 * {@code
 *   static {
 *    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,
 *      "io.vertx.core.logging.FolioLog4j2LogDelegateFactory");
 *   }
 * }
 * </pre>
 */
public class FolioLog4j2LogDelegate extends Log4j2LogDelegate {
  public FolioLog4j2LogDelegate(final String name) {
    // the super constructor is package protected, therefore
    // FolioLog4j2LogDelegate must be in package io.vertx.core.logging
    super(name);
  }

  @Override
  public void fatal(final Object o) {
    if (o instanceof Throwable) {
      Throwable t = (Throwable) o;
      super.fatal(t.getMessage(), t);
    } else {
      super.fatal(o);
    }
  }

  @Override
  public void error(final Object o) {
    if (o instanceof Throwable) {
      Throwable t = (Throwable) o;
      super.error(t.getMessage(), t);
    } else {
      super.error(o);
    }
  }

  @Override
  public void warn(final Object o) {
    if (o instanceof Throwable) {
      Throwable t = (Throwable) o;
      super.warn(t.getMessage(), t);
    } else {
      super.warn(o);
    }
  }

  @Override
  public void info(final Object o) {
    if (o instanceof Throwable) {
      Throwable t = (Throwable) o;
      super.info(t.getMessage(), t);
    } else {
      super.info(o);
    }
  }

  @Override
  public void debug(final Object o) {
    if (o instanceof Throwable) {
      Throwable t = (Throwable) o;
      super.debug(t.getMessage(), t);
    } else {
      super.debug(o);
    }
  }

  @Override
  public void trace(final Object o) {
    if (o instanceof Throwable) {
      Throwable t = (Throwable) o;
      super.trace(t.getMessage(), t);
    } else {
      super.trace(o);
    }
  }
}
