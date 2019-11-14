package io.vertx.core.logging;

import io.vertx.core.spi.logging.LogDelegate;
import io.vertx.core.spi.logging.LogDelegateFactory;

/**
 * @see FolioLog4j2LogDelegate
 */
public class FolioLog4j2LogDelegateFactory implements LogDelegateFactory {
  @Override
  public LogDelegate createDelegate(final String name) {
     return new FolioLog4j2LogDelegate(name);
  }
}
