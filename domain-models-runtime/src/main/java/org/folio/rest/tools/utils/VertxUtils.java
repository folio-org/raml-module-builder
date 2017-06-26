package org.folio.rest.tools.utils;

import io.vertx.core.Context;
import io.vertx.core.Vertx;

public class VertxUtils {
    private VertxUtils() {
      throw new UnsupportedOperationException("Cannot instantiate utility class.");
    }

    /**
     * Return the Vertx of the current context; if there isn't a current context
     * create and return a new Vertx with default options.
     * @return the Vertx
     */
    public static Vertx getVertxFromContextOrNew() {
      Context context = Vertx.currentContext();

      if (context == null) {
          return Vertx.vertx();
      }

      return context.owner();
    }
}
