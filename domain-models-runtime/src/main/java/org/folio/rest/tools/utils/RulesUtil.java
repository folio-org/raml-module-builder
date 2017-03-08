package org.folio.rest.tools.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.List;

import org.folio.rest.RestVerticle;
import org.folio.rulez.Rules;
import org.kie.api.runtime.KieSession;

/**
 * @author shale
 *
 */
public class RulesUtil {

  private static final Logger log  = LoggerFactory.getLogger(RulesUtil.class);

  /**
   * will overwrite existing drools session and re-create it
   * will rules from list
   * */
  public static void reloadDrools(Vertx vertx, List<String> rules,
      Handler<AsyncResult<Object>> handler){

      vertx.executeBlocking( doThis -> {
        try {
          KieSession temp = new Rules(rules).buildSession();
          RestVerticle.updateDroolsSession(temp);
          doThis.complete();
        } catch (Exception e) {
          log.error("Unable to load new rules, " + e.getMessage());
          doThis.fail("error");
        }
      } , doThisWhenDone -> {
        handler.handle(doThisWhenDone);
      });
  }
}
