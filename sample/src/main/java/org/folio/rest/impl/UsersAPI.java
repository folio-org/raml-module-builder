package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.folio.rest.jaxrs.model.User;
import org.folio.rest.jaxrs.resource.UsersId;

public class UsersAPI implements UsersId {
  private static Logger log = LoggerFactory.getLogger(UsersAPI.class);

  @Override
  public void getUsersById(String id, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    log.info("getUsersById called");
  
    User user = new User();
    user.setAge(20);
    user.setFirstname("John");
    user.setLastname("Bar");
    
    if ("1".equals(id)) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetUsersByIdResponse.respond200WithApplicationJson(user)));
    } else {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetUsersByIdResponse.respond404()));
    }
  }
}
