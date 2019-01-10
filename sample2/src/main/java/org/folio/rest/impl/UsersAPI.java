package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.Address;
import org.folio.rest.jaxrs.model.User;
import org.folio.rest.jaxrs.resource.UsersId;

public class UsersAPI implements UsersId {

  private static Logger log = LoggerFactory.getLogger(UsersAPI.class);

  @Override
  @Validate
  public void getUsersById(String id, String lang, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    log.info("getUsersById called");

    User user = new User();
    user.setAge(20);
    user.setFirstname("John");
    user.setLastname("Bar");

    Address address = new Address();
    address.setCity("Dragør");
    address.setZip("2791");
    address.setStreet("Parkvej 2");
    address.setCountry("Denmark");
    user.setAddress(address);

    if (!"en".equals(lang)) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetUsersByIdResponse.respond400()));
      return;
    }
    if ("1".equals(id)) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetUsersByIdResponse.respond200WithApplicationJson(user)));
    } else {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetUsersByIdResponse.respond404()));
    }
  }

  @Override
  public void postUsersById(String id, String entity, Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    log.info("content=" + entity);
    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
      PostUsersByIdResponse.respond201WithApplicationXml(entity)));
  }

  // @Override
  public void postUsersById(String id, User entity, Map<String, String> okapiHeaders,
    Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    log.info("content=" + entity);
    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
      PostUsersByIdResponse.respond201WithApplicationXml("<user/>")));
  }

}
