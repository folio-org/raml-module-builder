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
  
  private class MyUser implements User {
    private String firstName, lastName;
    private Number age;
    
    @Override
    public String getFirstname() {
      return firstName;
    }

    @Override
    public void setFirstname(String firstname) {
      this.firstName = firstname;
    }

    @Override
    public String getLastname() {
      return lastName;
    }

    @Override
    public void setLastname(String lastname) {
      this.lastName = lastname;
    }

    @Override
    public Number getAge() {
      return age;
    }

    @Override
    public void setAge(Number age) {
      this.age = age;
    }    
  }

  @Override
  public void getUsersById(String id, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    log.info("getUsersById called");
  
    User user = new MyUser();
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
