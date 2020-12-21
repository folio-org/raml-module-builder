package org.folio.rest.jaxrs.model;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.Pattern;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.support.ResponseDelegate;

@Path("/users")
public interface Users {
  /**
   * Return a list of users
   * @param query	JSON array [{"field1","value1","operator1"},{"field2","value2","operator2"},...,{"fieldN","valueN","operatorN"}]
   *
   * @param orderBy	Order by field: field A, field B
   *
   * @param order	Order
   * 	default value: "desc"
   * @param offset	Skip over a number of elements by specifying an offset value for the query
   * 	default value: "0"
   * 	minimum value: "0.0"
   * 	maximum value: "2.147483647E9"
   * @param limit	Limit the number of elements returned in the response
   * 	default value: "10"
   * 	minimum value: "0.0"
   * 	maximum value: "2.147483647E9"
   * @param facets	facets to return in the collection result set, can be suffixed by a count of facet values to return, for example, patronGroup:10 default to top 5 facet values
   * @param lang	Requested language. Optional. [lang=en]
   *
   * 	default value: "en"
   * 	pattern: "[a-zA-Z]{2}"
   * @param asyncResultHandler An AsyncResult<Response> Handler  {@link Handler} which must be called as follows - Note the 'GetPatronsResponse' should be replaced with '[nameOfYourFunction]Response': (example only) <code>asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetPatronsResponse.withJsonOK( new ObjectMapper().readValue(reply.result().body().toString(), Patron.class))));</code> in the final callback (most internal callback) of the function.
   * @param vertxContext
   *  The Vertx Context Object <code>io.vertx.core.Context</code>
   * @param okapiHeaders
   *  Case insensitive map of x-okapi-* headers passed in as part of the request <code>java.util.Map<String, String></code>  */
  @GET
  @Produces({
      "application/json",
      "text/plain"
  })
  @Validate
  void getUsers(@QueryParam("query") String query, @QueryParam("orderBy") String orderBy, @QueryParam("order") @DefaultValue("desc") UsersGetOrder order, @QueryParam("offset") @DefaultValue("0") @Min(0) @Max(2147483647) int offset, @QueryParam("limit") @DefaultValue("10") @Min(0) @Max(2147483647) int limit, @QueryParam("facets") List<String> facets, @QueryParam("lang") @DefaultValue("en") @Pattern(regexp = "[a-zA-Z]{2}") String lang, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext);

  /**
   * Create a user
   * @param lang	Requested language. Optional. [lang=en]
   *
   * 	default value: "en"
   * 	pattern: "[a-zA-Z]{2}"
   * @param entity <code>org.folio.rest.jaxrs.model.User</code>
   * {
   *   "username": "jhandey",
   *   "id": "7261ecaae3a74dc68b468e12a70b1aec",
   *   "active": true,
   *   "type": "patron",
   *   "patronGroup": "4bb563d9-3f9d-4e1e-8d1d-04e75666d68f",
   *   "meta": {
   *     "creation_date": "2016-11-05T0723",
   *     "last_login_date": ""
   *   },
   *   "personal": {
   *     "lastName": "Handey",
   *     "firstName": "Jack",
   *     "email": "jhandey@biglibrary.org",
   *     "phone": "2125551212"
   *   }
   * }
   *
   * @param asyncResultHandler An AsyncResult<Response> Handler  {@link Handler} which must be called as follows - Note the 'GetPatronsResponse' should be replaced with '[nameOfYourFunction]Response': (example only) <code>asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetPatronsResponse.withJsonOK( new ObjectMapper().readValue(reply.result().body().toString(), Patron.class))));</code> in the final callback (most internal callback) of the function.
   * @param vertxContext
   *  The Vertx Context Object <code>io.vertx.core.Context</code>
   * @param okapiHeaders
   *  Case insensitive map of x-okapi-* headers passed in as part of the request <code>java.util.Map<String, String></code>  */
  @POST
  @Produces({
      "application/json",
      "text/plain"
  })
  @Consumes("application/json")
  @Validate
  void postUsers(@QueryParam("lang") @DefaultValue("en") @Pattern(regexp = "[a-zA-Z]{2}") String lang, User entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext);

  /**
   * Get a single user
   * @param lang	Requested language. Optional. [lang=en]
   *
   * 	default value: "en"
   * 	pattern: "[a-zA-Z]{2}"
   * @param asyncResultHandler An AsyncResult<Response> Handler  {@link Handler} which must be called as follows - Note the 'GetPatronsResponse' should be replaced with '[nameOfYourFunction]Response': (example only) <code>asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetPatronsResponse.withJsonOK( new ObjectMapper().readValue(reply.result().body().toString(), Patron.class))));</code> in the final callback (most internal callback) of the function.
   * @param vertxContext
   *  The Vertx Context Object <code>io.vertx.core.Context</code>
   * @param okapiHeaders
   *  Case insensitive map of x-okapi-* headers passed in as part of the request <code>java.util.Map<String, String></code>  */
  @GET
  @Path("/{userId}")
  @Produces({
      "application/json",
      "text/plain"
  })
  @Validate
  void getUsersByUserId(@PathParam("userId") String userId, @QueryParam("lang") @DefaultValue("en") @Pattern(regexp = "[a-zA-Z]{2}") String lang, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext);

  /**
   * Delete user item with given {userId}
   *
   * @param lang	Requested language. Optional. [lang=en]
   *
   * 	default value: "en"
   * 	pattern: "[a-zA-Z]{2}"
   * @param asyncResultHandler An AsyncResult<Response> Handler  {@link Handler} which must be called as follows - Note the 'GetPatronsResponse' should be replaced with '[nameOfYourFunction]Response': (example only) <code>asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetPatronsResponse.withJsonOK( new ObjectMapper().readValue(reply.result().body().toString(), Patron.class))));</code> in the final callback (most internal callback) of the function.
   * @param vertxContext
   *  The Vertx Context Object <code>io.vertx.core.Context</code>
   * @param okapiHeaders
   *  Case insensitive map of x-okapi-* headers passed in as part of the request <code>java.util.Map<String, String></code>  */
  @DELETE
  @Path("/{userId}")
  @Produces("text/plain")
  @Validate
  void deleteUsersByUserId(@PathParam("userId") String userId, @QueryParam("lang") @DefaultValue("en") @Pattern(regexp = "[a-zA-Z]{2}") String lang, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext);

  /**
   * Update user item with given {userId}
   *
   * @param lang	Requested language. Optional. [lang=en]
   *
   * 	default value: "en"
   * 	pattern: "[a-zA-Z]{2}"
   * @param entity <code>org.folio.rest.jaxrs.model.User</code>
   * {
   *   "username": "jhandey",
   *   "id": "7261ecaae3a74dc68b468e12a70b1aec",
   *   "active": true,
   *   "type": "patron",
   *   "patronGroup": "4bb563d9-3f9d-4e1e-8d1d-04e75666d68f",
   *   "meta": {
   *     "creation_date": "2016-11-05T0723",
   *     "last_login_date": ""
   *   },
   *   "personal": {
   *     "lastName": "Handey",
   *     "firstName": "Jack",
   *     "email": "jhandey@biglibrary.org",
   *     "phone": "2125551212"
   *   }
   * }
   *
   * @param asyncResultHandler An AsyncResult<Response> Handler  {@link Handler} which must be called as follows - Note the 'GetPatronsResponse' should be replaced with '[nameOfYourFunction]Response': (example only) <code>asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetPatronsResponse.withJsonOK( new ObjectMapper().readValue(reply.result().body().toString(), Patron.class))));</code> in the final callback (most internal callback) of the function.
   * @param vertxContext
   *  The Vertx Context Object <code>io.vertx.core.Context</code>
   * @param okapiHeaders
   *  Case insensitive map of x-okapi-* headers passed in as part of the request <code>java.util.Map<String, String></code>  */
  @PUT
  @Path("/{userId}")
  @Produces("text/plain")
  @Consumes("application/json")
  @Validate
  void putUsersByUserId(@PathParam("userId") String userId, @QueryParam("lang") @DefaultValue("en") @Pattern(regexp = "[a-zA-Z]{2}") String lang, User entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext);

  class GetUsersResponse extends ResponseDelegate {
    private GetUsersResponse(Response response, Object entity) {
      super(response, entity);
    }

    private GetUsersResponse(Response response) {
      super(response);
    }

    public static GetUsersResponse respond200WithApplicationJson(UserdataCollection entity) {
      Response.ResponseBuilder responseBuilder = Response.status(200).header("Content-Type", "application/json");
      responseBuilder.entity(entity);
      return new GetUsersResponse(responseBuilder.build(), entity);
    }

    public static GetUsersResponse respond400WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(400).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new GetUsersResponse(responseBuilder.build(), entity);
    }

    public static GetUsersResponse respond401WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(401).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new GetUsersResponse(responseBuilder.build(), entity);
    }

    public static GetUsersResponse respond500WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(500).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new GetUsersResponse(responseBuilder.build(), entity);
    }
  }

  class PostUsersResponse extends ResponseDelegate {
    private PostUsersResponse(Response response, Object entity) {
      super(response, entity);
    }

    private PostUsersResponse(Response response) {
      super(response);
    }

    public static PostUsersResponse respond422WithApplicationJson(Errors entity) {
      Response.ResponseBuilder responseBuilder = Response.status(422).header("Content-Type", "application/json");
      responseBuilder.entity(entity);
      return new PostUsersResponse(responseBuilder.build(), entity);
    }

    public static HeadersFor201 headersFor201() {
      return new HeadersFor201();
    }

    public static PostUsersResponse respond201() {
      Response.ResponseBuilder responseBuilder = Response.status(201);
      return new PostUsersResponse(responseBuilder.build());
    }

    public static PostUsersResponse respond201WithApplicationJson(Object entity, HeadersFor201 headers) {
      Response.ResponseBuilder responseBuilder = Response.status(201).header("Content-Type", "application/json");
      responseBuilder.entity(entity);
      headers.toResponseBuilder(responseBuilder);
      return new PostUsersResponse(responseBuilder.build(), entity);
    }

    public static PostUsersResponse respond400WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(400).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new PostUsersResponse(responseBuilder.build(), entity);
    }

    public static PostUsersResponse respond401WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(401).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new PostUsersResponse(responseBuilder.build(), entity);
    }

    public static PostUsersResponse respond409WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(409).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new PostUsersResponse(responseBuilder.build(), entity);
    }

    public static PostUsersResponse respond413WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(413).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new PostUsersResponse(responseBuilder.build(), entity);
    }

    public static PostUsersResponse respond500WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(500).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new PostUsersResponse(responseBuilder.build(), entity);
    }

    public static class HeadersFor201 extends HeaderBuilderBase {
      private HeadersFor201() {
      }

      public HeadersFor201 withLocation(final String p) {
        headerMap.put("Location", String.valueOf(p));;
        return this;
      }
    }
  }

  class GetUsersByUserIdResponse extends ResponseDelegate {
    private GetUsersByUserIdResponse(Response response, Object entity) {
      super(response, entity);
    }

    private GetUsersByUserIdResponse(Response response) {
      super(response);
    }

    public static GetUsersByUserIdResponse respond200WithApplicationJson(User entity) {
      Response.ResponseBuilder responseBuilder = Response.status(200).header("Content-Type", "application/json");
      responseBuilder.entity(entity);
      return new GetUsersByUserIdResponse(responseBuilder.build(), entity);
    }

    public static GetUsersByUserIdResponse respond404WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(404).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new GetUsersByUserIdResponse(responseBuilder.build(), entity);
    }

    public static GetUsersByUserIdResponse respond500WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(500).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new GetUsersByUserIdResponse(responseBuilder.build(), entity);
    }
  }

  class DeleteUsersByUserIdResponse extends ResponseDelegate {
    private DeleteUsersByUserIdResponse(Response response, Object entity) {
      super(response, entity);
    }

    private DeleteUsersByUserIdResponse(Response response) {
      super(response);
    }

    public static DeleteUsersByUserIdResponse respond204() {
      Response.ResponseBuilder responseBuilder = Response.status(204);
      return new DeleteUsersByUserIdResponse(responseBuilder.build());
    }

    public static DeleteUsersByUserIdResponse respond404WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(404).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new DeleteUsersByUserIdResponse(responseBuilder.build(), entity);
    }

    public static DeleteUsersByUserIdResponse respond400WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(400).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new DeleteUsersByUserIdResponse(responseBuilder.build(), entity);
    }

    public static DeleteUsersByUserIdResponse respond500WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(500).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new DeleteUsersByUserIdResponse(responseBuilder.build(), entity);
    }
  }

  class PutUsersByUserIdResponse extends ResponseDelegate {
    private PutUsersByUserIdResponse(Response response, Object entity) {
      super(response, entity);
    }

    private PutUsersByUserIdResponse(Response response) {
      super(response);
    }

    public static PutUsersByUserIdResponse respond204() {
      Response.ResponseBuilder responseBuilder = Response.status(204);
      return new PutUsersByUserIdResponse(responseBuilder.build());
    }

    public static PutUsersByUserIdResponse respond404WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(404).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new PutUsersByUserIdResponse(responseBuilder.build(), entity);
    }

    public static PutUsersByUserIdResponse respond400WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(400).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new PutUsersByUserIdResponse(responseBuilder.build(), entity);
    }

    public static PutUsersByUserIdResponse respond409WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(409).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new PutUsersByUserIdResponse(responseBuilder.build(), entity);
    }

    public static PutUsersByUserIdResponse respond500WithTextPlain(Object entity) {
      Response.ResponseBuilder responseBuilder = Response.status(500).header("Content-Type", "text/plain");
      responseBuilder.entity(entity);
      return new PutUsersByUserIdResponse(responseBuilder.build(), entity);
    }
  }
}
