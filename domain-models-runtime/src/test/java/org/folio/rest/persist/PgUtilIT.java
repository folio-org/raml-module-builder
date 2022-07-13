package org.folio.rest.persist;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import javax.ws.rs.core.Response;

import io.vertx.pgclient.PgException;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.okapi.testing.UtilityClassTester;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.Referencing;
import org.folio.rest.jaxrs.model.User;
import org.folio.rest.jaxrs.model.UserdataCollection;
import org.folio.rest.jaxrs.model.Users;
import org.folio.rest.jaxrs.model.Users.PostUsersResponse;
import org.folio.rest.jaxrs.resource.support.ResponseDelegate;
import org.folio.rest.persist.ddlgen.SchemaMaker;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.RoutingContext;
import junit.framework.AssertionFailedError;

@RunWith(VertxUnitRunner.class)
public class PgUtilIT {
  @Rule
  public Timeout timeoutRule = Timeout.seconds(40);

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  private int QUERY_TIMEOUT = 0;

  @Rule
  public final ExpectedException exception = ExpectedException.none();
  /** If we start and stop our own embedded postgres */
  static private final Map<String,String> okapiHeaders = Collections.singletonMap(XOkapiHeaders.TENANT, "testtenant");
  static private final String schema = PostgresClient.convertToPsqlStandard("testtenant");
  static private Vertx vertx;

  @BeforeClass
  public static void setUpClass(TestContext context) throws Exception {
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    vertx = VertxUtils.getVertxWithExceptionHandler();
    createUserTable(context);
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  private static final String DUMMY_VAL = "dummy value set by trigger";

  // a special user name used to test 409 response
  private static final String USER_409 = "user_409_raise_exception";

  private static void createUserTable(TestContext context) {
    execute(context, "CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;");
    execute(context, "CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;");
    execute(context, "DROP SCHEMA IF EXISTS " + schema + " CASCADE;");
    executeIgnore(context, "CREATE ROLE " + schema + " PASSWORD 'testtenant' NOSUPERUSER NOCREATEDB INHERIT LOGIN;");
    execute(context, "CREATE SCHEMA " + schema + " AUTHORIZATION " + schema);
    execute(context, "GRANT ALL PRIVILEGES ON SCHEMA " + schema + " TO " + schema);
    execute(context, "CREATE TABLE " + schema + ".users       (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);");
    execute(context, "CREATE TABLE " + schema + ".duplicateid (id UUID, jsonb JSONB NOT NULL);");
    execute(context, "CREATE TABLE " + schema + ".referencing (id UUID PRIMARY KEY, jsonb jsonb, "
                                                               + "userid UUID REFERENCES " + schema + ".users);");
    execute(context, "CREATE FUNCTION " + schema + ".userid() RETURNS TRIGGER AS "
                     + "$$ BEGIN NEW.userid = NEW.jsonb->>'userId'; RETURN NEW; END; $$ language 'plpgsql';");
    execute(context, "CREATE TRIGGER userid BEFORE INSERT OR UPDATE ON " + schema + ".referencing "
                     + "FOR EACH ROW EXECUTE PROCEDURE " + schema + ".userid();");
    execute(context, "CREATE FUNCTION " + schema + ".dummy() RETURNS TRIGGER AS "
                     + "$$ BEGIN NEW.jsonb = NEW.jsonb || '{\"dummy\" : \"" + DUMMY_VAL + "\"}'; RETURN NEW; END; $$ language 'plpgsql';");
    execute(context, "CREATE TRIGGER idusername BEFORE INSERT OR UPDATE ON " + schema + ".users "
                     + "FOR EACH ROW EXECUTE PROCEDURE " + schema + ".dummy();");
    execute(context, SchemaMaker.generateOptimisticLocking("testtenant", "raml_module_builder", "users"));

    execute(context, "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA " + schema + " TO " + schema);
    LoadGeneralFunctions.loadFuncs(context, PostgresClient.getInstance(vertx), schema);
  }

  private static void execute(TestContext context, String sql) {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    Async async = context.async();
    PostgresClientHelper.getClient(PostgresClient.getInstance(vertx)).query(sql).execute(reply -> {
      if (reply.failed()) {
        Throwable throwable = new AssertionFailedError(reply.cause().getMessage() + ": " + sql);
        throwable.setStackTrace(stackTrace);
        context.fail(throwable);
      }
      async.complete();
    });
    async.awaitSuccess();
  }

  private static void executeIgnore(TestContext context, String sql) {
    Async async = context.async();
    PostgresClientHelper.getClient(PostgresClient.getInstance(vertx)).query(sql).execute(reply -> {
      async.complete();
    });
    async.awaitSuccess();
  }

  private static String randomUuid() {
    return UUID.randomUUID().toString();
  }

  private Future<User> getUser(String id) {
    PostgresClient postgresClient = PgUtil.postgresClient(vertx.getOrCreateContext(), okapiHeaders);
    return postgresClient.getById("users", id, User.class);
  }

  /**
   * Return a handler that, when invoked, asserts that the AsyncResult succeeded, the Response has httpStatus,
   * and the toString() of the entity of Response contains the snippet.
   */
  private Handler<AsyncResult<Response>> asyncAssertSuccess(TestContext testContext, int httpStatus, String snippet) {
    Async async = testContext.async();
    return handler -> {
      testContext.assertTrue(handler.succeeded(), "handler.succeeded()");
      Response response = handler.result();
      String entity = "null";
      if (response.getEntity() != null) {
        entity = response.getEntity().toString();
      }
      if (httpStatus != response.getStatus() || ! entity.contains(snippet)) {
        testContext.fail("Expected " + httpStatus + " and entity containing " + snippet + "\n"
            + "but got " + response.getStatus() + " with entity=" + entity);
      }
      async.complete();
    };
  }

  /**
   * Return a handler that asserts that the result is successful and the response has
   * the expected status, and then passes the handler to nextHandler. An Async of testContext
   * is created that completes after nextHandler has been executed. Any Throwable thrown by
   * nextHandler fails testContext, for example an assertThat failure.
   */
  private Handler<AsyncResult<Response>> asyncAssertSuccess(TestContext testContext, int httpStatus,
      Handler<AsyncResult<Response>> nextHandler) {

    Async async = testContext.async();
    return newHandler -> {
      if (newHandler.failed()) {
        testContext.fail(newHandler.cause());
      }
      testContext.assertEquals(httpStatus, newHandler.result().getStatus(), "http status");
      try {
        if (nextHandler != null) {
          nextHandler.handle(newHandler);
        }
      } catch (Throwable e) {
        testContext.fail(e);
      }
      async.complete();
    };
  }

  private Handler<AsyncResult<Response>> asyncAssertSuccess(TestContext testContext, int httpStatus) {
    return asyncAssertSuccess(testContext, httpStatus, (Handler<AsyncResult<Response>>) null);
  }

  /**
   * Assert that response has the httpStatus and its result is a User with expected username and uuid.
   * uuid=null accepts any uuid. Returns the uuid of the User in response.
   */
  private String assertStatusAndUser(TestContext testContext, AsyncResult<Response> result,
      int httpStatus, String username, String uuid) {

    testContext.assertTrue(result.succeeded(), "succeeded()");
    Response response = result.result();
    testContext.assertEquals(httpStatus, response.getStatus(), "status of entity=" + response.getEntity());
    if (response.getEntity() == null) {
      testContext.fail("Expected response with a User instance but it was null");
      return null;
    }
    if (! (response.getEntity() instanceof User)) {
      testContext.fail("Expected response with a User instance but type was "
          + response.getEntity().getClass().getName());
      return null;
    }
    User user = (User) response.getEntity();
    testContext.assertEquals(username, user.getUsername(), "getUsername()");
    if (uuid != null) {
      testContext.assertEquals(uuid, user.getId(), "User::getId()");
    }
    return user.getId();
  }

  /**
   * Call getById(uuid) and assert that the returned User has the expected username.
   */
  private void assertGetById(TestContext testContext, String uuid, String username) {
    Async async = testContext.async();
    PgUtil.getById("users", User.class, uuid,
        okapiHeaders, vertx.getOrCreateContext(), ResponseImpl.class, result -> {
          assertStatusAndUser(testContext, result, 200, username, uuid);
          async.complete();
        });
  }

  /**
   * Return a handler that, when invoked, asserts that the AsyncResult failed, and the message of the
   * cause of the failure contains the snippet.
   */
  private Handler<AsyncResult<Response>> asyncAssertFail(TestContext testContext, String snippet) {
    Async async = testContext.async();
    return handler -> {
      testContext.assertTrue(handler.failed(), "handler.failed()");
      String message = handler.cause().getMessage();
      testContext.assertTrue(message.contains(snippet),
          "'" + snippet + "' expected in error message: " + message);
      async.complete();
    };
  }

  @Test
  public void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(PgUtil.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void respond422IllegalArgumentException() throws Throwable {
    class C {
      @SuppressWarnings("unused")
      private void privateMethod(Errors entity) {
      }
    }
    Method method = C.class.getDeclaredMethod("privateMethod", Errors.class);
    PgUtil.respond422(method, "foo", "bar", "baz");
  }

  @Test
  public void deleteByNonexistingId(TestContext testContext) {
    PgUtil.deleteById("users", randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 404, "Not found"));
  }

  @Test
  public void deleteById(TestContext testContext) {
    String uuid = randomUuid();
    post(testContext, "Ronja", uuid, 201);
    assertGetById(testContext, uuid, "Ronja");
    PgUtil.deleteById("users", uuid, okapiHeaders, vertx.getOrCreateContext(),
        Users.DeleteUsersByUserIdResponse.class, asyncAssertSuccess(testContext, 204, delete ->
          PgUtil.getById("users", User.class, uuid, okapiHeaders, vertx.getOrCreateContext(),
              Users.GetUsersByUserIdResponse.class, asyncAssertSuccess(testContext, 404, ""))
        ));
  }

  @Test
  public void deleteByIdDuplicateUuid(TestContext testContext) {
    String uuid = randomUuid();
    execute(testContext, "INSERT INTO " + schema + ".duplicateid VALUES "
        + "('" + uuid + "', '{}'),"
        + "('" + uuid + "', '{}')" );
    PgUtil.deleteById("duplicateid", uuid,
        okapiHeaders, vertx.getOrCreateContext(),
        Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 500, "Deleted 2 records in duplicateid for id: " + uuid));
  }

  @Test
  public void deleteByInvalidUuid(TestContext testContext) {
    PgUtil.deleteById("users", "invalidid", okapiHeaders, vertx.getOrCreateContext(),
        Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 400, "Invalid UUID format"));
  }

  @Test
  public void deleteByInvalidUuid422(TestContext testContext) {
    PgUtil.deleteById("users", "invalidid", okapiHeaders, vertx.getOrCreateContext(),
        ResponseWith422.class,
        asyncAssertSuccess(testContext, 422, response -> {
          Errors errors = (Errors) response.result().getEntity();
          assertThat(errors.getErrors(), hasSize(1));
          Error error = errors.getErrors().get(0);
          assertThat(error.getMessage(), containsString("Invalid UUID"));
          assertThat(error.getParameters(), hasSize(1));
          assertThat(error.getParameters().get(0).getKey(), is("users.id"));
          assertThat(error.getParameters().get(0).getValue(), is("invalidid"));
        }));
  }

  @Test
  public void deleteByIdNonexistingTable(TestContext testContext) {
    PgUtil.deleteById("otherTable", randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 500, "does not exist"));
  }

  private void insertReferencing(TestContext testContext, String id, String userId) {
    Async async = testContext.async();
    PgUtil.post("referencing", new Referencing().withId(id).withUserId(userId), okapiHeaders, vertx.getOrCreateContext(),
        ResponseImpl.class, asyncAssertSuccess(testContext, 201, post -> {
          async.complete();
    }));
    async.awaitSuccess(10000 /* ms */);
  }

  @Test
  public void deleteByIdForeignKeyViolation400(TestContext testContext) {
    String userId = randomUuid();
    String refId = randomUuid();
    post(testContext, "Folio", userId, 201);
    insertReferencing(testContext, refId, userId);
    PgUtil.deleteById("users", userId, okapiHeaders, vertx.getOrCreateContext(),
        Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 400, "is still referenced from table referencing"));
  }

  @Test
  public void deleteByIdForeignKeyViolation422(TestContext testContext) {
    String userId = randomUuid();
    String refId = randomUuid();
    post(testContext, "Folio", userId, 201);
    insertReferencing(testContext, refId, userId);
    PgUtil.deleteById("users", userId, okapiHeaders, vertx.getOrCreateContext(),
        ResponseWith422.class,
        asyncAssertSuccess(testContext, 422, response -> {
          Errors errors = (Errors) response.result().getEntity();
          assertThat(errors.getErrors(), hasSize(1));
          Error error = errors.getErrors().get(0);
          assertThat(error.getMessage(), containsString(
              "Cannot delete users.id = " + userId + " because id is still referenced from table referencing"));
          assertThat(error.getParameters(), hasSize(1));
          assertThat(error.getParameters().get(0).getKey(), is("users.id"));
          assertThat(error.getParameters().get(0).getValue(), is(userId));
        }));
  }

  @Test
  public void deleteByIdResponseWithout500(TestContext testContext) {
    PgUtil.deleteById("users", randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout500.class,
        asyncAssertFail(testContext, "respond500WithTextPlain"));
  }

  @Test
  public void deleteByIdResponseWithout204(TestContext testContext) {
    PgUtil.deleteById("users", randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout204.class,
        asyncAssertSuccess(testContext, 500, "respond204"));
  }

  @Test
  public void deleteByCQLwithNo500(TestContext testContext) {
    PgUtil.delete("users",  "username=delete_test",
        okapiHeaders, vertx.getOrCreateContext(), ResponseWithout500.class,
        asyncAssertFail(testContext, "respond500"));
  }

  @Test
  public void deleteByCQLwithNo400(TestContext testContext) {
    PgUtil.delete("users", "username=delete_test",
        okapiHeaders, vertx.getOrCreateContext(), ResponseWithout400.class,
        asyncAssertSuccess(testContext, 500, "respond400"));
  }

  @Test
  public void deleteByCQLwithNo204(TestContext testContext) {
    PgUtil.delete("users", "username=delete_test",
        okapiHeaders, vertx.getOrCreateContext(), ResponseWithout204.class,
        asyncAssertSuccess(testContext, 500, "respond204"));
  }

  @Test
  public void deleteByCQLNullHeaders(TestContext testContext) {
    PgUtil.delete("users",  "username==delete_test",
        null, vertx.getOrCreateContext(), Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 400, "null"));
  }

  @Test
  public void deleteByCQLWithoutCql(TestContext testContext) {
    PgUtil.delete("users", null,
        null, vertx.getOrCreateContext(), Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 400, "query with CQL expression is required"));
  }

  @Test
  public void deleteByCQLWithEmptyCql(TestContext testContext) {
    PgUtil.delete("users", "",
        null, vertx.getOrCreateContext(), Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 400, "query with CQL expression is required"));
  }

  @Test
  public void deleteByCQLWithWhitespaceCql(TestContext testContext) {
    PgUtil.delete("users", "\t\n \t\n ",
        null, vertx.getOrCreateContext(), Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 400, "query with CQL expression is required"));
  }

  @Test
  public void deleteByCQLOK(TestContext testContext) {
    PostgresClient pg = PostgresClient.getInstance(vertx, "testtenant");
    insert(testContext, pg, "delete_a",  1);
    insert(testContext, pg, "delete_b1",  1);
    insert(testContext, pg, "delete_b2",  1);

    // delete two
    {
      Async async = testContext.async();
      PgUtil.delete("users",  "username=delete_b*", okapiHeaders, vertx.getOrCreateContext(),
          Users.DeleteUsersByUserIdResponse.class,
          testContext.asyncAssertSuccess(res -> {
            assertThat(res.getStatus(), is(204));
            async.complete();
          }));
      async.await();
    }
    // and check 1 left
    {
      Async async = testContext.async();
      PgUtil.get(
          "users", User.class, UserdataCollection.class, "username=delete*", 0, 0, okapiHeaders,
          vertx.getOrCreateContext(), ResponseImpl.class, testContext.asyncAssertSuccess(response -> {
            if (response.getStatus() != 200) {
              testContext.fail("Expected status 200, got "
                  + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
              async.complete();
              return;
            }
            UserdataCollection c = (UserdataCollection) response.getEntity();
            assertThat(c.getTotalRecords(), is(1));
            async.complete();
          }));
      async.awaitSuccess(10000 /* ms */);
    }
  }

  @Test
  public void deleteByCQLSyntaxError(TestContext testContext) {
    PgUtil.delete("users",  "username==",
        okapiHeaders, vertx.getOrCreateContext(), Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 400, "expected index or term, got EOF"));
  }

  @Test
  public void deleteByCQLBadTable(TestContext testContext) {
    PgUtil.delete("users1",  "username==delete_test",
        okapiHeaders, vertx.getOrCreateContext(), Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 400, "relation \"testtenant_raml_module_builder.users1\" does not exist"));
  }

  @Test
  public void getWithoutTotalRecords(TestContext testContext) {
    PostgresClient pg = PostgresClient.getInstance(vertx, "testtenant");
    insert(testContext, pg, "getWithoutTotalRecords", 5);

    PgUtil.get("users", User.class, UserdataCollection.class, "id=*", "none", 0, 3,
        okapiHeaders, vertx.getOrCreateContext(), Users.GetUsersResponse.class,
        testContext.asyncAssertSuccess(result -> {
          assertThat(result.getStatus(), is(200));
          UserdataCollection collection = (UserdataCollection) result.getEntity();
          System.out.println(Json.encode(collection));
          assertThat(collection.getUsers().size(), is(3));
          assertThat(collection.getTotalRecords(), is(nullValue()));
        }));
  }

  @Test
  public void getByInvalidCql(TestContext testContext) {
    PgUtil.get("users", User.class, UserdataCollection.class, "/", 0, 9,
        okapiHeaders, vertx.getOrCreateContext(), Users.GetUsersResponse.class,
        asyncAssertSuccess(testContext, 400));
  }

  @Test
  public void getWithInvalidTable(TestContext testContext) {
    PgUtil.get("foo'bar", User.class, UserdataCollection.class, "username=b", 0, 9,
        okapiHeaders, vertx.getOrCreateContext(), Users.GetUsersResponse.class,
        asyncAssertSuccess(testContext, 500));
  }

  @Test(expected = NullPointerException.class)
  public void getWithInvalidTableWithout500(TestContext testContext) {
    PgUtil.get("foo'bar", User.class, UserdataCollection.class, "username=b", 0, 9,
        okapiHeaders, vertx.getOrCreateContext(), ResponseWithout500.class,
        asyncAssertSuccess(testContext, 500));
  }

  @Test
  public void getResponseWithout500(TestContext testContext) {
    PgUtil.get("users", User.class, UserdataCollection.class, "username=b", 0, 9,
        okapiHeaders, vertx.getOrCreateContext(), ResponseWithout500.class,
        asyncAssertFail(testContext, "respond500WithTextPlain"));
  }

  @Test
  public void getResponseWithout400(TestContext testContext) {
    PgUtil.get("users", User.class, UserdataCollection.class, "username=b", 0, 9,
        okapiHeaders, vertx.getOrCreateContext(), ResponseWithout400.class,
        asyncAssertSuccess(testContext, 500));
  }

  void assertSizeAndTotalRecords(String hasTotalRecords, int expectedSize, Integer expectedTotalRecords) {
    RoutingContext routingContext = mock(RoutingContext.class, Mockito.RETURNS_DEEP_STUBS);
    Buffer written = Buffer.buffer();
    Answer<Future<Void>> append = invocationOnMock -> {
      written.appendString(invocationOnMock.getArgument(0).toString());
      return null;
    };
    when(routingContext.response().write(anyString())).thenAnswer(append);
    when(routingContext.response().end(anyString())).thenAnswer(append);
    PgUtil.streamGet("users", User.class, "id=*", hasTotalRecords, 0, 12,
        null, "users", 0, routingContext, okapiHeaders, vertx.getOrCreateContext());

    verify(routingContext.response(), timeout(5000).atLeastOnce()).end(anyString());
    assertThat(new JsonObject(written).getJsonArray("users").size(), is(expectedSize));
    assertThat(new JsonObject(written).getInteger("totalRecords"), is(expectedTotalRecords));
  }

  @Test
  public void streamGetHasTotalRecords(TestContext testContext) {
    PostgresClient pg = PostgresClient.getInstance(vertx, "testtenant");
    truncateUsers(testContext, pg);
    insert(testContext, pg, "streamGetWithoutTotalRecords", 15);
    assertSizeAndTotalRecords("auto", 12, 15);
    assertSizeAndTotalRecords("none", 12, null);
  }

  @Test
  public void streamGetByInvalidCql(TestContext testContext) {
    RoutingContext routingContext = mock(RoutingContext.class, Mockito.RETURNS_DEEP_STUBS);
    PgUtil.streamGet("users", User.class, "/", 0, 9, null, "users",
        routingContext, okapiHeaders, vertx.getOrCreateContext());
    verify(routingContext.response(), timeout(100).atLeastOnce()).setStatusCode(400);
  }

  @Test
  public void streamGetException(TestContext testContext) {
    RoutingContext routingContext = mock(RoutingContext.class, Mockito.RETURNS_DEEP_STUBS);
    List<String> facets = mock(List.class);
    when(facets.size()).thenThrow(RuntimeException.class);
    PgUtil.streamGet("users", User.class, "/", 0, 9, facets, "users",
        routingContext, okapiHeaders, vertx.getOrCreateContext());
    verify(routingContext.response()).setStatusCode(500);
  }

  @Test
  public void streamGetException2(TestContext testContext) {
    RoutingContext routingContext = mock(RoutingContext.class, Mockito.RETURNS_DEEP_STUBS);
    List<String> facets = mock(List.class);
    when(facets.size()).thenThrow(RuntimeException.class);
    PgUtil.streamGet("users", User.class, "/", "auto", 0, 9, facets, "users", 0,
        routingContext, okapiHeaders, vertx.getOrCreateContext());
    verify(routingContext.response()).setStatusCode(500);
  }

  @Test
  public void getByIdInvalidUuid(TestContext testContext) {
    PgUtil.getById("users", User.class, "invalidUuid", okapiHeaders, vertx.getOrCreateContext(),
        Users.GetUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 404, "Invalid UUID"));
  }

  @Test
  public void getByIdPostgresError(TestContext testContext) {
    PgUtil.getById("doesnotexist", User.class, randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        Users.GetUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 500, "doesnotexist"));
  }

  @Test
  public void getByIdNotFound(TestContext testContext) {
    PgUtil.getById("users", User.class, randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        Users.GetUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 404, "Not found"));
  }

  @Test
  public void getByIdWithout500(TestContext testContext) {
    PgUtil.getById("users", User.class, randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout500.class,
        asyncAssertFail(testContext, "respond500WithTextPlain"));
  }

  @Test
  public void getByIdWithout200(TestContext testContext) {
    PgUtil.getById("users", User.class, randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout200.class,
        asyncAssertSuccess(testContext, 500, "respond200WithApplicationJson"));
  }

  /**
   * Post a new user, expect the httpStatus, and a returned User with matching username
   * and matching uuid if uuid was not null.
   * @param uuid  optional
   * @return uuid returned by the POST
   */
  private String post(TestContext testContext, String username, String uuid, int httpStatus) {
    Async async = testContext.async();
    String [] returnedUuid = new String [1];
    PgUtil.post("users", new User().withUsername(username).withId(uuid),
        okapiHeaders, vertx.getOrCreateContext(), ResponseImpl.class, result -> {
          returnedUuid[0] = assertStatusAndUser(testContext, result, httpStatus, username, uuid);
          async.complete();
        });
    async.awaitSuccess();
    return returnedUuid[0];
  }

  @Test
  public void post(TestContext testContext) {
    String uuid = post(testContext, "Frieda", null, 201);
    assertGetById(testContext, uuid, "Frieda");
  }

  @Test
  public void postWithId(TestContext testContext) {
    String uuid = randomUuid();
    post(testContext, "Cinderella", uuid, 201);
    assertGetById(testContext, uuid, "Cinderella");
  }

  @Test
  public void postDuplicateId400(TestContext testContext) {
    String uuid = randomUuid();
    post(testContext, "Anna", uuid, 201);
    PgUtil.post("users", new User().withUsername("Elsa").withId(uuid),
        okapiHeaders, vertx.getOrCreateContext(), ResponseImpl.class,
        asyncAssertSuccess(testContext, 400, "id value already exists in table users: " + uuid));
  }

  @Test
  public void postDuplicateId422(TestContext testContext) {
    String uuid = randomUuid();
    post(testContext, "Snow-White", uuid, 201);
    PgUtil.post("users", new User().withUsername("Rose-Red").withId(uuid),
        okapiHeaders, vertx.getOrCreateContext(), ResponseWith422.class,
        asyncAssertSuccess(testContext, 422, response -> {
          Errors errors = (Errors) response.result().getEntity();
          assertThat(errors.getErrors(), hasSize(1));
          Error error = errors.getErrors().get(0);
          assertThat(error.getMessage(), containsString("id value already exists in table users: " + uuid));
          assertThat(error.getParameters(), hasSize(1));
          assertThat(error.getParameters().get(0).getKey(), is("id"));
          assertThat(error.getParameters().get(0).getValue(), is(uuid));
        }));
  }

  @Test
  public void postInvalidId(TestContext testContext) {
    PgUtil.post("users", new User().withUsername("Kiri").withId("someInvalidUuid"),
        okapiHeaders, vertx.getOrCreateContext(), ResponseImpl.class,
        asyncAssertSuccess(testContext, 400, "Invalid UUID format"));
  }

  @Test
  public void postResponseWithUser201Method(TestContext testContext) {
    String uuid = randomUuid();
    PgUtil.post("users", new User().withUsername("Susi").withId(uuid),
        okapiHeaders, vertx.getOrCreateContext(), ResponseWithUserFor201Method.class,
        testContext.asyncAssertSuccess(result -> {
          assertThat(result.getStatus(), is(201));
          assertGetById(testContext, uuid, "Susi");
        }));
  }

  @Test
  public void postResponseWithUser201MethodAndTrigger(TestContext testContext) {
    String uuid = randomUuid();
    PgUtil.post("users", new User().withUsername("dummy").withId(uuid),
        okapiHeaders, vertx.getOrCreateContext(), ResponseWithUserFor201Method.class,
        testContext.asyncAssertSuccess(result -> {
          assertThat(result.getStatus(), is(201));
          assertThat(((User)result.getEntity()).getDummy(), is(DUMMY_VAL));
        }));
  }

  @Test
  public void postResponseWithout500(TestContext testContext) {
    PgUtil.post("users", "string", okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout500.class,
        asyncAssertFail(testContext, "respond500WithTextPlain"));
  }

  @Test
  public void postResponseWithoutHeadersFor201Class(TestContext testContext) {
    PgUtil.post("users", "string", okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithoutHeadersFor201Class.class,
        asyncAssertSuccess(testContext, 500, "HeadersFor201"));
  }

  @Test
  public void postResponseWithoutHeadersFor201Method(TestContext testContext) {
    PgUtil.post("users", "string", okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithoutHeadersFor201Method.class,
        asyncAssertSuccess(testContext, 500, ".headersFor201"));
  }

  @Test
  public void postResponseWithout201(TestContext testContext) {
    PgUtil.post("users", "string", okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout201.class,
        asyncAssertSuccess(testContext, 500, "respond201WithApplicationJson"));
  }

  @Test
  public void postResponseWithout400(TestContext testContext) {
    PgUtil.post("users", "string", okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout400.class,
        asyncAssertSuccess(testContext, 500, "respond400WithTextPlain"));
  }

  @Test
  public void postException(TestContext testContext) {
    PgUtil.post("users", "string", okapiHeaders, vertx.getOrCreateContext(),
        Users.PostUsersResponse.class,
        asyncAssertSuccess(testContext, 500, "java.lang.String.getId"));
  }

  @Test
  public void postPostgresError(TestContext testContext) {
    PgUtil.post("doesnotexist", new User(), okapiHeaders, vertx.getOrCreateContext(),
        Users.PostUsersResponse.class,
        asyncAssertSuccess(testContext, 500, "doesnotexist"));
  }

  @Test
  public void putNonexistingId(TestContext testContext) {
    String uuid = randomUuid();
    PgUtil.put("users", new User().withUsername("Rosamunde"), uuid,
        okapiHeaders, vertx.getOrCreateContext(),
        Users.PutUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 404, put -> {
          // make sure that a record with this uuid really hasn't been inserted
          PgUtil.getById("users", User.class, uuid, okapiHeaders, vertx.getOrCreateContext(),
              Users.GetUsersByUserIdResponse.class,
              asyncAssertSuccess(testContext, 404, "Not found"));
        }));
  }

  @Test
  public void put(TestContext testContext) {
    String uuid = randomUuid();
    post(testContext, "Pippilotta", uuid, 201);
    getUser(uuid)
    .compose(user -> PgUtil.put("users", user.withUsername("Momo").withId(randomUuid()),
        uuid, okapiHeaders, vertx.getOrCreateContext(), Users.PutUsersByUserIdResponse.class))
    .onComplete(asyncAssertSuccess(testContext, 204, put -> assertGetById(testContext, uuid, "Momo")));
  }

  @Test
  public void put409WhenOptimisticLockingVersionIsWrong(TestContext testContext) {
    String uuid = randomUuid();
    post(testContext, "Pippilotta", uuid, 201);
    getUser(uuid)
    .compose(user -> PgUtil.put("users", user.withUsername("Momo").withVersion(5),
        uuid, okapiHeaders, vertx.getOrCreateContext(), Users.PutUsersByUserIdResponse.class))
    .onComplete(asyncAssertSuccess(testContext, 409, put -> assertGetById(testContext, uuid, "Pippilotta")));
  }

  @Test
  public void putInvalidUuid(TestContext testContext) {
    PgUtil.put("users", new User().withUsername("Bö"), "SomeInvalidUuid",
        okapiHeaders, vertx.getOrCreateContext(),
        Users.PutUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 400, "Invalid UUID format"));
  }

  @Test
  public void putDuplicateUuid(TestContext testContext) {
    String uuid = randomUuid();
    execute(testContext, "INSERT INTO " + schema + ".duplicateid VALUES "
        + "('" + uuid + "', '{}'),"
        + "('" + uuid + "', '{}')" );
    PgUtil.put("duplicateid", new User(), uuid,
        okapiHeaders, vertx.getOrCreateContext(),
        Users.PutUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 500, "Updated 2 records in duplicateid for id: " + uuid));
  }

  @Test
  public void putForeignKeyViolation400(TestContext testContext) {
    String user1 = randomUuid();
    String user2 = randomUuid();
    String refId = randomUuid();
    post(testContext, "Folio", user1, 201);
    insertReferencing(testContext, refId, user1);
    PgUtil.put("referencing", new Referencing().withId(refId).withUserId(user2), refId, okapiHeaders, vertx.getOrCreateContext(),
        ResponseImpl.class,
        asyncAssertSuccess(testContext, 400, "referencing"));
  }

  @Test
  public void putForeignKeyViolation422(TestContext testContext) {
    String user1 = randomUuid();
    String user2 = randomUuid();
    String refId = randomUuid();
    post(testContext, "Folio", user1, 201);
    insertReferencing(testContext, refId, user1);
    PgUtil.put("referencing", new Referencing().withId(refId).withUserId(user2), refId, okapiHeaders, vertx.getOrCreateContext(),
        ResponseWith422.class,
        asyncAssertSuccess(testContext, 422, response -> {
          Errors errors = (Errors) response.result().getEntity();
          assertThat(errors.getErrors(), hasSize(1));
          Error error = errors.getErrors().get(0);
          assertThat(error.getMessage(), containsString(
              "Cannot set referencing.userid = " + user2 + " because it does not exist in users.id."));
          assertThat(error.getParameters(), hasSize(1));
          assertThat(error.getParameters().get(0).getKey(), is("referencing.userid"));
          assertThat(error.getParameters().get(0).getValue(), is(user2));
        }));
  }

  @Test
  public void putException(TestContext testContext) {
    PgUtil.put("users", "string", randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        Users.PutUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 500, "java.lang.String.setId"));
  }

  @Test
  public void putPostgresError(TestContext testContext) {
    PgUtil.put("doesnotexist", new User(), randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        Users.PutUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 500, "doesnotexist"));
  }

  @Test
  public void putResponseWithout500(TestContext testContext) {
    PgUtil.put("users", new User(), randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout500.class,
        asyncAssertFail(testContext, "respond500WithTextPlain"));
  }

  private Future<Response> postSync(List<User> entities, boolean upsert) {
    Map<String,String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    headers.putAll(okapiHeaders);
    headers.put(RestVerticle.OKAPI_USERID_HEADER, "okapiUser");
    return PgUtil.postSync("users", entities, 1000, upsert, headers,
        vertx.getOrCreateContext(), PostUsersResponse.class);
  }

  @Test
  public void postSync(TestContext testContext) {
    String id1 = randomUuid();
    String id2 = randomUuid();
    List<User> entities = Arrays.asList(new User().withId(id1), new User().withId(id2));
    List<User> entities1 = Arrays.asList(new User().withId(id1).withVersion(1), new User().withId(id2).withVersion(1));
    List<User> entities2 = Arrays.asList(new User().withId(id1).withVersion(2), new User().withId(id2).withVersion(2));
    boolean upsert = true;
    boolean noUpsert = false;
    postSync(entities, upsert)
    .onComplete(asyncAssertSuccess(testContext, 201))
    .compose(x -> getUser(id2))
    .onComplete(testContext.asyncAssertSuccess(user -> {
      assertThat(user.getMetadata().getCreatedByUserId(), is("okapiUser"));
    }))
    .compose(x -> postSync(entities, upsert))
    .onComplete(asyncAssertSuccess(testContext, 409))
    .compose(x -> postSync(entities1, upsert))
    .onComplete(asyncAssertSuccess(testContext, 201))
    .compose(x -> postSync(entities1, upsert))
    .onComplete(asyncAssertSuccess(testContext, 409))
    .compose(x -> postSync(entities2, noUpsert))
    .onComplete(asyncAssertSuccess(testContext, 422))
    .compose(x -> postSync(entities2, upsert))
    .onComplete(asyncAssertSuccess(testContext, 201));
  }

  @Test
  public void postSyncReponseWithout500(TestContext testContext) {
    PgUtil.postSync("users", Collections.emptyList(), 1000, true, okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout500.class, asyncAssertFail(testContext, "respond500WithTextPlain"));
  }

  @Test
  public void postSync413(TestContext testContext) {
    PgUtil.postSync("users", Arrays.asList(new User [1001]), 1000, true, okapiHeaders, vertx.getOrCreateContext(),
        PostUsersResponse.class, asyncAssertSuccess(testContext, 413,
            "Expected a maximum of 1000 records to prevent out of memory but got 1001"));
  }

  @Test
  public void postSync409(TestContext testContext) {
    String uuid = randomUuid();
    post(testContext, "abc", uuid, 201);
    List<User> users = Arrays.asList(new User(), new User().withId(uuid).withUsername("xyz"));
    postSync(users, true)
    .onComplete(result -> assertStatusAndUser(testContext, result, 409, "abc", uuid));
  }

  @Test
  public void postSyncException(TestContext testContext) {
    PgUtil.postSync("users", Collections.emptyList(), 1000, true, okapiHeaders, null,
        PostUsersResponse.class, asyncAssertSuccess(testContext, 500));
  }

  @Test
  public void message(TestContext testContext) {
    testContext.assertEquals(null, PgUtil.message(new RuntimeException()));
  }

  @Test
  public void responseLocation4Nulls(TestContext testContext) {
    Future<Response> future = PgUtil.response(new User(), "localhost", null, null, null, null);
    assertTrue(future.failed());
    assertThat(future.cause(), is(instanceOf(NullPointerException.class)));
  }

  @Test
  public void responseLocation3Nulls(TestContext testContext) throws Exception {
    Method respond500 = ResponseImpl.class.getMethod("respond500WithTextPlain", Object.class);
    Future<Response> future = PgUtil.response(new User(), "localhost", null, null, null, respond500);
    assertTrue(future.succeeded());
    assertThat(future.result().getStatus(), is(500));
  }

  @Test
  public void responseValueWithNullsFailResponseMethod(TestContext testContext) throws Exception {
    Method respond200 = ResponseImpl.class.getMethod("respond200WithApplicationJson", User.class);
    Future<Response> future = PgUtil.response(new User(), respond200, null);
    assertTrue(future.failed());
    assertThat(future.cause(), is(instanceOf(NullPointerException.class)));
  }

  public static void exceptionMethod() {
    throw new RuntimeException("some runtime exception");
  }

  public static void exceptionMethod(Object object) {
    throw new RuntimeException("some runtime exception");
  }

  @Test
  public void responseValueWithExceptionInValueMethod(TestContext testContext) throws Exception {
    Method exceptionMethod = PgUtilIT.class.getMethod("exceptionMethod", Object.class);
    Method respond500 = ResponseImpl.class.getMethod("respond500WithTextPlain", Object.class);
    Future<Response> future = PgUtil.response(new User(), exceptionMethod, respond500);
    assertTrue(future.succeeded());
    Response response = future.result();
    assertThat(response.getStatus(), is(500));
    assertThat(response.getEntity(), is("some runtime exception"));
  }

  @Test
  public void responseValueWithExceptionInFailResponseMethod(TestContext testContext) throws Exception {
    Method exceptionMethod = PgUtilIT.class.getMethod("exceptionMethod", Object.class);
    Future<Response> future = PgUtil.response(new User(), exceptionMethod, exceptionMethod);
    assertTrue(future.failed());
    assertThat(future.cause().getCause().getMessage(), is("some runtime exception"));
  }

  @Test
  public void response2Nulls(TestContext testContext) throws Exception {
    Future<Response> future = PgUtil.response(null, null);
    assertTrue(future.failed());
    assertThat(future.cause(), is(instanceOf(NullPointerException.class)));
  }

  @Test
  public void response204Null(TestContext testContext) throws Exception {
    Method respond204 = ResponseImpl.class.getMethod("respond204");
    Future<Response> future = PgUtil.response(respond204, null);
    assertTrue(future.failed());
    assertThat(future.cause(), is(instanceOf(NullPointerException.class)));
  }

  @Test
  public void responseNull500(TestContext testContext) throws Exception {
    Method respond500 = ResponseImpl.class.getMethod("respond500WithTextPlain", Object.class);
    Future<Response> future = PgUtil.response(null, respond500);
    assertTrue(future.succeeded());
    Response response = future.result();
    assertThat(response.getStatus(), is(500));
    assertThat(response.getEntity(), is("responseMethod must not be null"));
  }

  @Test
  public void responseWithExceptionInValueMethod(TestContext testContext) throws Exception {
    Method exceptionMethod = PgUtilIT.class.getMethod("exceptionMethod");
    Method respond500 = ResponseImpl.class.getMethod("respond500WithTextPlain", Object.class);
    Future<Response> future = PgUtil.response(exceptionMethod, respond500);
    assertTrue(future.succeeded());
    Response response = future.result();
    assertThat(response.getStatus(), is(500));
    assertThat(response.getEntity(), is("some runtime exception"));
  }

  @Test
  public void responseWithExceptionInFailResponseMethod(TestContext testContext) throws Exception {
    Method exceptionMethod = PgUtilIT.class.getMethod("exceptionMethod");
    Method exceptionMethod2 = PgUtilIT.class.getMethod("exceptionMethod", Object.class);
    Future<Response> future = PgUtil.response(exceptionMethod, exceptionMethod2);
    assertTrue(future.failed());
    assertThat(future.cause().getCause().getMessage(), is("some runtime exception"));
  }

  @Test
  public void response6WithException(TestContext testContext) {
    Future<Response> future = PgUtil.response((String)null, (String)null, (Throwable)null, null, null, null);
    assertTrue(future.failed());
    assertThat(future.cause(), is(instanceOf(NullPointerException.class)));
  }

  @Test
  public void responseInvalidUuidNull(TestContext testContext) {
    Future<Response> future = PgUtil.responseInvalidUuid(null, null, null, null, null);
    assertTrue(future.failed());
    assertThat(future.cause(), is(instanceOf(NullPointerException.class)));
  }

  @Test
  public void responseForeignKeyViolationException(TestContext testContext) {
    Future<Response> future = PgUtil.responseForeignKeyViolation(null, null, null, null, null, null);
    assertTrue(future.failed());
    assertThat(future.cause(), is(instanceOf(NullPointerException.class)));
  }

  @Test
  public void responseForeignKeyViolationNoMatch400(TestContext testContext) throws Exception {
    Exception genericDatabaseException = new PgException("", null, "", "barMessage");
    PgExceptionFacade exception = new PgExceptionFacade(genericDatabaseException);
    Method respond400 = ResponseImpl.class.getMethod("respond400WithTextPlain", Object.class);
    Method respond500 = ResponseImpl.class.getMethod("respond500WithTextPlain", Object.class);
    Future<Response> future = PgUtil.responseForeignKeyViolation("mytable", "myid", exception, null, respond400, respond500);
    assertTrue(future.succeeded());
    assertThat(future.result().getStatus(), is(400));
    assertThat(future.result().getEntity().toString(), containsString("barMessage"));
  }

  @Test
  public void responseForeignKeyViolationNoMatch422(TestContext testContext) throws Exception {
    Exception genericDatabaseException = new PgException("", null, "", "bazMessage");
    PgExceptionFacade exception = new PgExceptionFacade(genericDatabaseException);
    Method respond400 = ResponseImpl.class.getMethod("respond400WithTextPlain", Object.class);
    Method respond500 = ResponseImpl.class.getMethod("respond500WithTextPlain", Object.class);
    Method respond422 = PgUtil.respond422method(ResponseWith422.class);
    Future<Response> future = PgUtil.responseForeignKeyViolation("mytable", "myid", exception, respond422, respond400, respond500);
    assertTrue(future.succeeded());
    assertThat(future.result().getStatus(), is(422));
    Errors errors = (Errors) future.result().getEntity();
    assertThat(errors.getErrors().get(0).getMessage(), containsString("bazMessage"));
  }

  @Test
  public void responseUniqueViolationException(TestContext testContext) {
    Future<Response> future = PgUtil.responseUniqueViolation(null, null, null, null, null, null);
    assertTrue(future.failed());
    assertThat(future.cause(), is(instanceOf(NullPointerException.class)));
  }

  @Test
  public void responseUniqueViolationNoMatch400(TestContext testContext) throws Exception {
    Exception genericDatabaseException = new PgException("", null, "", "fooMessage");
    PgExceptionFacade exception = new PgExceptionFacade(genericDatabaseException);
    Method respond400 = ResponseImpl.class.getMethod("respond400WithTextPlain", Object.class);
    Method respond500 = ResponseImpl.class.getMethod("respond500WithTextPlain", Object.class);
    Future<Response> future = PgUtil.responseUniqueViolation("mytable", "myid", exception, null, respond400, respond500);
    assertTrue(future.succeeded());
    assertThat(future.result().getStatus(), is(400));
    assertThat(future.result().getEntity().toString(), containsString("fooMessage"));
  }

  @Test
  public void responseUniqueViolationNoMatch422(TestContext testContext) throws Exception {
    Exception genericDatabaseException = new PgException("", null, "", "fooMessage");
    PgExceptionFacade exception = new PgExceptionFacade(genericDatabaseException);
    Method respond400 = ResponseImpl.class.getMethod("respond400WithTextPlain", Object.class);
    Method respond500 = ResponseImpl.class.getMethod("respond500WithTextPlain", Object.class);
    Method respond422 = PgUtil.respond422method(ResponseWith422.class);
    Future<Response> future = PgUtil.responseUniqueViolation("mytable", "myid", exception, respond422, respond400, respond500);
    assertTrue(future.succeeded());
    assertThat(future.result().getStatus(), is(422));
    Errors errors = (Errors) future.result().getEntity();
    assertThat(errors.getErrors().get(0).getMessage(), containsString("fooMessage"));
  }

  @Test(expected = NoSuchMethodException.class)
  public void getListSetterMissingSetterMethod() throws Exception {
    PgUtil.getListSetter(String.class);
  }

  @Test
  public void getSortNodeException() {
    assertThat(PgUtil.getSortNode(null), is(nullValue()));
  }

  @Test
  public void canGetWithUnOptimizedSql(TestContext testContext) {

    PostgresClient pg = PostgresClient.getInstance(vertx, "testtenant");

    setUpUserDBForTest(testContext, pg);
    //unoptimized sql case
    UserdataCollection c = searchForDataUnoptimized("username=*", 0, 9, testContext);
    int val = c.getUsers().size();
    assertThat(val, is(9));
    assertThat(c.getTotalRecords(), is(greaterThanOrEqualTo(9)));  // estimation

    // limit=9
    c = searchForDataUnoptimized("username=foo sortBy username", 0, 9, testContext);
    val = c.getUsers().size();
    assertThat(val, is(9));
    assertThat(c.getTotalRecords(), is(greaterThanOrEqualTo(9)));  // estimation

    // limit=5
    c = searchForDataUnoptimized("username=foo sortBy username", 0, 5, testContext);
    assertThat(c.getUsers().size(), is(5));

    // limit=0
    c = searchForDataUnoptimized("username=foo sortBy username", 0, 0, testContext);
    assertThat(c.getUsers().size(), is(0));
    assertThat(c.getTotalRecords(), is(greaterThanOrEqualTo(1)));  // estimation

    // offset=6, limit=3
    c = searchForDataUnoptimized("username=foo sortBy username", 6, 3, testContext);
    assertThat(c.getUsers().size(), is(3));

    // offset=1, limit=8
    c = searchForDataUnoptimized("username=foo sortBy username", 1, 8, testContext);
    assertThat(c.getUsers().size(), is(8));

    // "b foo", offset=1, limit=20
    c = searchForDataUnoptimized("username=b sortBy username/sort.ascending", 1, 4, testContext);
    assertThat(c.getUsers().size(), is(4));

    // sort.descending, offset=1, limit=3
    c = searchForDataUnoptimized("username=foo sortBy username/sort.descending", 1, 3, testContext);
    assertThat(c.getUsers().size(), is(3));

    // sort.descending, offset=6, limit=3
    c = searchForDataUnoptimized("username=foo sortBy username/sort.descending", 6, 3, testContext);
    assertThat(c.getUsers().size(), is(3));

    searchForDataUnoptimizedNoClass("username=foo sortBy username/sort.descending", 6, 3, testContext);

    exception.expect(NullPointerException.class);
    searchForDataUnoptimizedNo500("username=foo sortBy username/sort.descending", 6, 3, testContext);
  }

  @Test
  public void canGetWithOptimizedSql(TestContext testContext) {
    PostgresClient pg = PostgresClient.getInstance(vertx, "testtenant");

    setUpUserDBForTest(testContext, pg);
    //unoptimized sql case
    UserdataCollection c = searchForData("username=*", 0, 9, testContext);
    int val = c.getUsers().size();
    assertThat(val, is(9));
    assertThat(c.getTotalRecords(), is(greaterThanOrEqualTo(9)));  // estimation

    // limit=9
    c = searchForData("username=foo sortBy username", 0, 9, testContext);
    val = c.getUsers().size();
    assertThat(val, is(9));
    assertThat(c.getTotalRecords(), is(greaterThanOrEqualTo(9)));  // estimation
    for (int i=0; i<5; i++) {
      User user = c.getUsers().get(i);
      assertThat(user.getUsername(), is("b foo " + (i + 1)));
    }
    for (int i=0; i<3; i++) {
      User user = c.getUsers().get(5 + i);
      assertThat(user.getUsername(), is("d foo " + (i + 1)));
    }

    // limit=5
    c = searchForData("username=foo sortBy username", 0, 5, testContext);
    assertThat(c.getUsers().size(), is(5));
    for (int i=0; i<5; i++) {
      User user = c.getUsers().get(i);
      assertThat(user.getUsername(), is("b foo " + (i + 1)));
    }

    // limit=0
    c = searchForData("username=foo sortBy username", 0, 0, testContext);
    assertThat(c.getUsers().size(), is(0));
    assertThat(c.getTotalRecords(), is(greaterThanOrEqualTo(1)));  // estimation

    // offset=99, limit=0
    c = searchForData("username=foo sortBy username", 99, 0, testContext);
    assertThat(c.getUsers().size(), is(0));
    assertThat(c.getTotalRecords(), is(greaterThanOrEqualTo(1)));  // estimation

    // offset=6, limit=3
    c = searchForData("username=foo sortBy username", 6, 3, testContext);
    assertThat(c.getUsers().size(), is(3));

    for (int i=0; i<3; i++) {
      User user = c.getUsers().get(i);
      assertThat(user.getUsername(), is("d foo " + (1 + i + 1)));
    }

    // offset=1, limit=8
    c = searchForData("username=foo sortBy username", 1, 8, testContext);
    assertThat(c.getUsers().size(), is(8));

    for (int i=0; i<4; i++) {
      User user = c.getUsers().get(i);
      assertThat(user.getUsername(), is("b foo " + (1 + i + 1)));
    }
    for (int i=0; i<4; i++) {
      User user = c.getUsers().get(4 + i);
      assertThat(user.getUsername(), is("d foo " + (i + 1)));
    }

    // "b foo", offset=1, limit=20
    c = searchForData("username=b sortBy username/sort.ascending", 1, 20, testContext);
    assertThat(c.getUsers().size(), is(4));

    for (int i=0; i<4; i++) {
      User user = c.getUsers().get(i);
      assertThat(user.getUsername(), is("b foo " + (1 + i + 1)));
    }

    // sort.descending, offset=1, limit=3
    c = searchForData("username=foo sortBy username/sort.descending", 1, 3, testContext);
    assertThat(c.getUsers().size(), is(3));

    for (int i=0; i<3; i++) {
      User user = c.getUsers().get(i);
      assertThat(user.getUsername(), is("d foo " + (4 - i)));
    }

    // sort.descending, offset=6, limit=3
    c = searchForData("username=foo sortBy username/sort.descending", 6, 3, testContext);
    assertThat(c.getUsers().size(), is(3));

    for (int i=0; i<3; i++) {
      User user = c.getUsers().get(i);
      assertThat(user.getUsername(), is("b foo " + (4 - i)));
    }
    searchForData("username=foo sortBy username&%$sort.descending", 6, 3, testContext);
    searchForDataNullHeadersExpectFailure("username=foo sortBy username/sort.descending", 6, 3, testContext);
    searchForDataNoClass("username=foo sortBy username/sort.descending",6, 3, testContext);
  }

  @Test
  public void getWithOptimizedSqlCanFailDueToResponse(TestContext testContext) {
    PostgresClient pg = PostgresClient.getInstance(vertx, "testtenant");
    setUpUserDBForTest(testContext, pg);
    exception.expect(NullPointerException.class);
    searchForDataWithNo500("username=b sortBy username/sort.ascending", 1, 20, testContext);
    searchForDataWithNo400("username=b sortBy username/sort.ascending", 1, 20, testContext);
  }

  @Test
  public void optimizedSQLwithNo500(TestContext testContext) {
    PgUtil.getWithOptimizedSql("users", User.class, UserdataCollection.class,
        "title", "username=a sortBy username", 0, 10,
        okapiHeaders, vertx.getOrCreateContext(), ResponseWithout500.class, testContext.asyncAssertFailure(e -> {
          assertThat(e, is(instanceOf(NullPointerException.class)));
        }));
  }

  @Test
  public void optimizedSqlCanSetSize() {
    int oldSize = PgUtil.getOptimizedSqlSize();
    int newSize = 54321;
    PgUtil.setOptimizedSqlSize(54321);
    assertThat(PgUtil.getOptimizedSqlSize(), is(newSize));
    PgUtil.setOptimizedSqlSize(oldSize);
    assertThat(PgUtil.getOptimizedSqlSize(), is(oldSize));
  }

  @Test
  public void optimizedSqlInvalidCql(TestContext testContext) {
    PgUtil.getWithOptimizedSql("users", User.class, UserdataCollection.class,
        "title", "/", 0, 10,
        okapiHeaders, vertx.getOrCreateContext(), Users.GetUsersResponse.class,
        asyncAssertSuccess(testContext, 400));
  }

  @Test
  public void optimizedSqlInvalidSortModifier(TestContext testContext) {
    String cql = "username=a sortBy username/sort.ignoreCase";
    String msg = searchForDataExpectFailure(cql, 0, 10, testContext);
    assertThat(msg, containsString("Unsupported modifier sort.ignorecase"));
  }

  @Test
  public void optimizedSqlInvalidSortModifier2(TestContext testContext) {
    String cql = "username=a sortBy username/sort.ascending/sort.respectAccents";
    String msg = searchForDataExpectFailure(cql, 0, 10, testContext);
    assertThat(msg, containsString("Unsupported modifier sort.respectaccents"));
  }

  private void truncateUsers(TestContext testContext, PostgresClient pg) {
    Async async = testContext.async();
    pg.execute("truncate " + schema + ".users CASCADE", testContext.asyncAssertSuccess(truncated -> {
      async.complete();
    }));
    async.awaitSuccess(1000 /* ms */);
  }

  private void setUpUserDBForTest(TestContext testContext, PostgresClient pg) {
    truncateUsers(testContext, pg);
    int optimizdSQLSize = 10000;
    int n = optimizdSQLSize / 2;
    insert(testContext, pg, "a", n);
    insert(testContext, pg, "b foo", 5);
    insert(testContext, pg, "c", n);
    insert(testContext, pg, "d foo", 5);
    insert(testContext, pg, "e", n);
  }

  private UserdataCollection searchForDataWithNo500(String cql, int offset, int limit, TestContext testContext) {
    UserdataCollection userdataCollection = new UserdataCollection();
    Async async = testContext.async();
    PgUtil.getWithOptimizedSql(
        "users", User.class, UserdataCollection.class, "username", cql, offset, limit,
        QUERY_TIMEOUT, okapiHeaders,
        vertx.getOrCreateContext(), ResponseWithout500.class, testContext.asyncAssertSuccess(response -> {
          if (response.getStatus() != 500) {
            testContext.fail("Expected status 500, got "
                + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
            async.complete();
            return;
          }
          async.complete();
    }));
    async.awaitSuccess(10000 /* ms */);
    return userdataCollection;
  }
  private UserdataCollection searchForDataWithNo400(String cql, int offset, int limit, TestContext testContext) {
    UserdataCollection userdataCollection = new UserdataCollection();
    Async async = testContext.async();
    PgUtil.getWithOptimizedSql(
        "users", User.class, UserdataCollection.class, "username", cql, offset, limit,
        QUERY_TIMEOUT, okapiHeaders,
        vertx.getOrCreateContext(), ResponseWithout400.class, testContext.asyncAssertSuccess(response -> {
          if (response.getStatus() != 500) {
            testContext.fail("Expected status 500, got "
                + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
            async.complete();
            return;
          }

          async.complete();
    }));
    async.awaitSuccess(10000 /* ms */);
    return userdataCollection;
  }
  private UserdataCollection searchForDataUnoptimized(String cql, int offset, int limit, TestContext testContext) {
    UserdataCollection userdataCollection = new UserdataCollection();
    Async async = testContext.async();
    PgUtil.get(
        "users", User.class, UserdataCollection.class, cql, offset, limit, okapiHeaders,
        vertx.getOrCreateContext(), ResponseImpl.class, testContext.asyncAssertSuccess(response -> {
          if (response.getStatus() != 200) {
            testContext.fail("Expected status 200, got "
                + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
            async.complete();
            return;
          }
          UserdataCollection c = (UserdataCollection) response.getEntity();
          userdataCollection.setTotalRecords(c.getTotalRecords());
          userdataCollection.setUsers(c.getUsers());
          async.complete();
    }));
    async.awaitSuccess(10000 /* ms */);
    return userdataCollection;
  }
  private UserdataCollection searchForDataUnoptimizedNoClass(String cql, int offset, int limit, TestContext testContext) {
    UserdataCollection userdataCollection = new UserdataCollection();
    Async async = testContext.async();
    PgUtil.get(
        "users", User.class, Object.class, cql, offset, limit, okapiHeaders,
        vertx.getOrCreateContext(), ResponseImpl.class, testContext.asyncAssertSuccess(response -> {
          testContext.assertEquals(500, response.getStatus());
          async.complete();
    }));
    async.awaitSuccess(10000 /* ms */);
    return userdataCollection;
  }
  private UserdataCollection searchForDataUnoptimizedNo500(String cql, int offset, int limit, TestContext testContext) {
    UserdataCollection userdataCollection = new UserdataCollection();
    Async async = testContext.async();
    PgUtil.get(
        "users", User.class, UserdataCollection.class, cql, offset, limit, okapiHeaders,
        vertx.getOrCreateContext(), ResponseWithout500.class, testContext.asyncAssertSuccess(response -> {
          async.complete();
    }));
    async.awaitSuccess(10000 /* ms */);
    return userdataCollection;
  }
  private UserdataCollection searchForData(String cql, int offset, int limit, TestContext testContext) {
    UserdataCollection userdataCollection = new UserdataCollection();
    Async async = testContext.async();
    PgUtil.getWithOptimizedSql(
        "users", User.class, UserdataCollection.class, "username", cql, offset, limit,
        QUERY_TIMEOUT, okapiHeaders,
        vertx.getOrCreateContext(), ResponseImpl.class, testContext.asyncAssertSuccess(response -> {
          if (response.getStatus() != 200) {
            testContext.fail("Expected status 200, got "
                + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
            async.complete();
            return;
          }
          UserdataCollection c = (UserdataCollection) response.getEntity();
          userdataCollection.setTotalRecords(c.getTotalRecords());
          userdataCollection.setUsers(c.getUsers());
          async.complete();
    }));
    async.awaitSuccess(10000 /* ms */);
    return userdataCollection;
  }
  private UserdataCollection searchForDataNoClass(String cql, int offset, int limit, TestContext testContext) {
    UserdataCollection userdataCollection = new UserdataCollection();
    Async async = testContext.async();
    PgUtil.getWithOptimizedSql(
        "users", User.class, Object.class, "username", cql, offset, limit, QUERY_TIMEOUT, okapiHeaders,
        vertx.getOrCreateContext(), ResponseImpl.class, testContext.asyncAssertSuccess(response -> {
          testContext.assertEquals(500, response.getStatus());
          async.complete();
    }));
    async.awaitSuccess(10000 /* ms */);
    return userdataCollection;
  }
  private String searchForDataExpectFailure(String cql, int offset, int limit, TestContext testContext) {
    StringBuilder responseString = new StringBuilder();
    Async async = testContext.async();
    PgUtil.getWithOptimizedSql(
        "users", User.class, UserdataCollection.class, "username", cql, offset, limit,
        QUERY_TIMEOUT, okapiHeaders,
        vertx.getOrCreateContext(), ResponseImpl.class, testContext.asyncAssertSuccess(response -> {
          if (response.getStatus() != 400) {
            testContext.fail("Expected status 400, got "
                + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
            async.complete();
            return;
          }
          String c = (String) response.getEntity();
          responseString.append(c);
          async.complete();
    }));
    async.awaitSuccess(10000 /* ms */);
    return responseString.toString();
  }
  private String searchForDataNullHeadersExpectFailure(String cql, int offset, int limit, TestContext testContext) {
    String responseString = new String();
    Async async = testContext.async();
    PgUtil.getWithOptimizedSql(
        "users", User.class, UserdataCollection.class, "username", cql, offset, limit,
        QUERY_TIMEOUT, null,
        vertx.getOrCreateContext(), ResponseImpl.class, testContext.asyncAssertSuccess(response -> {
          testContext.assertEquals(500, response.getStatus());
          async.complete();
    }));
    async.awaitSuccess(10000 /* ms */);
    return responseString;
  }

  /**
   * Insert n records into instance table where the title field is build using
   * prefix and the number from 1 .. n.
   */
  private void insert(TestContext testContext, PostgresClient pg, String prefix, int n) {
    Async async = testContext.async();
    String table = schema + ".users ";
    String sql = "INSERT INTO " + table +
        " SELECT md5(username)::uuid, json_build_object('username', username, 'id', md5(username)::uuid)" +
        "  FROM (SELECT '" + prefix + " ' || generate_series(1, " + n + ") AS username) AS subquery";
    pg.execute(sql, testContext.asyncAssertSuccess(updated -> {
        testContext.assertEquals(n, updated.rowCount());
        async.complete();
      }));
    async.awaitSuccess(10000 /* ms */);
  }

  static class ResponseImpl extends ResponseDelegate {
    public static class AnotherInnerClass {  // for code coverage of the for loop in PgUtil.post
      public String foo;
    }
    protected ResponseImpl(Response response, Object entity) {
      super(response, entity);
    }
    protected ResponseImpl(Response response) {
      super(response);
    }
    private static Response plain(int status, Object entity) {
      Response response = Response.status(status).header("Content-Type", "text/plain").entity(entity).build();
      return new ResponseImpl(response, entity);
    }
    public static Response respond200WithApplicationJson(User entity) {
      Response response = Response.status(200).header("Content-Type", "application/json").entity(entity).build();
      return new ResponseImpl(response, entity);
    }
    public static Response respond200WithApplicationJson(UserdataCollection entity) {
      Response response = Response.status(200).header("Content-Type", "application/json").entity(entity).build();
      return new ResponseImpl(response, entity);
    }
    public static class HeadersFor201 extends HeaderBuilderBase {
      private HeadersFor201() {
      }
      public HeadersFor201 withLocation(final String p) {
        headerMap.put("Location", String.valueOf(p));;
        return this;
      }
    }
    public static HeadersFor201 headersFor201() {
      return new HeadersFor201();
    }
    public static Response respond201WithApplicationJson(Object entity, HeadersFor201 headers) {
      ResponseBuilder responseBuilder = Response.status(201).header("Content-Type", "application/json").entity(entity);
      Response response = headers.toResponseBuilder(responseBuilder).build();
      return new ResponseImpl(response, entity);
    }
    public static Response respond204() {
      return new ResponseImpl(Response.status(204).build());
    }
    public static Response respond400WithTextPlain(Object entity) {
      return plain(400, entity);
    }
    public static Response respond404WithTextPlain(Object entity) {
      return plain(404, entity);
    }
    public static Response respond409WithTextPlain(Object entity) {
      return plain(409, entity);
    }
    public static Response respond500WithTextPlain(Object entity) {
      return plain(500, entity);
    }
  };

  static class ResponseWith422 extends ResponseImpl {
    private ResponseWith422(Response response) {
      super(response);
    }
    public static Response respond422WithApplicationJson(Errors entity) {
      Response.ResponseBuilder responseBuilder = Response.status(422).header("Content-Type", "application/json");
      responseBuilder.entity(entity);
      return new ResponseImpl(responseBuilder.build(), entity);
    }
  }

  static class ResponseWithout200 extends ResponseDelegate {
    private ResponseWithout200(Response response) {
      super(response);
    }
    public static class HeadersFor201 extends ResponseImpl.HeadersFor201 {
    }
    public static HeadersFor201 headersFor201() {
      return new HeadersFor201();
    }
    public static Response respond201WithApplicationJson(Object entity, HeadersFor201 headers) {
      return ResponseImpl.respond201WithApplicationJson(entity, headers);
    }
    public static Response respond204() {
      return ResponseImpl.respond204();
    }
    public static Response respond400WithTextPlain(Object entity) {
      return ResponseImpl.respond400WithTextPlain(entity);
    }
    public static Response respond500WithTextPlain(Object entity) {
      return ResponseImpl.respond500WithTextPlain(entity);
    }
  };

  static class ResponseWithoutHeadersFor201Method extends ResponseDelegate {
    private ResponseWithoutHeadersFor201Method(Response response) {
      super(response);
    }
    public static Response respond200WithApplicationJson(User entity) {
      return ResponseImpl.respond200WithApplicationJson(entity);
    }
    public static class HeadersFor201 extends ResponseImpl.HeadersFor201 {
    }
    public static Response respond201WithApplicationJson(Object entity, HeadersFor201 headers) {
      return ResponseImpl.respond201WithApplicationJson(entity, headers);
    }
    public static Response respond204() {
      return ResponseImpl.respond204();
    }
    public static Response respond400WithTextPlain(Object entity) {
      return ResponseImpl.respond400WithTextPlain(entity);
    }
    public static Response respond500WithTextPlain(Object entity) {
      return ResponseImpl.respond500WithTextPlain(entity);
    }
  };

  static class ResponseWithUserFor201Method extends ResponseDelegate {
    private ResponseWithUserFor201Method(Response response) {
      super(response);
    }
    public static Response respond200WithApplicationJson(User entity) {
      return ResponseImpl.respond200WithApplicationJson(entity);
    }
    public static class HeadersFor201 extends ResponseImpl.HeadersFor201 {
    }
    public static HeadersFor201 headersFor201() {
      return new HeadersFor201();
    }
    public static Response respond201WithApplicationJson(User entity, HeadersFor201 headers) {
      return ResponseImpl.respond201WithApplicationJson(entity, headers);
    }
    public static Response respond204() {
      return ResponseImpl.respond204();
    }
    public static Response respond400WithTextPlain(Object entity) {
      return ResponseImpl.respond400WithTextPlain(entity);
    }
    public static Response respond500WithTextPlain(Object entity) {
      return ResponseImpl.respond500WithTextPlain(entity);
    }
  };

  static class ResponseWithoutHeadersFor201Class extends ResponseDelegate {
    private ResponseWithoutHeadersFor201Class(Response response) {
      super(response);
    }
    public static Response respond200WithApplicationJson(User entity) {
      return ResponseImpl.respond200WithApplicationJson(entity);
    }
    public static Object headersFor201() {
      return null;
    }
    public static Response respond204() {
      return ResponseImpl.respond204();
    }
    public static Response respond400WithTextPlain(Object entity) {
      return ResponseImpl.respond400WithTextPlain(entity);
    }
    public static Response respond500WithTextPlain(Object entity) {
      return ResponseImpl.respond500WithTextPlain(entity);
    }
  };

  static class ResponseWithout201 extends ResponseDelegate {
    private ResponseWithout201(Response response) {
      super(response);
    }
    public static Response respond200WithApplicationJson(User entity) {
      return ResponseImpl.respond200WithApplicationJson(entity);
    }
    public static class HeadersFor201 extends ResponseImpl.HeadersFor201 {
    }
    public static HeadersFor201 headersFor201() {
      return new HeadersFor201();
    }
    public static Response respond204() {
      return ResponseImpl.respond204();
    }
    public static Response respond400WithTextPlain(Object entity) {
      return ResponseImpl.respond400WithTextPlain(entity);
    }
    public static Response respond500WithTextPlain(Object entity) {
      return ResponseImpl.respond500WithTextPlain(entity);
    }
  };

  static class ResponseWithout204 extends ResponseDelegate {
    private ResponseWithout204(Response response) {
      super(response);
    }
    public static Response respond200WithApplicationJson(User entity) {
      return ResponseImpl.respond200WithApplicationJson(entity);
    }
    public static class HeadersFor201 extends ResponseImpl.HeadersFor201 {
    }
    public static HeadersFor201 headersFor201() {
      return new HeadersFor201();
    }
    public static Response respond201WithApplicationJson(Object entity, HeadersFor201 headers) {
      return ResponseImpl.respond201WithApplicationJson(entity, headers);
    }
    public static Response respond400WithTextPlain(Object entity) {
      return ResponseImpl.respond400WithTextPlain(entity);
    }
    public static Response respond500WithTextPlain(Object entity) {
      return ResponseImpl.respond500WithTextPlain(entity);
    }
  };

  static class ResponseWithout400 extends ResponseDelegate {
    private ResponseWithout400(Response response) {
      super(response);
    }
    public static Response respond200WithApplicationJson(User entity) {
      return ResponseImpl.respond200WithApplicationJson(entity);
    }
    public static class HeadersFor201 extends ResponseImpl.HeadersFor201 {
    }
    public static HeadersFor201 headersFor201() {
      return new HeadersFor201();
    }
    public static Response respond201WithApplicationJson(Object entity, HeadersFor201 headers) {
      return ResponseImpl.respond201WithApplicationJson(entity, headers);
    }
    public static Response respond204() {
      return ResponseImpl.respond204();
    }
    public static Response respond500WithTextPlain(Object entity) {
      return ResponseImpl.respond500WithTextPlain(entity);
    }
  };

  static class ResponseWithout500 extends ResponseDelegate {
    private ResponseWithout500(Response response) {
      super(response);
    }
    public static Response respond200WithApplicationJson(User entity) {
      return ResponseImpl.respond200WithApplicationJson(entity);
    }
    public static class HeadersFor201 extends ResponseImpl.HeadersFor201 {
    }
    public static HeadersFor201 headersFor201() {
      return new HeadersFor201();
    }
    public static Response respond201WithApplicationJson(Object entity, HeadersFor201 headers) {
      return ResponseImpl.respond201WithApplicationJson(entity, headers);
    }
    public static Response respond204() {
      return ResponseImpl.respond204();
    }
    public static Response respond400WithTextPlain(Object entity) {
      return ResponseImpl.respond400WithTextPlain(entity);
    }
  }
}
