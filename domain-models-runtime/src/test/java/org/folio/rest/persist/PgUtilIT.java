package org.folio.rest.persist;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.User;
import org.folio.rest.jaxrs.model.UserdataCollection;
import org.folio.rest.jaxrs.model.Users;
import org.folio.rest.jaxrs.resource.support.ResponseDelegate;
import org.folio.rest.testing.UtilityClassTester;
import org.folio.rest.tools.utils.VertxUtils;
import org.hamcrest.junit.ExpectedException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import junit.framework.AssertionFailedError;

@RunWith(VertxUnitRunner.class)
public class PgUtilIT {
  @Rule
  public Timeout timeoutRule = Timeout.seconds(10);

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Rule
  public final ExpectedException exception = ExpectedException.none();
  /** If we start and stop our own embedded postgres */
  static private boolean ownEmbeddedPostgres = false;
  static private final Map<String,String> okapiHeaders = Collections.singletonMap("x-okapi-tenant", "testtenant");
  static private final String schema = PostgresClient.convertToPsqlStandard("testtenant");
  static private Vertx vertx;

  @BeforeClass
  public static void setUpClass(TestContext context) throws Exception {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    startEmbeddedPostgres(vertx);
    createUserTable(context);
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    if (ownEmbeddedPostgres) {
      PostgresClient.stopEmbeddedPostgres();
    }

    vertx.close(context.asyncAssertSuccess());
  }

  public static void startEmbeddedPostgres(Vertx vertx) throws IOException {
    if (PostgresClient.isEmbedded()) {
      // starting and stopping embedded postgres is done by someone else
      return;
    }

    // Read configuration
    PostgresClient postgresClient = PostgresClient.getInstance(vertx);

    if (! PostgresClient.isEmbedded()) {
      // some external postgres
      return;
    }

    postgresClient.startEmbeddedPostgres();

    // We started our own embedded postgres, we also need to stop it.
    ownEmbeddedPostgres = true;
  }

  private static void createUserTable(TestContext context) {
    execute(context, "CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;");
    execute(context, "CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;");
    execute(context, "DROP SCHEMA IF EXISTS " + schema + " CASCADE;");
    executeIgnore(context, "CREATE ROLE " + schema + " PASSWORD 'testtenant' NOSUPERUSER NOCREATEDB INHERIT LOGIN;");
    execute(context, "CREATE SCHEMA " + schema + " AUTHORIZATION " + schema);
    execute(context, "GRANT ALL PRIVILEGES ON SCHEMA " + schema + " TO " + schema);
    execute(context, "CREATE OR REPLACE FUNCTION f_unaccent(text) RETURNS text AS $func$ "
                     + "SELECT public.unaccent('public.unaccent', $1) $func$ LANGUAGE sql IMMUTABLE;");
    execute(context, "CREATE TABLE " + schema + ".users       (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);");
    execute(context, "CREATE TABLE " + schema + ".duplicateid (id UUID, jsonb JSONB NOT NULL);");
    execute(context, "CREATE TABLE " + schema + ".referencing (id UUID PRIMARY KEY, jsonb jsonb, "
                                                               + "userid UUID REFERENCES " + schema + ".users);");
    execute(context, "CREATE FUNCTION " + schema + ".userid() RETURNS TRIGGER AS "
                     + "$$ BEGIN NEW.userid = NEW.jsonb->>'userId'; RETURN NEW; END; $$ language 'plpgsql';");
    execute(context, "CREATE TRIGGER userid BEFORE INSERT OR UPDATE ON " + schema + ".referencing "
                     + "FOR EACH ROW EXECUTE PROCEDURE " + schema + ".userid();");
    execute(context, "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA " + schema + " TO " + schema);
  }

  private static void execute(TestContext context, String sql) {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    Async async = context.async();
    PostgresClient.getInstance(vertx).getClient().querySingle(sql, reply -> {
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
    PostgresClient.getInstance(vertx).getClient().querySingle(sql, reply -> {
      async.complete();
    });
    async.awaitSuccess();
  }

  private static String randomUuid() {
    return UUID.randomUUID().toString();
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
        nextHandler.handle(newHandler);
      } catch (Throwable e) {
        testContext.fail(e);
      }
      async.complete();
    };
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
        asyncAssertSuccess(testContext, 500, "42P01"));
  }

  private void insertReferencing(TestContext testContext, String id, String userId) {
    Async async = testContext.async();
    PgUtil.post("referencing", new Referencing(id, userId), okapiHeaders, vertx.getOrCreateContext(),
        ResponseImpl.class, asyncAssertSuccess(testContext, 201, post -> {
          async.complete();
    }));
    async.awaitSuccess(5000 /* ms */);
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
  public void getResponseWithout500(TestContext testContext) {
    PgUtil.get("users", User.class, UserdataCollection.class, "username=b", 0, 9,
        okapiHeaders, vertx.getOrCreateContext(), ResponseWithout500.class,
        asyncAssertFail(testContext, "respond500WithTextPlain"));
  }

  @Test
  public void getResponseWithout400(TestContext testContext) {
    PgUtil.get("users", User.class, UserdataCollection.class, "username=b", 0, 9,
        okapiHeaders, vertx.getOrCreateContext(), ResponseWithout400.class,
        asyncAssertFail(testContext, "respond400WithTextPlain"));
  }

  @Test
  public void getByIdInvalidUuid(TestContext testContext) {
    PgUtil.getById("users", User.class, "invalidUuid", okapiHeaders, vertx.getOrCreateContext(),
        Users.GetUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 404, "Invalid UUID format"));
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
    PgUtil.put("users", new User().withUsername("Momo").withId(randomUuid()), uuid,
        okapiHeaders, vertx.getOrCreateContext(),
        Users.PutUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 204, put -> assertGetById(testContext, uuid, "Momo")));
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
    PgUtil.put("referencing", new Referencing(refId, user2), refId, okapiHeaders, vertx.getOrCreateContext(),
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
    PgUtil.put("referencing", new Referencing(refId, user2), refId, okapiHeaders, vertx.getOrCreateContext(),
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
    Exception genericDatabaseException = PgExceptionUtilTest.genericDatabaseException('D', "barMessage");
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
    Exception genericDatabaseException = PgExceptionUtilTest.genericDatabaseException('D', "bazMessage");
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
    Exception genericDatabaseException = PgExceptionUtilTest.genericDatabaseException('D', "fooMessage");
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
    Exception genericDatabaseException = PgExceptionUtilTest.genericDatabaseException('D', "fooMessage");
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

    // limit=9
     c = searchForDataUnoptimized("username=foo sortBy username", 0, 9, testContext);
    val = c.getUsers().size();
    assertThat(val, is(9));

    // limit=5
    c = searchForDataUnoptimized("username=foo sortBy username", 0, 5, testContext);
    assertThat(c.getUsers().size(), is(5));

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

    exception.expect(ClassCastException.class);
    searchForDataUnoptimizedNoClass("username=foo sortBy username/sort.descending", 6, 3, testContext);

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

    // limit=9
     c = searchForData("username=foo sortBy username", 0, 9, testContext);
    val = c.getUsers().size();
    assertThat(val, is(9));
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
    searchForDataExpectFailure("username=foo sortBy username&%$sort.descending", 6, 3, testContext);
    exception.expect(NullPointerException.class);
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
    PgUtil.getWithOptimizedSql("users", User.class, UserdataCollection.class, "title", "username=a sortBy title",
        0, 10, okapiHeaders, vertx.getOrCreateContext(), ResponseWithout500.class, response -> {

          testContext.assertTrue( response.cause() instanceof NullPointerException);
        });
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

  private void setUpUserDBForTest(TestContext testContext, PostgresClient pg) {
    Async async = testContext.async();
    pg.execute("truncate " + schema + ".users CASCADE", testContext.asyncAssertSuccess(truncated -> {
      async.complete();
    }));
    async.awaitSuccess(1000 /* ms */);

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
        "users", User.class, UserdataCollection.class, "username", cql, offset, limit, okapiHeaders,
        vertx.getOrCreateContext(), ResponseWithout500.class, testContext.asyncAssertSuccess(response -> {
          if (response.getStatus() != 500) {
            testContext.fail("Expected status 500, got "
                + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
            async.complete();
            return;
          }
          async.complete();
    }));
    async.awaitSuccess(5000 /* ms */);
    return userdataCollection;
  }
  private UserdataCollection searchForDataWithNo400(String cql, int offset, int limit, TestContext testContext) {
    UserdataCollection userdataCollection = new UserdataCollection();
    Async async = testContext.async();
    PgUtil.getWithOptimizedSql(
        "users", User.class, UserdataCollection.class, "username", cql, offset, limit, okapiHeaders,
        vertx.getOrCreateContext(), ResponseWithout400.class, testContext.asyncAssertSuccess(response -> {
          if (response.getStatus() != 500) {
            testContext.fail("Expected status 500, got "
                + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
            async.complete();
            return;
          }

          async.complete();
    }));
    async.awaitSuccess(5000 /* ms */);
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
    async.awaitSuccess(5000 /* ms */);
    return userdataCollection;
  }
  private UserdataCollection searchForDataUnoptimizedNoClass(String cql, int offset, int limit, TestContext testContext) {
    UserdataCollection userdataCollection = new UserdataCollection();
    Async async = testContext.async();
    PgUtil.get(
        "users", User.class, Object.class, cql, offset, limit, okapiHeaders,
        vertx.getOrCreateContext(), ResponseImpl.class, testContext.asyncAssertSuccess(response -> {
          if (response.getStatus() != 500) {
            testContext.fail("Expected status 500, got "
                + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
            async.complete();
            return;
          }
          UserdataCollection c = (UserdataCollection) response.getEntity();
          userdataCollection.setTotalRecords(c.getTotalRecords());
          userdataCollection.setUsers(c.getUsers());
          async.complete();
    }));
    async.awaitSuccess(5000 /* ms */);
    return userdataCollection;
  }
  private UserdataCollection searchForDataUnoptimizedNo500(String cql, int offset, int limit, TestContext testContext) {
    UserdataCollection userdataCollection = new UserdataCollection();
    Async async = testContext.async();
    PgUtil.get(
        "users", User.class, UserdataCollection.class, cql, offset, limit, okapiHeaders,
        vertx.getOrCreateContext(), ResponseWithout500.class, testContext.asyncAssertSuccess(response -> {
          if (response.getStatus() != 400) {
            testContext.fail("Expected status 400, got "
                + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
            async.complete();
            return;
          }
          UserdataCollection c = (UserdataCollection) response.getEntity();
          userdataCollection.setTotalRecords(c.getTotalRecords());
          userdataCollection.setUsers(c.getUsers());
          async.complete();
    }));
    async.awaitSuccess(5000 /* ms */);
    return userdataCollection;
  }
  private UserdataCollection searchForData(String cql, int offset, int limit, TestContext testContext) {
    UserdataCollection userdataCollection = new UserdataCollection();
    Async async = testContext.async();
    PgUtil.getWithOptimizedSql(
        "users", User.class, UserdataCollection.class, "username", cql, offset, limit, okapiHeaders,
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
    async.awaitSuccess(5000 /* ms */);
    return userdataCollection;
  }
  private UserdataCollection searchForDataNoClass(String cql, int offset, int limit, TestContext testContext) {
    UserdataCollection userdataCollection = new UserdataCollection();
    Async async = testContext.async();
    PgUtil.getWithOptimizedSql(
        "users", User.class, Object.class, "username", cql, offset, limit, okapiHeaders,
        vertx.getOrCreateContext(), ResponseImpl.class, testContext.asyncAssertSuccess(response -> {
          if (response.getStatus() != 500) {
            testContext.fail("Expected status 500, got "
                + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
            async.complete();
            return;
          }

          async.complete();
    }));
    async.awaitSuccess(5000 /* ms */);
    return userdataCollection;
  }
  private String searchForDataExpectFailure(String cql, int offset, int limit, TestContext testContext) {
    String responseString = new String();
    Async async = testContext.async();
    PgUtil.getWithOptimizedSql(
        "users", User.class, UserdataCollection.class, "username", cql, offset, limit, okapiHeaders,
        vertx.getOrCreateContext(), ResponseImpl.class, testContext.asyncAssertSuccess(response -> {
          if (response.getStatus() != 400) {
            testContext.fail("Expected status 400, got "
                + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
            async.complete();
            return;
          }
          String c = (String) response.getEntity();
          responseString.concat(c);
          async.complete();
    }));
    async.awaitSuccess(5000 /* ms */);
    return responseString;
  }
  private String searchForDataNullHeadersExpectFailure(String cql, int offset, int limit, TestContext testContext) {
    String responseString = new String();
    Async async = testContext.async();
    PgUtil.getWithOptimizedSql(
        "users", User.class, UserdataCollection.class, "username", cql, offset, limit, null,
        vertx.getOrCreateContext(), ResponseImpl.class, testContext.asyncAssertSuccess(response -> {
          if (response.getStatus() != 500) {
            testContext.fail("Expected status 500, got "
                + response.getStatus() + " " + response.getStatusInfo().getReasonPhrase());
            async.complete();
            return;
          }
          String c = (String) response.getEntity();
          responseString.concat(c);
          async.complete();
    }));
    async.awaitSuccess(5000 /* ms */);
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
        testContext.assertEquals(n, updated.getUpdated());
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
  };

  /**
   * Record of the table "referencing". referencing.userId is a foreign key to users.id.
   */
  public class Referencing {
    public String id;
    public String userId;
    public Referencing(String id, String userId) {
      this.id = id;
      this.userId = userId;
    }
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getUserId() {
      return userId;
    }
    public void setUserId(String userId) {
      this.userId = userId;
    }
  }
}
