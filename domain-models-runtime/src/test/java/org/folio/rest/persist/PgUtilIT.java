package org.folio.rest.persist;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.folio.rest.jaxrs.model.User;
import org.folio.rest.jaxrs.model.Users;
import org.folio.rest.jaxrs.resource.support.ResponseDelegate;
import org.folio.rest.testing.UtilityClassTester;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class PgUtilIT {
  @Rule
  public Timeout timeoutRule = Timeout.seconds(10);

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
    execute(context, "DROP SCHEMA IF EXISTS " + schema + " CASCADE;");
    executeIgnore(context, "CREATE ROLE " + schema + " PASSWORD 'testtenant' NOSUPERUSER NOCREATEDB INHERIT LOGIN;");
    execute(context, "CREATE SCHEMA " + schema + " AUTHORIZATION " + schema);
    execute(context, "GRANT ALL PRIVILEGES ON SCHEMA " + schema + " TO " + schema);
    execute(context, "CREATE TABLE " + schema + ".user " +
        "(_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), jsonb JSONB NOT NULL);");
    execute(context, "CREATE TABLE " + schema + ".duplicateid " +
        "(_id UUID DEFAULT             gen_random_uuid(), jsonb JSONB NOT NULL);");
    execute(context, "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA " + schema + " TO " + schema);
  }

  private static void execute(TestContext context, String sql) {
    Async async = context.async();
    PostgresClient.getInstance(vertx).getClient().querySingle(sql, reply -> {
      if (reply.failed()) {
        context.fail(reply.cause());
      }
      async.complete();
    });
    async.await();
  }

  private static void executeIgnore(TestContext context, String sql) {
    Async async = context.async();
    PostgresClient.getInstance(vertx).getClient().querySingle(sql, reply -> {
      async.complete();
    });
    async.await();
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    if (ownEmbeddedPostgres) {
      PostgresClient.stopEmbeddedPostgres();
      vertx.close();
    }
  }

  private String randomUuid() {
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
   * is created that completes after nextHandler has been executed.
   */
  private Handler<AsyncResult<Response>> asyncAssertSuccess(TestContext testContext, int httpStatus,
      Handler<AsyncResult<Response>> nextHandler) {

    Async async = testContext.async();
    return newHandler -> {
      testContext.assertTrue(newHandler.succeeded(), "handler.succeeded()");
      testContext.assertEquals(httpStatus, newHandler.result().getStatus(), "http status");
      nextHandler.handle(newHandler);
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
    PgUtil.getById("user", User.class, uuid,
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

  @Test
  public void deleteByNonexistingId(TestContext testContext) {
    PgUtil.deleteById("user", randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 404, "Not found"));
  }

  @Test
  public void deleteById(TestContext testContext) {
    String uuid = randomUuid();
    post(testContext, "Ronja", uuid, 201);
    assertGetById(testContext, uuid, "Ronja");
    PgUtil.deleteById("user", uuid, okapiHeaders, vertx.getOrCreateContext(),
        Users.DeleteUsersByUserIdResponse.class, asyncAssertSuccess(testContext, 204, delete ->
          PgUtil.getById("user", User.class, uuid, okapiHeaders, vertx.getOrCreateContext(),
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
    PgUtil.deleteById("user", "invalidid", okapiHeaders, vertx.getOrCreateContext(),
        Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 500, "invalidid"));
  }

  @Test
  public void deleteByIdNonexistingTable(TestContext testContext) {
    PgUtil.deleteById("otherTable", randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        Users.DeleteUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 500, "42P01"));
  }

  @Test
  public void deleteByIdResponseWithout500(TestContext testContext) {
    PgUtil.deleteById("user", randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout500.class,
        asyncAssertFail(testContext, "respond500WithTextPlain"));
  }

  @Test
  public void deleteByIdResponseWithout204(TestContext testContext) {
    PgUtil.deleteById("user", randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout204.class,
        asyncAssertSuccess(testContext, 500, "respond204"));
  }

  @Test
  public void getByIdInvalidUuid(TestContext testContext) {
    PgUtil.getById("user", User.class, "invalidUuid", okapiHeaders, vertx.getOrCreateContext(),
        Users.GetUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 500, "22P02"));
  }

  @Test
  public void getByIdPostgresError(TestContext testContext) {
    PgUtil.getById("doesnotexist", User.class, randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        Users.GetUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 500, "doesnotexist"));
  }

  @Test
  public void getByIdNotFound(TestContext testContext) {
    PgUtil.getById("user", User.class, randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        Users.GetUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 404, "Not found"));
  }

  @Test
  public void getByIdWithout500(TestContext testContext) {
    PgUtil.getById("user", User.class, randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout500.class,
        asyncAssertFail(testContext, "respond500WithTextPlain"));
  }

  @Test
  public void getByIdWithout200(TestContext testContext) {
    PgUtil.getById("user", User.class, randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
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
    PgUtil.post("user", new User().withUsername(username).withId(uuid),
        okapiHeaders, vertx.getOrCreateContext(), ResponseImpl.class, result -> {
          returnedUuid[0] = assertStatusAndUser(testContext, result, httpStatus, username, uuid);
          async.complete();
        });
    async.await();
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
  public void postDuplicateId(TestContext testContext) {
    String uuid = randomUuid();
    post(testContext, "Anna", uuid, 201);
    PgUtil.post("user", new User().withUsername("Elsa").withId(uuid),
        okapiHeaders, vertx.getOrCreateContext(), ResponseImpl.class,
        asyncAssertSuccess(testContext, 400, "duplicate key value"));
  }

  @Test
  public void postResponseWithout500(TestContext testContext) {
    PgUtil.post("user", "string", okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout500.class,
        asyncAssertFail(testContext, "respond500WithTextPlain"));
  }

  @Test
  public void postResponseWithoutHeadersFor201Class(TestContext testContext) {
    PgUtil.post("user", "string", okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithoutHeadersFor201Class.class,
        asyncAssertSuccess(testContext, 500, "$HeadersFor201"));
  }

  @Test
  public void postResponseWithoutHeadersFor201Method(TestContext testContext) {
    PgUtil.post("user", "string", okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithoutHeadersFor201Method.class,
        asyncAssertSuccess(testContext, 500, ".headersFor201"));
  }

  @Test
  public void postResponseWithout201(TestContext testContext) {
    PgUtil.post("user", "string", okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout201.class,
        asyncAssertSuccess(testContext, 500, "respond201WithApplicationJson"));
  }

  @Test
  public void postResponseWithout400(TestContext testContext) {
    PgUtil.post("user", "string", okapiHeaders, vertx.getOrCreateContext(),
        ResponseWithout400.class,
        asyncAssertSuccess(testContext, 500, "respond400WithTextPlain"));
  }

  @Test
  public void postException(TestContext testContext) {
    PgUtil.post("user", "string", okapiHeaders, vertx.getOrCreateContext(),
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
    PgUtil.put("user", new User().withUsername("Rosamunde"), uuid,
        okapiHeaders, vertx.getOrCreateContext(),
        Users.PutUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 404, put -> {
          // make sure that a record with this uuid really hasn't been inserted
          PgUtil.getById("user", User.class, uuid, okapiHeaders, vertx.getOrCreateContext(),
              Users.GetUsersByUserIdResponse.class,
              asyncAssertSuccess(testContext, 404, "Not found"));
        }));
  }

  @Test
  public void put(TestContext testContext) {
    String uuid = randomUuid();
    post(testContext, "Pippilotta", uuid, 201);
    PgUtil.put("user", new User().withUsername("Momo").withId(randomUuid()), uuid,
        okapiHeaders, vertx.getOrCreateContext(),
        Users.PutUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 204, put -> assertGetById(testContext, uuid, "Momo")));
  }

  @Test
  public void putInvalidUuid(TestContext testContext) {
    PgUtil.put("user", new User().withUsername("Bö"), "SomeInvalidUuid",
        okapiHeaders, vertx.getOrCreateContext(),
        Users.PutUsersByUserIdResponse.class,
        asyncAssertSuccess(testContext, 400, "SomeInvalidUuid"));
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
  public void putException(TestContext testContext) {
    PgUtil.put("user", "string", randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
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
    PgUtil.put("user", new User(), randomUuid(), okapiHeaders, vertx.getOrCreateContext(),
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
    testContext.assertTrue(future.failed());
    testContext.assertTrue(future.cause() instanceof NullPointerException);
  }

  @Test
  public void responseLocation3Nulls(TestContext testContext) throws Exception {
    Method respond500 = ResponseImpl.class.getMethod("respond500WithTextPlain", Object.class);
    Future<Response> future = PgUtil.response(new User(), "localhost", null, null, null, respond500);
    testContext.assertTrue(future.succeeded());
    testContext.assertEquals(500, future.result().getStatus());
  }

  @Test
  public void responseValueWithNullsFailResponseMethod(TestContext testContext) throws Exception {
    Method respond200 = ResponseImpl.class.getMethod("respond200WithApplicationJson", User.class);
    Future<Response> future = PgUtil.response(new User(), respond200, null);
    testContext.assertTrue(future.failed());
    testContext.assertTrue(future.cause() instanceof NullPointerException);
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
    testContext.assertTrue(future.succeeded());
    Response response = future.result();
    testContext.assertEquals(500, response.getStatus());
    testContext.assertEquals("some runtime exception", response.getEntity());
  }

  @Test
  public void responseValueWithExceptionInFailResponseMethod(TestContext testContext) throws Exception {
    Method exceptionMethod = PgUtilIT.class.getMethod("exceptionMethod", Object.class);
    Future<Response> future = PgUtil.response(new User(), exceptionMethod, exceptionMethod);
    testContext.assertTrue(future.failed());
    testContext.assertEquals("some runtime exception", future.cause().getCause().getMessage());
  }

  @Test
  public void response2Nulls(TestContext testContext) throws Exception {
    Future<Response> future = PgUtil.response(null, null);
    testContext.assertTrue(future.failed());
    testContext.assertTrue(future.cause() instanceof NullPointerException);
  }

  @Test
  public void response204Null(TestContext testContext) throws Exception {
    Method respond204 = ResponseImpl.class.getMethod("respond204");
    Future<Response> future = PgUtil.response(respond204, null);
    testContext.assertTrue(future.failed());
    testContext.assertTrue(future.cause() instanceof NullPointerException);
  }

  @Test
  public void responseNull500(TestContext testContext) throws Exception {
    Method respond500 = ResponseImpl.class.getMethod("respond500WithTextPlain", Object.class);
    Future<Response> future = PgUtil.response(null, respond500);
    testContext.assertTrue(future.succeeded());
    Response response = future.result();
    testContext.assertEquals(500, response.getStatus());
    testContext.assertEquals("responseMethod must not be null", response.getEntity());
  }

  @Test
  public void responseWithExceptionInValueMethod(TestContext testContext) throws Exception {
    Method exceptionMethod = PgUtilIT.class.getMethod("exceptionMethod");
    Method respond500 = ResponseImpl.class.getMethod("respond500WithTextPlain", Object.class);
    Future<Response> future = PgUtil.response(exceptionMethod, respond500);
    testContext.assertTrue(future.succeeded());
    Response response = future.result();
    testContext.assertEquals(500, response.getStatus());
    testContext.assertEquals("some runtime exception", response.getEntity());
  }

  @Test
  public void responseWithExceptionInFailResponseMethod(TestContext testContext) throws Exception {
    Method exceptionMethod = PgUtilIT.class.getMethod("exceptionMethod");
    Method exceptionMethod2 = PgUtilIT.class.getMethod("exceptionMethod", Object.class);
    Future<Response> future = PgUtil.response(exceptionMethod, exceptionMethod2);
    testContext.assertTrue(future.failed());
    testContext.assertEquals("some runtime exception", future.cause().getCause().getMessage());
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
}
