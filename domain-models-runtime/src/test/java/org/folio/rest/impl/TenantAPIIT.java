package org.folio.rest.impl;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.folio.dbschema.Schema;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.jaxrs.model.Book;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.TenantJob;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.tools.utils.VertxUtils;
import org.hamcrest.CoreMatchers;
import org.junit.*;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import freemarker.template.TemplateException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class TenantAPIIT {
  private static final String tenantId = "folio_shared";
  protected static Vertx vertx;
  private static Map<String,String> okapiHeaders = new HashMap<>();
  private static final int TIMER_WAIT = 10000;

  @Rule
  public Timeout rule = Timeout.seconds(20);

  @BeforeClass
  public static void setUpClass() {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
  }

  @AfterClass
  public static void afterClass() {
    vertx.close();
  }


  @After
  public void after(TestContext context) {
    TenantAPI tenantAPI = new TenantAPI();
    TenantAttributes attributes = new TenantAttributes().withPurge(true);
    tenantAPI.postTenantSync(attributes, okapiHeaders, context.asyncAssertSuccess(), vertx.getOrCreateContext());
  }

  /**
   * Return a lambda that handles an AsyncResult this way: On success pass the result
   * to the handler, on failure pass the causing Throwable to testContext.
   * @param testContext - where to invoke fail(Throwable)
   * @param handler - where to inject the result
   * @return the lambda
   */
  protected static <T> Handler<AsyncResult<T>> onSuccess(TestContext testContext, Handler<T> handler) {
    return asyncResult -> {
      if (asyncResult.succeeded()) {
        handler.handle(asyncResult.result());
      } else {
        testContext.fail(asyncResult.cause());
      }
    };
  }

  public void tenantDeleteAsync(TestContext context) {
    TenantAttributes tenantAttributes = new TenantAttributes();
    tenantAttributes.setPurge(true);
    Async async = context.async();
    TenantAPI tenantAPI = new TenantAPI();
    tenantAPI.postTenant(tenantAttributes, okapiHeaders, onSuccess(context, res1 -> {
      context.assertEquals(204, res1.getStatus());
      tenantAPI.tenantExists(vertx.getOrCreateContext(), tenantId)
          .onComplete(context.asyncAssertSuccess(bool -> {
            context.assertFalse(bool, "tenant exists after purge");
            async.complete();
          }));
    }), vertx.getOrCreateContext());
    async.await();
  }

  public String tenantPost(TestContext context) {
    return tenantPost(new TenantAPI(), context, null);
  }

  public String tenantPost(TenantAPI api, TestContext context, TenantAttributes tenantAttributes) {
    Async async = context.async();
    StringBuilder id = new StringBuilder();
    api.postTenant(tenantAttributes, okapiHeaders, onSuccess(context, res1 -> {
      TenantJob job = (TenantJob) res1.getEntity();
      id.append(job.getId());
      api.getTenantByOperationId(job.getId(), TIMER_WAIT, okapiHeaders, onSuccess(context, res2 -> {
        TenantJob o = (TenantJob) res2.getEntity();
        context.assertTrue(o.getComplete());
        api.tenantExists(Vertx.currentContext(), tenantId)
            .onComplete(onSuccess(context, bool -> {
              context.assertTrue(bool, "tenant exists after post");
              async.complete();
            }));
      }), vertx.getOrCreateContext());
    }), vertx.getOrCreateContext());
    async.await();
    return id.toString();
  }

  public boolean tenantGet(TestContext context) {
    boolean [] result = new boolean [1];
    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      tenantAPI.tenantExists(Vertx.currentContext(), tenantId).onComplete(res -> {
        if (res.succeeded()) {
          result[0] = res.result();
        } else {
          context.fail(res.cause());
        }
        async.complete();
      });
    });
    async.await();
    return result[0];
  }

  /**
   * asMetadata() = '{}'
   * <p>
   * asMetadata("a", "foo", "b", "baz") = '"metadata": {"a": "foo", "b": "baz"}'
   */
  private String asMetadata(String ... keyValue) {
    if (keyValue.length == 0) {
      return "{}";
    }
    JsonObject json = new JsonObject();
    for (int i=0; i<keyValue.length; i+=2) {
      json.put(keyValue[i], keyValue[i+1]);
    }
    return "{\"metadata\": " + json.encode() + "}";
  }

  private static String table = " folio_shared_raml_module_builder.test_tenantapi ";
  /** set database client time zone to check that this doesn't interfere with UTC storage */
  private static String setTimeZone5 = "SET TIME ZONE '+05'";
  private static String setTimeZone6 = "SET TIME ZONE '+06'";

  private Book insert(TestContext context, String ... keyValue) {
    String uuid = UUID.randomUUID().toString();
    Book [] book = new Book [1];
    Async async = context.async();
    PostgresClient postgresClient = PgUtil.postgresClient(vertx.getOrCreateContext(), okapiHeaders);
    String sql = "INSERT INTO" + table + "VALUES ('" + uuid + "', '" + asMetadata(keyValue) + "')";
    postgresClient.execute(setTimeZone5, context.asyncAssertSuccess(zone5 -> {
      postgresClient.execute(sql, context.asyncAssertSuccess(i -> {
        postgresClient.execute(setTimeZone6, context.asyncAssertSuccess(zone6 -> {
          postgresClient.getById("test_tenantapi", uuid, Book.class, context.asyncAssertSuccess(g -> {
            book[0] = g;
            async.complete();
          }));
        }));
      }));
    }));
    async.await(5000 /* ms */);
    return book[0];
  }

  private Book update(TestContext context, String uuid, String ... keyValue) {
    Book [] book = new Book [1];

    Async async = context.async();
    PostgresClient postgresClient = PgUtil.postgresClient(vertx.getOrCreateContext(), okapiHeaders);
    String sql = "UPDATE" + table + "SET jsonb='" + asMetadata(keyValue) + "' WHERE id='" + uuid + "'";
    postgresClient.execute(setTimeZone5, context.asyncAssertSuccess(zone5 -> {
      postgresClient.execute(sql, context.asyncAssertSuccess(u -> {
        postgresClient.execute(setTimeZone6, context.asyncAssertSuccess(zone6 -> {
          postgresClient.getById("test_tenantapi", uuid, Book.class, context.asyncAssertSuccess(g -> {
            book[0] = g;
            async.complete();
          }));
        }));
      }));
    }));
    async.await(5000 /* ms */);
    return book[0];
  }

  private void assertDateFormat(TestContext context, String bookId) {
    Async async = context.async();
    String sql = "SELECT jsonb FROM " + table + " WHERE id ='" + bookId + "'";
    PostgresClient postgresClient = PgUtil.postgresClient(vertx.getOrCreateContext(), okapiHeaders);
    postgresClient.select(sql, context.asyncAssertSuccess(rowSet -> {
      JsonObject metadata = rowSet.iterator().next().getJsonObject(0).getJsonObject("metadata");
      String dateFormatPattern = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z";
      assertThat(metadata.getString("createdDate"), matchesRegex(dateFormatPattern));
      assertThat(metadata.getString("updatedDate"), matchesRegex(dateFormatPattern));
      async.complete();
    }));
    async.await(500 /* ms */);
  }

  private String utc(Date date) {
    if (date == null) {
      return null;
    }
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    return dateFormat.format(date);
  }

  private void assertMetadata(Book book, String createdDate, String createdByUserId, String updateDate, String updatedByUserId) {
    assertThat(utc(book.getMetadata().getCreatedDate()), is(createdDate));
    assertThat(book.getMetadata().getCreatedByUserId(), is(createdByUserId));
    assertThat(utc(book.getMetadata().getUpdatedDate()), is(updateDate));
    assertThat(book.getMetadata().getUpdatedByUserId(), is(updatedByUserId));
  }

  private void testMetadata(TestContext context) {
    String date1   = "2019-12-31T23:15:57.999";
    String date1tz = "2019-12-31T21:15:57.999-02";
    String date2 = "2020-01-16T14:15:16.777";
    String date3 = "2021-02-19T09:10:11.666";
    String date4   = "2022-04-21T19:20:21.555";
    String date4tz = "2022-04-21T22:20:21.555+03";

    Book book = insert(context, "createdDate", date1, "createdByUserId", "foo");
    assertMetadata(book, date1, "foo", date1, null);
    assertDateFormat(context, book.getId());
    book = update(context, book.getId(), "updatedDate", date2, "updatedByUserId", "bar");
    assertMetadata(book, date1, "foo", date2, "bar");
    assertDateFormat(context, book.getId());
    book = update(context, book.getId(), "updatedDate", date3, "updatedByUserId", null);
    assertMetadata(book, date1, "foo", date3, null);
    assertDateFormat(context, book.getId());
    book = update(context, book.getId(), "updatedDate", date4tz, "updatedByUserId", "bin");
    assertMetadata(book, date1, "foo", date4, "bin");
    assertDateFormat(context, book.getId());

    // RMB-320: all metadata is populated, updating with empty metadata caused trigger failure:
    // null value in column "jsonb" violates not-null constraint
    book = update(context, book.getId());
    assertThat(book.getMetadata(), is(nullValue()));

    book = insert(context, "createdDate", date1tz);
    assertMetadata(book, date1, null, date1, null);
    book = update(context, book.getId(), "createdDate", date3, "createdByUserId", "foo",
                                         "updatedDate", date2, "updatedByUserId", "baz");
    assertMetadata(book, date1, null, date2, "baz");
    book = update(context, book.getId(), "updatedDate", date4, "updatedByUserId", null);
    assertMetadata(book, date1, null, date4, null);

    book = insert(context);
    assertThat(book.getMetadata(), is(nullValue()));
    book = update(context, book.getId(), "updatedDate", date2, "updatedByUserId", "bee");
    assertMetadata(book, null, null, date2, "bee");
    book = update(context, book.getId(), "updatedDate", date3, "updatedByUserId", null);
    assertMetadata(book, null, null, date3, null);
    book = update(context, book.getId());
    assertThat(book.getMetadata(), is(nullValue()));
  }

  @Test
  public void requirePostgresSuccess(TestContext context) {
    new TenantAPI().requirePostgres(vertx.getOrCreateContext(), 99000, "9.9")
    .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void requirePostgresFail(TestContext context) {
    new TenantAPI().requirePostgres(vertx.getOrCreateContext(), 990000, "99.0")
    .onComplete(context.asyncAssertFailure(
        e -> assertThat(e.getMessage(), startsWith("Expected PostgreSQL server version 99.0 or later"))));
  }

  @Test
  public void requireCustomPostgresFail(TestContext textContext) {
    Context context = vertx.getOrCreateContext();
    context.putLocal("postgres_min_version_num", "1000000");
    context.putLocal("postgres_min_version", "100.0");
    new TenantAPI().requirePostgresVersion(context)
    .onComplete(textContext.asyncAssertFailure(
        e -> assertThat(e.getMessage(), startsWith("Expected PostgreSQL server version 100.0 or later"))));
  }

  @Test
  public void requireCustomPostgresSuccess(TestContext textContext) {
    Context context = vertx.getOrCreateContext();
    context.putLocal("postgres_min_version_num", "100000");
    context.putLocal("postgres_min_version", "10.0");
    new TenantAPI().requirePostgresVersion(context)
    .onComplete(textContext.asyncAssertSuccess());
  }

  @Test
  public void previousSchemaSqlExistsTrue(TestContext context) {
    TenantAPI tenantAPI = new TenantAPI();
    String tenantId = TenantTool.tenantId(okapiHeaders);
    tenantAPI.previousSchema(vertx.getOrCreateContext(), tenantId, true)
      .onComplete(context.asyncAssertFailure());
  }

  @Test
  public void previousSchemaSqlExistsTrue2(TestContext context) {
    TenantAPI tenantAPI = new TenantAPI();
    String tenantId = TenantTool.tenantId(okapiHeaders);
    Async async = context.async();
    // only run create.ftl .. This makes previousSchema to find nothing on 2nd invocation
    tenantAPI.sqlFile(vertx.getOrCreateContext(), tenantId, null, false)
        .compose(files -> tenantAPI.postgresClient(vertx.getOrCreateContext()).runSQLFile(files[0], true))
        .compose(x -> tenantAPI.previousSchema(vertx.getOrCreateContext(), tenantId, true)
        .onComplete(context.asyncAssertSuccess(schema -> {
          context.assertNull(schema);
          async.complete();
        })));
    async.await();
  }

  @Test
  public void postWithSqlError(TestContext context) {
    PostgresClient postgresClient = mock(PostgresClient.class);
    when(postgresClient.runSQLFile(anyString(), anyBoolean())).thenReturn(Future.failedFuture("mock returns failure"));
    TenantAPI tenantAPI = new TenantAPI() {
      @Override
      PostgresClient postgresClient(Context context) {
        return postgresClient;
      }

      @Override
      Future<Void> requirePostgresVersion(Context context) {
        return Future.succeededFuture();
      }

      @Override
      Future<Boolean> tenantExists(Context context, String tenantId) {
        return Future.succeededFuture(false);
      }
    };
    TenantAttributes tenantAttributes = new TenantAttributes();
    tenantAPI.postTenant(tenantAttributes, okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(400));
      assertThat((String) response.getEntity(), is("mock returns failure"));

    }), vertx.getOrCreateContext());
  }

  @Test
  public void postWithSqlFailure(TestContext context) {
    PostgresClient postgresClient = mock(PostgresClient.class);
    List<String> failureList = Collections.singletonList("first failure");
    when(postgresClient.runSQLFile(anyString(), anyBoolean())).thenReturn(Future.succeededFuture(failureList));
    TenantAPI tenantAPI = new TenantAPI() {
      @Override
      PostgresClient postgresClient(Context context) {
        return postgresClient;
      }

      @Override
      Future<Void> requirePostgresVersion(Context context) {
        return Future.succeededFuture();
      }

      @Override
      Future<Boolean> tenantExists(Context context, String tenantId) {
        return Future.succeededFuture(false);
      }
    };
    tenantAPI.postTenant(null, okapiHeaders, context.asyncAssertSuccess(result -> {
      assertThat(result.getStatus(), is(400));
      String error = (String) result.getEntity();

      assertThat(error, is("first failure"));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void postWithoutSchemaJson(TestContext context) {
    TenantAPI tenantAPI = new TenantAPI() {
      @Override
      String getTablePath() {
        return "does/not/exist";
      }
    };
    tenantAPI.postTenant(null, okapiHeaders, context.asyncAssertSuccess(result -> {
      assertThat(result.getStatus(), is(400));
      assertThat(((String) result.getEntity()), is("No schema.json"));
    }), vertx.getOrCreateContext());
  }

  private void postWithSqlFileException(TestContext context, TenantAPI tenantAPI, Class<? extends Exception> exceptionClass) {
    tenantAPI.postTenant(null, okapiHeaders, context.asyncAssertSuccess(result -> {
      assertThat(result.getStatus(), is(400));
      assertThat(((String) result.getEntity()),is(CoreMatchers.startsWith(exceptionClass.getName())));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void postWithSqlFileIOException(TestContext context) {
    TenantAPI tenantAPI = new TenantAPI() {
      @Override
      public String [] sqlFile(String tenantId, boolean tenantExists, TenantAttributes entity, Schema previousSchema)
          throws IOException {
        throw new IOException();
      }
    };
    postWithSqlFileException(context, tenantAPI, IOException.class);
  }

  @Test
  public void postWithSqlFileTemplateException(TestContext context) {
    TenantAPI tenantAPI = new TenantAPI() {
      @Override
      public String [] sqlFile(String tenantId, boolean tenantExists, TenantAttributes entity, Schema previousSchema)
          throws TemplateException {
        throw new TemplateException(null);
      }
    };
    postWithSqlFileException(context, tenantAPI, TemplateException.class);
  }

  @Test
  public void multi(TestContext context) {
    tenantDeleteAsync(context);  // make sure tenant does not exist
    assertThat(tenantGet(context), is(false));
    tenantPost(context);    // create tenant
    assertThat(tenantGet(context), is(true));
    tenantPost(context);    // create tenant when tenant already exists
    tenantPost(new TenantAPI(), context, new TenantAttributes());
    testMetadata(context);
    tenantDeleteAsync(context);  // delete existing tenant
    assertThat(tenantGet(context), is(false));
    tenantDeleteAsync(context);  // delete non existing tenant
    assertThat(tenantGet(context), is(false));
    tenantPost(context);    // create tenant
    assertThat(tenantGet(context), is(true));
    String sql = "SELECT count(*) FROM " + PostgresClient.convertToPsqlStandard(tenantId) + ".test_tenantapi";
    PostgresClient.getInstance(vertx, tenantId).selectSingle(sql, context.asyncAssertSuccess(result -> {
      assertThat(result.getInteger(0), is(0));
    }));
  }

  @Test
  public void invalidTenantName(TestContext context) {
    String invalidTenantId = "- ";
    vertx.runOnContext(run -> {
      new TenantAPI().tenantExists(Vertx.currentContext(), invalidTenantId).onComplete(
          context.asyncAssertSuccess(bool -> context.assertFalse(bool)));
    });
  }

  @Test
  public void getTenantByOperationIdNotFound(TestContext context) {
    tenantPost(context);    // create tenant

    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      String unknownId = UUID.randomUUID().toString();
      tenantAPI.getTenantByOperationId(unknownId, 0, okapiHeaders, onSuccess(context, result -> {
        assertThat(result.getStatus(), is(404));
        assertThat((String) result.getEntity(), is("Job not found " + unknownId));
        async.complete();
      }), Vertx.currentContext());
    });
    async.await();
  }

  @Test
  public void getTenantByOperationIdInvalidUUID(TestContext context) {
    tenantPost(context);    // create tenant

    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      String invalidUUID = "1234";
      tenantAPI.getTenantByOperationId(invalidUUID, 0, okapiHeaders, onSuccess(context, result -> {
        assertThat(result.getStatus(), is(400));
        assertThat((String) result.getEntity(), is("Invalid UUID string: " + invalidUUID));
        async.complete();
      }), Vertx.currentContext());
    });
    async.await();
  }

  @Test
  public void getTenantByOperationTenantNotFound(TestContext context) {
    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      tenantAPI.getTenantByOperationId(UUID.randomUUID().toString(), 0, okapiHeaders, onSuccess(context, result -> {
        assertThat(result.getStatus(), is(404));
        assertThat((String) result.getEntity(), is("Tenant not found folio_shared"));
        async.complete();
      }), Vertx.currentContext());
    });
    async.await();
  }

  @Test
  public void getTenantByOperationIdFound(TestContext context) {
    String id = tenantPost(context);    // create tenant

    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      tenantAPI.getTenantByOperationId(id, 0, okapiHeaders, onSuccess(context, result -> {
        assertThat(result.getStatus(), is(200));
        async.complete();
      }), Vertx.currentContext());
    });
    async.awaitSuccess();
  }

  @Test
  public void deleteTenantByOperationIdNotFound(TestContext context) {
    tenantPost(context);    // create tenant

    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      String unknownId = UUID.randomUUID().toString();
      tenantAPI.deleteTenantByOperationId(unknownId, okapiHeaders, onSuccess(context, result -> {
        assertThat(result.getStatus(), is(404));
        String msg = (String) result.getEntity();
        assertThat(msg, is("Job not found " + unknownId));
        async.complete();
      }), Vertx.currentContext());
    });
    async.await();
  }

  @Test
  public void deleteTenantByOperationIdOK(TestContext context) {
    String id = tenantPost(context);    // create tenant
    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      tenantAPI.deleteTenantByOperationId(id, okapiHeaders, onSuccess(context, result -> {
        assertThat(result.getStatus(), is(204));
        async.complete();
      }), Vertx.currentContext());
    });
    async.await();
  }

  @Test
  public void deleteTenantByOperationIdTenantNotFound(TestContext context) {
    tenantDeleteAsync(context);
    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      tenantAPI.deleteTenantByOperationId(UUID.randomUUID().toString(), okapiHeaders, onSuccess(context, result -> {
        assertThat(result.getStatus(), is(404));
        String msg = (String) result.getEntity();
        assertThat(msg, is("Tenant not found folio_shared"));
        async.complete();
      }), Vertx.currentContext());
    });
    async.await();
  }

  @Test
  public void deleteTenantByOperationIdInvalidUUID(TestContext context) {
    Async async = context.async();
      TenantAPI tenantAPI = new TenantAPI();
      String invalidUUID = "1234";
      tenantAPI.deleteTenantByOperationId(invalidUUID, okapiHeaders, onSuccess(context, result -> {
        assertThat(result.getStatus(), is(400));
        String msg = (String) result.getEntity();
        assertThat(msg, is("Invalid UUID string: " + invalidUUID));
        async.complete();
      }), Vertx.currentContext());
    async.await();
  }

  @Test
  public void postTenantWithLoadFailSync(TestContext context) {
    TenantAPI tenantAPI = new TenantAPI() {
      @Override
      Future<Integer> loadData(TenantAttributes attributes, String tenantId, Map<String, String> headers, Context ctx) {
        return Future.failedFuture("Load Failure");
      }
    };
    tenantAPI.postTenantSync(new TenantAttributes(), okapiHeaders, context.asyncAssertSuccess(result -> {
      assertThat(result.getStatus(), is(400));
      String msg = (String) result.getEntity();
      assertThat(msg, is("Load Failure"));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void postTenantWithLoadFailAsync(TestContext context) {
    TenantAPI tenantAPI = new TenantAPI() {
      @Override
      Future<Integer> loadData(TenantAttributes attributes, String tenantId, Map<String, String> headers, Context ctx) {
        return Future.failedFuture("Load Failure");
      }
    };
    String id = tenantPost(tenantAPI, context, new TenantAttributes());
    tenantAPI.getTenantByOperationId(id, 0, okapiHeaders, onSuccess(context, result -> {
      assertThat(result.getStatus(), is(200));
      TenantJob job = (TenantJob) result.getEntity();
      assertThat(job.getComplete(), is(true));
      assertThat(job.getError(), is("Load Failure"));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void postTenantWithSqlErrorAsync(TestContext context) {
    TenantAPI tenantAPI = new TenantAPI();
    TenantJob job = new TenantJob();
    tenantAPI.runAsync(null, "SELECT (", job, okapiHeaders, vertx.getOrCreateContext())
        .onComplete(context.asyncAssertFailure(cause -> {
          assertThat(job.getError(), is("SQL error"));
          // for some bizarre reason a space is put in front the returned stmt
          assertThat(job.getMessages(), containsInAnyOrder(CoreMatchers.startsWith(" SELECT (\n")));
        }));
  }

  @Test
  public void postTenantEnableSync(TestContext context) {
    TenantAPI tenantAPI = new TenantAPI();
    TenantAttributes tenantAttributes = new TenantAttributes();
    tenantAttributes.setModuleFrom("mod-0.0.0");
    tenantAttributes.setModuleTo("mod-1.0.0");
    tenantAPI.postTenantSync(tenantAttributes, okapiHeaders, context.asyncAssertSuccess(result -> {
      assertThat(result.getStatus(), is(204));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void postTenantDisable(TestContext context) {
    TenantAPI tenantAPI = new TenantAPI();
    TenantAttributes tenantAttributes = new TenantAttributes();
    tenantAttributes.setModuleFrom("mod-0.0.0");
    tenantAPI.postTenant(tenantAttributes, okapiHeaders, context.asyncAssertSuccess(result -> {
      assertThat(result.getStatus(), is(204));
    }), vertx.getOrCreateContext());
  }
}
