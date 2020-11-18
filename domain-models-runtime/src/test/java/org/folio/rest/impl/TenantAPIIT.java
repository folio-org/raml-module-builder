package org.folio.rest.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.folio.dbschema.Schema;
import org.folio.rest.jaxrs.model.Book;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.resource.Tenant;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.VertxUtils;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import de.flapdoodle.embed.process.collections.Collections;
import freemarker.template.TemplateException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class TenantAPIIT {
  private static final String tenantId = "folio_shared";
  protected static Vertx vertx;
  private static Map<String,String> okapiHeaders = new HashMap<>();

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4j2LogDelegateFactory");
    okapiHeaders.put("TenantId", tenantId);
  }

  @Rule
  public Timeout rule = Timeout.seconds(20);

  @BeforeClass
  public static void setUpClass() {
    vertx = VertxUtils.getVertxWithExceptionHandler();
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
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

  public void tenantDelete(TestContext context) {
    TenantAttributes tenantAttributes = new TenantAttributes();
    tenantAttributes.setPurge(true);
    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      tenantAPI.postTenantSync(tenantAttributes, okapiHeaders, onSuccess(context, res1 -> {
        JsonObject operation = (JsonObject) res1.getEntity();
        tenantAPI.getTenantByOperationId(operation.getString("id"), okapiHeaders, onSuccess(context, res2 -> {
          tenantAPI.tenantExists(Vertx.currentContext(), tenantId, onSuccess(context, bool -> {
            context.assertFalse(bool, "tenant exists after purge");
            async.complete();
          }));
        }), Vertx.currentContext());
      }), Vertx.currentContext());
    });
    async.await();
  }

  public String tenantPost(TestContext context) {
    return tenantPost(context, null);
  }

  public String tenantPost(TestContext context, TenantAttributes tenantAttributes) {
    Async async = context.async();
    StringBuilder id = new StringBuilder();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();

      tenantAPI.postTenant(tenantAttributes, null, okapiHeaders, onSuccess(context, res1 -> {
        JsonObject operation = (JsonObject) res1.getEntity();
        id.append(operation.getString("id"));
        tenantAPI.getTenantByOperationId(id.toString(), okapiHeaders, onSuccess(context, res2 -> {
          tenantAPI.tenantExists(Vertx.currentContext(), tenantId, onSuccess(context, bool -> {
            context.assertTrue(bool, "tenant exists after post");
            async.complete();
          }));
        }), Vertx.currentContext());
      }), Vertx.currentContext());
    });
    async.await();
    return id.toString();
  }

  public boolean tenantGet(TestContext context) {
    boolean [] result = new boolean [1];
    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      tenantAPI.tenantExists(Vertx.currentContext(), tenantId, res -> {
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
    assertMetadata(book, date1, "foo", null, null);
    book = update(context, book.getId(), "updatedDate", date2, "updatedByUserId", "bar");
    assertMetadata(book, date1, "foo", date2, "bar");
    book = update(context, book.getId(), "updatedDate", date3, "updatedByUserId", null);
    assertMetadata(book, date1, "foo", date3, null);
    book = update(context, book.getId(), "updatedDate", date4tz, "updatedByUserId", "bin");
    assertMetadata(book, date1, "foo", date4, "bin");

    // RMB-320: all metadata is populated, updating with empty metadata caused trigger failure:
    // null value in column "jsonb" violates not-null constraint
    book = update(context, book.getId());
    assertThat(book.getMetadata(), is(nullValue()));

    book = insert(context, "createdDate", date1tz);
    assertMetadata(book, date1, null, null, null);
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
  public void postWithSqlError(TestContext context) {
    PostgresClient postgresClient = mock(PostgresClient.class);
    when(postgresClient.runSQLFile(anyString(), anyBoolean())).thenReturn(Future.failedFuture("mock returns failure"));
    TenantAPI tenantAPI = new TenantAPI() {
      @Override
      PostgresClient postgresClient(Context context) {
        return postgresClient;
      }

      @Override
      void tenantExists(Context context, String tenantId, Handler<AsyncResult<Boolean>> handler) {
        handler.handle(Future.succeededFuture(false));
      }
    };
    TenantAttributes tenantAttributes = new TenantAttributes();
    tenantAPI.postTenantSync(tenantAttributes, okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(201));
      assertThat(((JsonObject) response.getEntity()).getString("error"), is("mock returns failure"));

    }), vertx.getOrCreateContext());
  }

  @Test
  public void postWithSqlFailure(TestContext context) {
    PostgresClient postgresClient = mock(PostgresClient.class);
    List<String> failureList = Collections.newArrayList("first failure");
    when(postgresClient.runSQLFile(anyString(), anyBoolean())).thenReturn(Future.succeededFuture(failureList));
    TenantAPI tenantAPI = new TenantAPI() {
      @Override
      PostgresClient postgresClient(Context context) {
        return postgresClient;
      }

      @Override
      void tenantExists(Context context, String tenantId, Handler<AsyncResult<Boolean>> handler) {
        handler.handle(Future.succeededFuture(false));
      }
    };
    tenantAPI.postTenantSync(null, okapiHeaders, context.asyncAssertSuccess(result -> {
      assertThat(result.getStatus(), is(201));
      JsonObject operation = (JsonObject) result.getEntity();

      assertThat(operation.getString("error"), is("SQL error"));
      assertThat(operation.getJsonArray("messages").encode(), is("[\"first failure\"]"));
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
    tenantAPI.postTenantSync(null, okapiHeaders, context.asyncAssertSuccess(result -> {
      assertThat(result.getStatus(), is(201));
      assertThat(((JsonObject) result.getEntity()).getString("error"), is("No schema.json"));
    }), vertx.getOrCreateContext());
  }

  private void postWithSqlFileException(TestContext context, Class<? extends Exception> exceptionClass) {
    TenantAPI tenantAPI = new TenantAPI() {
      @Override
      public String sqlFile(String tenantId, boolean tenantExists, TenantAttributes entity, Schema previousSchema)
          throws IOException, TemplateException {
        switch (exceptionClass.getName()) {
        case "java.io.IOException": throw new IOException();
        default: throw new TemplateException(null);
        }
      }
    };
    tenantAPI.postTenantSync(null, okapiHeaders, context.asyncAssertSuccess(result -> {
      assertThat(result.getStatus(), is(201));
      assertThat(((JsonObject) result.getEntity()).getString("error"),is(CoreMatchers.startsWith(exceptionClass.getName())));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void postWithSqlFileIOException(TestContext context) {
    postWithSqlFileException(context, IOException.class);
  }

  @Test
  public void postWithSqlFileTemplateException(TestContext context) {
    postWithSqlFileException(context, TemplateException.class);
  }

  @Test
  public void multi(TestContext context) {
    tenantDelete(context);  // make sure tenant does not exist
    assertThat(tenantGet(context), is(false));
    tenantPost(context);    // create tenant
    assertThat(tenantGet(context), is(true));
    tenantPost(context);    // create tenant when tenant already exists
    tenantPost(context, new TenantAttributes());
    testMetadata(context);
    tenantDelete(context);  // delete existing tenant
    assertThat(tenantGet(context), is(false));
    tenantDelete(context);  // delete non existing tenant
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
    Async async = context.async();
    vertx.runOnContext(run -> {
      new TenantAPI().tenantExists(Vertx.currentContext(), invalidTenantId, h -> {
        context.assertTrue(h.succeeded());
        context.assertFalse(h.result());
        async.complete();
      });
    });
  }

  @Test
  public void getTenantByOperationIdNotFound(TestContext context) {
    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      tenantAPI.getTenantByOperationId("1234", okapiHeaders, onSuccess(context, result -> {
        assertThat(result.getStatus(), is(404));
        async.complete();
      }), Vertx.currentContext());
    });
    async.await();
  }

  @Test
  public void deleteTenantByOperationIdNotFound(TestContext context) {
    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      tenantAPI.deleteTenantByOperationId("1234", okapiHeaders, onSuccess(context, result -> {
        assertThat(result.getStatus(), is(404));
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
      tenantAPI.getTenantByOperationId(id, okapiHeaders, onSuccess(context, result -> {
        assertThat(result.getStatus(), is(200));
        async.complete();
      }), Vertx.currentContext());
    });
    async.awaitSuccess();
  }

  @Test
  public void deleteTenantByOperationIdFound(TestContext context) {
    String id = tenantPost(context);    // create tenant

    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      tenantAPI.deleteTenantByOperationId(id, okapiHeaders, onSuccess(context, result -> {
        assertThat(result.getStatus(), is(204));
        async.complete();
      }), Vertx.currentContext());
    });
    async.awaitSuccess();
  }

}
