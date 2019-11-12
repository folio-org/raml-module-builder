package org.folio.rest.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.folio.rest.jaxrs.model.Book;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
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
    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      try {
        tenantAPI.deleteTenant(okapiHeaders, h -> {
          tenantAPI.tenantExists(Vertx.currentContext(), tenantId, onSuccess(context, bool -> {
            context.assertFalse(bool, "tenant exists during delete");
            async.complete();
          }));
        }, Vertx.currentContext());
      } catch (Exception e) {
        context.fail(e);
      }
    });
    async.awaitSuccess();
  }

  public void tenantPost(TestContext context) {
    tenantPost(context, null);
  }

  public void tenantPost(TestContext context, TenantAttributes tenantAttributes) {
    Async async = context.async();
    vertx.runOnContext(run -> {
      TenantAPI tenantAPI = new TenantAPI();
      try {
        tenantAPI.postTenant(tenantAttributes, okapiHeaders, h -> {
          tenantAPI.tenantExists(Vertx.currentContext(), tenantId, onSuccess(context, bool -> {
            context.assertTrue(bool, "tenant exists after post");
            async.complete();
          }));
        }, Vertx.currentContext());
      } catch (Exception e) {
        context.fail(e);
      }
    });
    async.awaitSuccess();
  }

  public boolean tenantGet(TestContext context) {
    boolean [] result = new boolean [1];
    Async async = context.async();
    vertx.runOnContext(run -> {
      try {
        TenantAPI tenantAPI = new TenantAPI();
        tenantAPI.getTenant(okapiHeaders, context.asyncAssertSuccess(response -> {
          result[0] = "true".equals(response.getEntity());
          async.complete();
        }), Vertx.currentContext());
      } catch (Exception e) {
        context.fail(e);
      }
    });
    async.awaitSuccess();
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
}
