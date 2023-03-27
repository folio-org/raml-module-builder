package org.folio.rest.impl;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.folio.HttpStatus;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class AdminAPIIT {
  private static final String tenantId = "folio_shared";
  protected static Vertx vertx;
  private static Map<String,String> okapiHeaders = new HashMap<>();

  @Rule
  public Timeout rule = Timeout.seconds(20);

  @BeforeClass
  public static void setUpClass() {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  private String body(Response response) {
    if (response.getEntity() instanceof String) {
      return response.getEntity().toString();
    }
    OutStream outStream = (OutStream) response.getEntity();
    return outStream.getData().toString();
  }

  @Test
  public void getAdminJstack(TestContext context) {
    new AdminAPI().getAdminJstack(okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      assertThat(response.getMediaType(), is(MediaType.TEXT_HTML_TYPE));
      assertThat(body(response), containsString(" java.lang.Thread.State: "));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void getAdminMemory(TestContext context) {
    new AdminAPI().getAdminMemory(true, okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      assertThat(response.getMediaType(), is(MediaType.TEXT_HTML_TYPE));
      assertThat(body(response), containsString("memory usage after latest gc"));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void getAdminPostgresActiveSessions(TestContext context) {
    new AdminAPI().getAdminPostgresActiveSessions("postgres", okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      assertThat(response.getMediaType(), is(MediaType.APPLICATION_JSON_TYPE));
      JsonArray jsonArray = new JsonArray(body(response));
      JsonObject jsonObject = jsonArray.getJsonObject(0);
      assertThat(jsonObject.getInteger("pid"), is(greaterThan(-1)));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void getAdminPostgresLoad(TestContext context) {
    new AdminAPI().getAdminPostgresLoad("postgres", okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      assertThat(response.getMediaType(), is(MediaType.APPLICATION_JSON_TYPE));
      JsonArray jsonArray = new JsonArray(body(response));
      JsonObject jsonObject = jsonArray.getJsonObject(0);
      assertThat(jsonObject.getInteger("connections"), is(greaterThan(-1)));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void getAdminPostgresTableAccessStats(TestContext context) {
    new AdminAPI().getAdminPostgresTableAccessStats(okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      assertThat(response.getMediaType(), is(MediaType.APPLICATION_JSON_TYPE));
      JsonArray jsonArray = new JsonArray(body(response));
      // there is no easy way to fill pg_stat_user_tables with non-zero data
      assertThat(jsonArray, is(notNullValue()));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void getAdminPostgresTableSize(TestContext context) {
    new AdminAPI().getAdminPostgresTableSize("postgres", okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      assertThat(response.getMediaType(), is(MediaType.APPLICATION_JSON_TYPE));
      JsonArray jsonArray = new JsonArray(body(response));
      assertThat(jsonArray, is(notNullValue()));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void getAdminTableIndexUsage(TestContext context) {
    new AdminAPI().getAdminTableIndexUsage(okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      assertThat(response.getMediaType(), is(MediaType.APPLICATION_JSON_TYPE));
      JsonArray jsonArray = new JsonArray(body(response));
      // there is no easy way to fill pg_stat_user_tables with non-zero data
      assertThat(jsonArray, is(notNullValue()));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void getAdminCacheHitRates(TestContext context) {
    new AdminAPI().getAdminCacheHitRates(okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      assertThat(response.getMediaType(), is(MediaType.APPLICATION_JSON_TYPE));
      JsonArray jsonArray = new JsonArray(body(response));
      assertThat(jsonArray, is(notNullValue()));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void getAdminSlowQueries(TestContext context) {
    new AdminAPI().getAdminSlowQueries(0, okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      assertThat(response.getMediaType(), is(MediaType.APPLICATION_JSON_TYPE));
      JsonArray jsonArray = new JsonArray(body(response));
      assertThat(jsonArray, is(notNullValue()));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void getAdminHealth(TestContext context) {
    new AdminAPI().getAdminHealth(okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      assertThat(body(response), is("OK"));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void getAdminTotalDbSize(TestContext context) {
    new AdminAPI().getAdminTotalDbSize("postgres", okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      assertThat(response.getMediaType(), is(MediaType.APPLICATION_JSON_TYPE));
      JsonArray jsonArray = new JsonArray(body(response));
      JsonObject jsonObject = jsonArray.getJsonObject(0);
      assertThat(jsonObject.getString("db_size"), is(notNullValue()));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void canGetAdminDbCacheSummary(TestContext context) {
    new AdminAPI().getAdminDbCacheSummary(okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      assertThat(response.getMediaType(), is(MediaType.APPLICATION_JSON_TYPE));
      JsonArray jsonArray = new JsonArray(body(response));
      JsonObject jsonObject = jsonArray.getJsonObject(0);
      assertThat(jsonObject.getString("relname"), is(notNullValue()));
      // Each value has always been a String, this includes a numbers
      // buffers_percent rounded to 1 decimal
      assertThat(jsonObject.getDouble("buffers_percent").toString(), matchesRegex("^\\d*\\.\\d$"));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void getAdminListLockingQueries(TestContext context) {
    new AdminAPI().getAdminListLockingQueries("postgres", okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
      assertThat(response.getMediaType(), is(MediaType.APPLICATION_JSON_TYPE));
      JsonArray jsonArray = new JsonArray(body(response));
      assertThat(jsonArray, is(notNullValue()));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void deleteAdminKillQuery(TestContext context) {
    new AdminAPI().deleteAdminKillQuery("99999999", okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_NOT_FOUND.toInt()));
      assertThat(response.getMediaType(), is(MediaType.TEXT_PLAIN_TYPE));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void postAdminImportSql(TestContext context) {
    var stream = new ByteArrayInputStream("SELECT 1".getBytes(StandardCharsets.UTF_8));
    new AdminAPI().postAdminImportSQL(stream, okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_OK.toInt()));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void postAdminImportSqlError(TestContext context) {
    var sql = "DO $$ BEGIN RAISE EXCEPTION 'spaghetti'; END $$";
    var stream = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8));
    new AdminAPI().postAdminImportSQL(stream, okapiHeaders, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_BAD_REQUEST.toInt()));
      assertThat(response.getEntity().toString(), containsString("ERROR: spaghetti"));
    }), vertx.getOrCreateContext());
  }

  @Test
  public void postAdminImportSqlException(TestContext context) {
    new AdminAPI().postAdminImportSQL(null, null, context.asyncAssertSuccess(response -> {
      assertThat(response.getStatus(), is(HttpStatus.HTTP_INTERNAL_SERVER_ERROR.toInt()));
    }), null);
  }

  @Test
  public void putAdminPostgresCreateIndexesFailure(TestContext context) {
    Map<String,String> headers = Map.of("X-Okapi-Tenant", "nonexistingcreateindexestenant");
    new AdminAPI().putAdminPostgresCreateIndexes(headers, context.asyncAssertSuccess(response ->
      assertThat(response.getStatus(), is(HttpStatus.HTTP_BAD_REQUEST.toInt()))
    ), vertx.getOrCreateContext());
  }

  @Test
  public void putAdminPostgresCreateIndexesException(TestContext context) {
    new AdminAPI().putAdminPostgresCreateIndexes(null, context.asyncAssertSuccess(response ->
      assertThat(response.getStatus(), is(HttpStatus.HTTP_INTERNAL_SERVER_ERROR.toInt()))
    ), null);
  }
}
