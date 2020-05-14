package org.folio.rest.persist.ddlgen;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.PostgresClientITBase;
import org.folio.util.ResourceUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class UuidIT extends PostgresClientITBase {
  /** table name */
  private static final String t = schema + ".t";
  private static final long timeoutMillis = 10000; /* 10 seconds */

  @BeforeClass
  public static void uuidFtl(TestContext context) {
    String uuidFtl = ResourceUtil.asString("templates/db_scripts/uuid.ftl");
    uuidFtl = uuidFtl.replace("${myuniversity}_${mymodule}", schema);
    runSqlFileAsSuperuser(context, uuidFtl);
  }

  /**
   * @return PostgresClient for superuser
   */
  private static PostgresClient getClient() {
    return PostgresClient.getInstance(vertx);
  }

  private void assertMinMax(TestContext context, String expectedMin, String expectedMax) {
    Async async = context.async();
    String sql = "SELECT " + schema + ".min(uuid), " + schema + ".max(uuid) FROM " + t;
    getClient().selectSingle(sql, context.asyncAssertSuccess(result -> {
      context.assertEquals(expectedMin, result.getUUID(0) != null ? result.getUUID(0).toString() : null, "min");
      context.assertEquals(expectedMax, result.getUUID(1) != null ? result.getUUID(1).toString() : null, "max");
      async.complete();
    }));
    async.await(timeoutMillis);
  }

  @Test
  public void minMax(TestContext context) {
    executeSuperuser(context, "DROP TABLE IF EXISTS " + t);
    executeSuperuser(context, "CREATE TABLE " + t + " (uuid UUID)");
    assertMinMax(context, null, null);
    executeSuperuser(context, "INSERT INTO " + t + " VALUES (null)");
    assertMinMax(context, null, null);
    executeSuperuser(context, "INSERT INTO " + t + " VALUES (null)");
    assertMinMax(context, null, null);
    executeSuperuser(context, "INSERT INTO " + t + " VALUES ('ffffffff-4321-4321-8765-1234567890ab')");
    assertMinMax(context, "ffffffff-4321-4321-8765-1234567890ab", "ffffffff-4321-4321-8765-1234567890ab");
    executeSuperuser(context, "INSERT INTO " + t + " VALUES ('01234567-4321-4321-8765-1234567890ab')");
    assertMinMax(context, "01234567-4321-4321-8765-1234567890ab", "ffffffff-4321-4321-8765-1234567890ab");
    executeSuperuser(context, "INSERT INTO " + t + " VALUES ('00000000-0000-0000-0000-000000000000')");
    assertMinMax(context, "00000000-0000-0000-0000-000000000000", "ffffffff-4321-4321-8765-1234567890ab");
    executeSuperuser(context, "INSERT INTO " + t + " VALUES ('ffffffff-ffff-ffff-ffff-ffffffffffff')");
    assertMinMax(context, "00000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff");
    executeSuperuser(context, "INSERT INTO " + t + " VALUES (null)");
    assertMinMax(context, "00000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff");
  }

  @Test
  public void minMaxUsingIndex(TestContext context) {
    executeSuperuser(context, "DROP TABLE IF EXISTS " + t);
    executeSuperuser(context, "CREATE TABLE " + t + " (uuid UUID PRIMARY KEY)");
    executeSuperuser(context, "INSERT INTO " + t + " VALUES (md5(generate_series(1, 1000)::text)::uuid)");
    getClient().select("EXPLAIN SELECT " + schema + ".min(uuid) FROM " + t, context.asyncAssertSuccess(result -> {
      RowIterator<Row> iterator = result.iterator();
      String explain = "";
      while (iterator.hasNext()) {
        Row row = iterator.next();
        context.assertEquals(1, row.size());
        explain = explain + row.getString(0);
      }
      context.assertTrue(explain.contains("Index Only Scan using t_pkey on t"), explain);
    }));
    getClient().select("EXPLAIN SELECT " + schema + ".max(uuid) FROM " + t, context.asyncAssertSuccess(result -> {
      RowIterator<Row> iterator = result.iterator();
      String explain = "";
      while (iterator.hasNext()) {
        Row row = iterator.next();
        context.assertEquals(1, row.size());
        explain = explain + row.getString(0);
      }
      context.assertTrue(explain.contains("Index Only Scan Backward using t_pkey on t"), explain);
    }));
  }

  private void nextUuid(TestContext context, String uuid, String nextUuid) {
    getClient().selectSingle("SELECT " + schema + ".next_uuid('" + uuid +  "')", context.asyncAssertSuccess(result -> {
      context.assertEquals(nextUuid, result.getUUID(0).toString(), "next_uuid(" + uuid + ") = " + nextUuid);
    }));
  }

  @Test
  public void nextUuid(TestContext context) {
    nextUuid(context, "00000000-0000-0000-0000-000000000000", "00000000-0000-0000-0000-000000000001");
    nextUuid(context, "ffffffff-ffff-ffff-ffff-fffffffffffe", "ffffffff-ffff-ffff-ffff-ffffffffffff");
    nextUuid(context, "ffffffff-ffff-ffff-ffff-ffffffffffff", "00000000-0000-f000-f000-000000000000");
    nextUuid(context, "01234567-89ab-cde0-8fff-ffffffffffff", "01234567-89ab-cde1-8000-000000000000");
    nextUuid(context, "01234567-89ab-cde1-8fff-ffffffffffff", "01234567-89ab-cde2-8000-000000000000");
    nextUuid(context, "01234567-89ab-cde2-8fff-ffffffffffff", "01234567-89ab-cde3-8000-000000000000");
    nextUuid(context, "01234567-89ab-cde3-8fff-ffffffffffff", "01234567-89ab-cde4-8000-000000000000");
    nextUuid(context, "01234567-89ab-cde4-8fff-ffffffffffff", "01234567-89ab-cde5-8000-000000000000");
    nextUuid(context, "01234567-89ab-cde5-8fff-ffffffffffff", "01234567-89ab-cde6-8000-000000000000");
    nextUuid(context, "01234567-89ab-cde6-8fff-ffffffffffff", "01234567-89ab-cde7-8000-000000000000");
    nextUuid(context, "01234567-89ab-cde7-8fff-ffffffffffff", "01234567-89ab-cde8-8000-000000000000");
    nextUuid(context, "01234567-89ab-cde8-8fff-ffffffffffff", "01234567-89ab-cde9-8000-000000000000");
    nextUuid(context, "01234567-89ab-cde9-8fff-ffffffffffff", "01234567-89ab-cdea-8000-000000000000");
    nextUuid(context, "01234567-89ab-cdea-8fff-ffffffffffff", "01234567-89ab-cdeb-8000-000000000000");
    nextUuid(context, "01234567-89ab-cdeb-8fff-ffffffffffff", "01234567-89ab-cdec-8000-000000000000");
    nextUuid(context, "01234567-89ab-cdec-8fff-ffffffffffff", "01234567-89ab-cded-8000-000000000000");
    nextUuid(context, "01234567-89ab-cded-8fff-ffffffffffff", "01234567-89ab-cdee-8000-000000000000");
    nextUuid(context, "01234567-89ab-cdee-8fff-ffffffffffff", "01234567-89ab-cdef-8000-000000000000");
    nextUuid(context, "01234567-89ab-cdef-8fff-ffffffffffff", "01234567-89ab-cdf0-8000-000000000000");
  }
}
