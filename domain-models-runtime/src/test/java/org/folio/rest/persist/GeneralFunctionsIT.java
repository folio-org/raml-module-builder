package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import java.util.UUID;

/**
 * Test src/main/resources/templates/db_scripts/general_functions.ftl
 */
@RunWith(VertxUnitRunner.class)
public class GeneralFunctionsIT extends PostgresClientITBase {

  private Future<Row> upsert(TestContext context, UUID id, JsonObject json) {
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenant);
    String sql = "SELECT upsert('t', $1, $2)";
    return postgresClient.selectSingle(sql, Tuple.of(id, json))
    .onComplete(context.asyncAssertSuccess(upsert -> {
      assertThat(sql, upsert.getUUID(0), is(id));
      postgresClient.getById("t", id.toString(), context.asyncAssertSuccess(get -> {
        assertThat(get, is(json));
      }));
    }));
  }

  @Test
  public void upsert(TestContext context) {
    execute(context, "CREATE TABLE " + schema + ".t (id uuid primary key, jsonb jsonb);");
    UUID id = UUID.randomUUID();
    upsert(context, id, new JsonObject().put("a", 1))
    .compose(x -> upsert(context, id, new JsonObject().put("b", 2)));
  }

  void normalizeDigits(TestContext context, String raw, String expected) {
    String sql = "SELECT normalize_digits('" + raw + "')";
    PostgresClient.getInstance(vertx, tenant).selectSingle(sql, context.asyncAssertSuccess(result -> {
      assertThat(sql, result.getString(0), is(expected));
    }));
  }

  @Test
  public void normalizeDigits(TestContext context) {
    String [][] pairList = {
        { "9789280115659",               "9789280115659" },
        { "978928011565*",               "978928011565*" },
        { "9789280115659(pbk.)",         "9789280115659 (pbk.)" },
        { "978928011565*(pbk.)",         "978928011565* (pbk.)" },
        { "9789280115659 (pbk.)",        "9789280115659 (pbk.)" },
        { "978928011565* (pbk.)",        "978928011565* (pbk.)" },
        { "978 92 8011 565 9",           "9789280115659" },
        { "-978 92-8011 565-9 ",         "9789280115659" },
        { " 978-92 8011-565 9-",         "9789280115659" },
        { "\t978-92\t8011-565\t\t9- ",   "9789280115659" },
        { "9280115650 (Vol. 1011-1021)", "9280115650 (Vol. 1011-1021)" },
        { "Vol. 1011-1021",              "Vol. 1011-1021" },
    };
    for (String [] pair : pairList) {
      normalizeDigits(context, pair[0], pair[1]);
    }
  }
}
