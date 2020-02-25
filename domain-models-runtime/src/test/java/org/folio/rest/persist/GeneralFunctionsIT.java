package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Test src/main/resources/templates/db_scripts/general_functions.ftl
 */
@RunWith(VertxUnitRunner.class)
public class GeneralFunctionsIT extends PostgresClientITBase {

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
        { "9789280115659(pbk.)",         "9789280115659 (pbk.)" },
        { "9789280115659 (pbk.)",        "9789280115659 (pbk.)" },
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
