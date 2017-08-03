package org.folio.rest.persist;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.Arrays;
import java.util.stream.Collectors;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;

import org.folio.rest.tools.utils.VertxUtils;
import org.junit.Test;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class RunSQLIT {
  @Parameter(0)
  public int expectedResultSize;
  @Parameter(1)
  public String sql;

  @Parameterized.Parameters(name = "{0} {1}")
  static public Iterable<Object []> data() {
    return Arrays.asList(new Object [][] {
      { 0, "update pg_database set datname=null where false" },
      { 0, "update pg_database set datname=null where false;" },
      { 0, "update pg_database set datname=null where false\n" },
      { 1, "syntaxerror" },
      { 1, "syntaxerror;" },
      { 1, "syntaxerror\n" },
      { 2, "syntaxerror1;\nsyntaxerror2" },
      { 2, "syntaxerror1;\nsyntaxerror2;" },
      { 2, "syntaxerror1;\nsyntaxerror2\n" }
    });
  }

  @Test
  public void sql(TestContext context) {
    Async async = context.async();
    Vertx vertx = VertxUtils.getVertxFromContextOrNew();
    PostgresClient client = PostgresClient.getInstance(vertx);
    client.runSQLFile(sql, false, replyHandler -> {
      context.assertTrue(replyHandler.succeeded(), "runSQL succeed status");
      int resultSize = replyHandler.result().size();
      if (resultSize != expectedResultSize) {
        String results = replyHandler.result().stream().collect(Collectors.joining("\n"));
        context.assertEquals(expectedResultSize, resultSize,
              "runSQL result.size()=" + resultSize
            + " [" + results + "]");
      }
      client.closeClient(done -> async.complete());
    });
  }
}
