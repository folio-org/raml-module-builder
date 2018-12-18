package org.folio.rest.persist;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.Arrays;
import java.util.stream.Collectors;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;

import org.folio.rest.tools.utils.VertxUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class RunSQLIT {
  @Parameter(0)
  public int expectedResultSize;
  @Parameter(1)
  public String sql;

  private Vertx vertx;
  private PostgresClient client;

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

  @Before
  public void setUp() {
    vertx = VertxUtils.getVertxFromContextOrNew();
    client = PostgresClient.getInstance(vertx);
  }

  @After
  public void tearDown(TestContext context) {
    client.closeClient(context.asyncAssertSuccess());
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void sql(TestContext context) {
    client.runSQLFile(sql, false, context.asyncAssertSuccess(result -> {
      String results = result.stream().collect(Collectors.joining("\n"));
      context.assertEquals(expectedResultSize, result.size(),
            "runSQL result.size()=" + result.size() + " [" + results + "]");
    }));
  }
}
