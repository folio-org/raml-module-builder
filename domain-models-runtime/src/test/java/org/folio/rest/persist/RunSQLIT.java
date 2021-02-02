package org.folio.rest.persist;

import org.junit.BeforeClass;
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
  public int expectedResult2;
  @Parameter(2)
  public String sql;

  private Vertx vertx;
  private PostgresClient client;

  @Parameterized.Parameters(name = "{0} {1} {2}")
  static public Iterable<Object []> data() {
    return Arrays.asList(new Object [][] {
        { 0, 0, "update pg_database set datname=null where false" },
        { 0, 0, "update pg_database set datname=null where false;" },
        { 0, 0, "update pg_database set datname=null where false\n" },
        { 1, 1, "update pg_database set datname=null where false\nsyntax error" },
        { 1, 1, "syntaxerror" },
        { 1, 1, "syntaxerror;" },
        { 1, 1, "syntaxerror\n" },
        { 2, 1, "syntaxerror1;\nsyntaxerror2" },
        { 2, 1, "syntaxerror1;\nsyntaxerror2;" },
        { 2, 1, "syntaxerror1;\nsyntaxerror2\n" }
    });
  }

  @BeforeClass
  public static void beforeClass() {
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
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

  @Test
  public void sqlStopOnError(TestContext context) {
    client.runSQLFile(sql, true, context.asyncAssertSuccess(result -> {
      String results = result.stream().collect(Collectors.joining("\n"));
      context.assertEquals(expectedResult2, result.size(),
          "runSQL result.size()=" + result.size() + " [" + results + "]");
    }));
  }

}
