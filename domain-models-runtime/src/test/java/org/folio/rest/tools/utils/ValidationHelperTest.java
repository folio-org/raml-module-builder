package org.folio.rest.tools.utils;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.z3950.zing.cql.cql2pgjson.QueryValidationException;

import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException;
import com.github.mauricio.async.db.postgresql.messages.backend.ErrorMessage;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

/**
 * @author shale
 *
 */
@RunWith(VertxUnitRunner.class)
public class ValidationHelperTest {

  private static final String QueryValidationException1 = "org.z3950.zing.cql.cql2pgjson.QueryValidationException: Field name 'abc' is not present in index.";
  private static final String QueryValidationException2 = "org.z3950.zing.cql.cql2pgjson.QueryValidationException: cql.serverChoice requested, but no serverChoiceIndexes defined.";
  private static final String QueryValidationException3 = "org.z3950.zing.cql.CQLParseException: expected index or term, got EOF";

  @Test
  public void uuidTest(TestContext context) {
    Async async = context.async();
    ErrorMessage em = createErrorMessage("22P02", null, "invalid input syntax for uuid: \"1234567\"");
    Throwable t = new GenericDatabaseException(em);
    ValidationHelper.handleError(t, r -> {
      context.assertEquals(422, r.result().getStatus());
      async.complete();
    });
  }

  @Test
  public void dupTest(TestContext context) {
    Async async = context.async();
    ErrorMessage em = createErrorMessage("23505", "Key (_id)=(55835c7c-2885-44f4-96ac-f03525b8e608) already exists.",
      "duplicate key value violates unique constraint \"123456\"");
    Throwable t = new GenericDatabaseException(em);
    ValidationHelper.handleError(t, r -> {
      context.assertEquals(422, r.result().getStatus());
      async.complete();
    });
  }

  @Test
  public void fkTest(TestContext context) {
    Async async = context.async();
    ErrorMessage em = createErrorMessage("23503",
      "Key (permanentloantypeid)=(2b94c631-fca9-4892-a730-03ee529ffe27) is not present in table \"loan_type\".",
      "insert or update on table \"item\" violates foreign key constraint \"item_permanentloantypeid_fkey\"");
    Throwable t = new GenericDatabaseException(em);
    ValidationHelper.handleError(t, r -> {
      context.assertEquals(422, r.result().getStatus());
      async.complete();
    });
  }

  @Test
  public void cql1Test(TestContext context) {
    Async async = context.async();
    Exception e = new Exception();
    e.initCause(new QueryValidationException(QueryValidationException1));
    ValidationHelper.handleError(e, r -> {
      context.assertEquals(422, r.result().getStatus());
      async.complete();
    });
  }

  @Test
  public void cql2Test(TestContext context) {
    Async async = context.async();
    Exception e = new Exception();
    e.initCause(new QueryValidationException(QueryValidationException2));
    ValidationHelper.handleError(e, r -> {
      context.assertEquals(422, r.result().getStatus());
      async.complete();
    });
  }

  @Test
  public void cql3Test(TestContext context) {
    Async async = context.async();
    Exception e = new Exception();
    e.initCause(new QueryValidationException(QueryValidationException3));
    ValidationHelper.handleError(e, r -> {
      context.assertEquals(422, r.result().getStatus());
      async.complete();
    });
  }

  @Test
  public void generalTest(TestContext context) {
    Async async = context.async();
    Exception e = new Exception("any text");
    ValidationHelper.handleError(e, r -> {
      context.assertEquals(500, r.result().getStatus());
      async.complete();
    });
  }

  @Test
  public void authTest(TestContext context) {
    Async async = context.async();
    ErrorMessage em = createErrorMessage("28P01",
      null,
      "password authentication failed for user \"harvard9_mod_configuration\"");
    Throwable t = new GenericDatabaseException(em);
    ValidationHelper.handleError(t, r -> {
      context.assertEquals(401, r.result().getStatus());
      async.complete();
    });
  }


  private ErrorMessage createErrorMessage(String sqlstate, String detail, String message){
      Map<Object, String> javaMap = new HashMap<>();
      javaMap.put('C', sqlstate);
      javaMap.put('D', detail);
      javaMap.put('M', message);
      ErrorMessage errorMessage = new ErrorMessage(scalaMap(javaMap));
      return errorMessage;
  }

  private scala.collection.immutable.Map<Object, String> scalaMap(Map<Object, String> javaMap) {
    return JavaConverters.mapAsScalaMapConverter(javaMap).asScala().toMap(Predef.<Tuple2<Object, String>>conforms());
  }
}
