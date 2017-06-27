package org.folio.rest.persist;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * Create PostgresClient on different vertx.
 *
 * Checks for the bug <a href="https://issues.folio.org/browse/RMB-38">https://issues.folio.org/browse/RMB-38</a>
 * that yields this error:
 *
 * org.folio.rest.persist.PostgresClient SEVERE Task io.vertx.core.impl.OrderedExecutorFactory$OrderedExecutor$$Lambda$9/1205555397@3306fc96 rejected from java.util.concurrent.ThreadPoolExecutor@76047492[Terminated, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 1]
 * java.util.concurrent.RejectedExecutionException: Task io.vertx.core.impl.OrderedExecutorFactory$OrderedExecutor$$Lambda$9/1205555397@3306fc96 rejected from java.util.concurrent.ThreadPoolExecutor@76047492[Terminated, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 1]
 * at java.util.concurrent.ThreadPoolExecutor$AbortPolicy.rejectedExecution(ThreadPoolExecutor.java:2047)
 */
@RunWith(VertxUnitRunner.class)
public class PostgresClientMultiVertxIT {
  @Rule
  public Timeout rule = Timeout.seconds(5);

  @Rule
  public RunTestOnContext contextRule = new RunTestOnContext();  // different vertx for each @Test

  private void run(TestContext context) {
    Async async = context.async();
    PostgresClient client = PostgresClient.getInstance(contextRule.vertx());
    client.runSQLFile("UPDATE pg_database SET datname=null WHERE false;\n", true, r -> {
      async.complete();
      // it does not trigger the bug when replacing the previous line with:
      // client.closeClient(whenDone -> async.complete());
      // But it must work with two clients running in parallel.
    });
  }

  @Test
  public void test1(TestContext context) {
    run(context);
  }

  @Test
  public void test2(TestContext context) {
    run(context);
  }
}
