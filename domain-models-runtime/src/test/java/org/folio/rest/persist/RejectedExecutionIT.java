package org.folio.rest.persist;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class RejectedExecutionIT {
  @Rule
  public Timeout rule = Timeout.seconds(5);

  @Rule
  public RunTestOnContext contextRule = new RunTestOnContext();  // different vertx for each @Test

  private void run(TestContext context) {
    Async async = context.async();
    PostgresClient client = PostgresClient.getInstance(contextRule.vertx());
    client.runSQLFile("UPDATE pg_database SET datname=null WHERE false;\n", true, r -> {
      async.complete();
      // it works when replacing the previous line with:
      // client.closeClient(whenDone -> async.complete());
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

  /*
   * The second run throws RejectedExecutionException:
   *
   * org.folio.rest.persist.PostgresClient SEVERE Task io.vertx.core.impl.OrderedExecutorFactory$OrderedExecutor$$Lambda$9/1205555397@3306fc96 rejected from java.util.concurrent.ThreadPoolExecutor@76047492[Terminated, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 1]
java.util.concurrent.RejectedExecutionException: Task io.vertx.core.impl.OrderedExecutorFactory$OrderedExecutor$$Lambda$9/1205555397@3306fc96 rejected from java.util.concurrent.ThreadPoolExecutor@76047492[Terminated, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 1]
   * at java.util.concurrent.ThreadPoolExecutor$AbortPolicy.rejectedExecution(ThreadPoolExecutor.java:2047)
   * …
   * at io.vertx.core.impl.VertxImpl.executeBlocking(VertxImpl.java:634)
   * at org.folio.rest.persist.PostgresClient.execute(PostgresClient.java:1788)
   * at org.folio.rest.persist.PostgresClient.runSQLFile(PostgresClient.java:1702)
   * at org.folio.rest.persist.RejectedExecutionIT.run(RejectedExecutionIT.java:27)
   * at org.folio.rest.persist.RejectedExecutionIT.test2(RejectedExecutionIT.java:41)
   */
}
