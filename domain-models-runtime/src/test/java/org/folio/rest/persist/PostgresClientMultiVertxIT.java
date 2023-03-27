package org.folio.rest.persist;

import io.vertx.core.Promise;
import java.util.List;

import org.folio.rest.tools.utils.VertxUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
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
  public Timeout rule = Timeout.seconds(15);

  @Rule
  public RunTestOnContext contextRule = new RunTestOnContext();  // different vertx for each @Test

  private void run(TestContext context) {
    Async async = context.async();
    PostgresClient client = PostgresClient.getInstance(contextRule.vertx());
    client.runSqlFile("UPDATE pg_database SET datname=null WHERE false;\n")
    .onComplete(context.asyncAssertSuccess(x -> async.complete()));
    // it does not trigger the bug when replacing the previous line with:
    // client.closeClient(whenDone -> async.complete());
    // But it must work with two clients running in parallel.
  }

  @Test
  public void test1(TestContext context) {
    run(context);
  }

  @Test
  public void test2(TestContext context) {
    run(context);
  }

  public class Verticle extends AbstractVerticle {
    private PostgresClient client;

    @Override
    public void start(Promise<Void> startPromise) {
      client = PostgresClient.getInstance(vertx);
      startPromise.complete();
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
      client.closeClient(stopPromise);
    }

    public void runSQL(Handler<AsyncResult<List<String>>> handler) {
      client.runSQLFile("UPDATE pg_database SET datname=null WHERE false;\n", true, handler);
    }
  }

  @Test
  public void testParallel(TestContext context) {
    Async async = context.async();
    Vertx vertx1 = VertxUtils.getVertxWithExceptionHandler();
    Vertx vertx2 = VertxUtils.getVertxWithExceptionHandler();
    Vertx vertx3 = VertxUtils.getVertxWithExceptionHandler();
    Verticle v1 = new Verticle();
    Verticle v2 = new Verticle();
    Verticle v3 = new Verticle();
    vertx1.deployVerticle(v1, d1 -> {
      vertx2.deployVerticle(v2, d2 -> {
        vertx3.deployVerticle(v3, d3 -> {
          v1.runSQL(r1 -> {
            v2.runSQL(r2 -> {
              v3.runSQL(r3 -> {
                vertx1.undeploy(d1.result(), u1 -> {
                  vertx1.close(c1 -> {
                    vertx3.undeploy(d3.result(), u3 -> {
                      vertx3.close(c3 -> {
                        // does v2 work after v1 and v3 have been removed?
                        v2.runSQL(v2after -> {
                          context.assertTrue(v2after.succeeded());
                          context.assertEquals(0, v2after.result().size());
                          async.complete();
                        });
                      });
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  }
}
