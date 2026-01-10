package org.folio.rest.persist;

import io.vertx.core.VerticleBase;

import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

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

  @BeforeClass
  public static void setUp() {
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
  }

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

  public class Verticle extends VerticleBase {
    private Vertx vertx = VertxUtils.getVertxWithExceptionHandler();
    private String deploymentId;
    private PostgresClient client;

    public Future<String> deploy() {
      return vertx.deployVerticle(this)
          .onSuccess(id -> deploymentId = id);
    }

    public Future<Void> undeploy() {
      return vertx.undeploy(deploymentId);
    }

    @Override
    public Future<?> start() {
      try {
        client = PostgresClient.getInstance(vertx);
        return super.start();
      } catch (Exception e) {
        return Future.failedFuture(e);
      }
    }

    @Override
    public Future<?> stop() {
      return client.closeClient()
          .compose(x -> {
            try {
              return super.stop();
            } catch (Exception e) {
              return Future.failedFuture(e);
            }
          });
    }

    public Future<RowSet<Row>> runSQL() {
      return client.execute("UPDATE pg_database SET datname=null WHERE false");
    }
  }

  @Test
  public void testParallel(TestContext context) {
    Verticle v1 = new Verticle();
    Verticle v2 = new Verticle();
    Verticle v3 = new Verticle();
    v1.deploy()
    .compose(x -> v2.deploy())
    .compose(x -> v3.deploy())
    .compose(x -> v1.runSQL())
    .compose(x -> v2.runSQL())
    .compose(x -> v3.runSQL())
    .compose(x -> v1.undeploy())
    .compose(x -> v3.undeploy())
    // does v2 work after v1 and v3 have been removed?
    .compose(c3 -> v2.runSQL())
    .onComplete(context.asyncAssertSuccess());
  }
}
