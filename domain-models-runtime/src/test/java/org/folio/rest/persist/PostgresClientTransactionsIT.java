package org.folio.rest.persist;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgNotification;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.spi.DatabaseMetadata;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.helpers.SimplePojo;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import java.util.concurrent.atomic.AtomicInteger;

@RunWith(VertxUnitRunner.class)
public class PostgresClientTransactionsIT extends PostgresClientITBase {
  private static final String table = "z";
  private static final String fullTable = schema + ".z";

  @BeforeClass
  public static void beforeClass(TestContext context) throws Exception {
    setUpClass(context);
    executeSuperuser(context,
        "CREATE TABLE " + fullTable + " (id int PRIMARY KEY, jsonb JSONB NOT NULL)",
        "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA " + schema + " TO " + schema);
  }

  private void fillTable(TestContext context, String name) {
    execute(context, "INSERT INTO " + fullTable + " VALUES (1, '{ \"name\": \"" + name + "\"}');\n");
  }

  private void updateTransaction(TestContext context) {
    PostgresClient c1 = PostgresClient.getInstance(vertx, tenant);
    Async async = context.async();
    //create connection
    c1.startTx(handler -> {
      if(handler.succeeded()){
        SimplePojo z = new SimplePojo();
        z.setId("1");
        z.setName("me");
        //update record
        CQL2PgJSON cql2pgJson = null;
        try {
          cql2pgJson = new CQL2PgJSON(table + ".jsonb");
        } catch (FieldException e1) {
          e1.printStackTrace();
          context.fail(e1);
        }
        CQLWrapper cql = new CQLWrapper(cql2pgJson, "name==d");
        c1.update(handler,
          "z", z, cql, true, reply -> {
            if(reply.succeeded()){
              //make sure record is not updated since not committed yet
              c1.select("SELECT jsonb->>'name' FROM " + fullTable, reply2 -> {
                if (! reply2.succeeded()) {
                  context.fail(reply2.cause());
                }
                try {
                  String name = reply2.result().iterator().next().getString(0);
                  context.assertEquals("d", name, "Name property should not have been changed");
                } catch (Exception e) {
                  e.printStackTrace();
                  context.fail(e.getMessage());
                }
                //end transaction / commit
                c1.endTx(handler, done -> {
                  if(done.succeeded()){
                    //record should have been updated
                    c1.select("SELECT jsonb->>'name' FROM " + fullTable, selectReply -> {
                      if (! selectReply.succeeded()) {
                        context.fail(selectReply.cause());
                      }
                      else{
                        try {
                          String name = selectReply.result().iterator().next().getString(0);
                          context.assertEquals("me", name, "Name property should have been changed");
                         } catch (Exception e) {
                           e.printStackTrace();
                           context.fail(e.getMessage());
                         }
                        async.complete();
                      }
                    });
                  }
                  else{
                    context.fail(done.cause());
                  }
                });
              });
            }
            else{
              context.fail(reply.cause());
            }
          });
      }
      else{
        context.fail(handler.cause());
      }
    });
    async.await(5000 /* ms */);
    c1.closeClient(context.asyncAssertSuccess());
  }

  private void rollback(TestContext context) {
    PostgresClient c1 = PostgresClient.getInstance(vertx, tenant);
    Async async = context.async();
    c1.startTx( handler -> {
      if(handler.succeeded()){
        SimplePojo z = new SimplePojo();
        z.setId("1");
        z.setName("me");
        c1.update(handler,
          table, z, "jsonb", "where (jsonb->>'name') = 'd'", true, reply -> {
            if(reply.succeeded()){
              c1.rollbackTx(handler, done -> {
                if(done.succeeded()){
                  c1.select("SELECT jsonb->>'name' FROM " + table, reply2 -> {
                    if (! reply2.succeeded()) {
                      context.fail(reply2.cause());
                    }
                    else{
                      try {
                        String name = reply2.result().iterator().next().getString(0);
                        context.assertEquals("me", name, "Name property should not have been changed");
                      } catch (Exception e) {
                         e.printStackTrace();
                         context.fail(e.getMessage());
                      }
                      async.complete();
                    }
                  });
                }
                else{
                  context.fail(done.cause());
                }
              });
            }
            else{
              context.fail(reply.cause());
            }
          });
      }
      else{
        context.fail(handler.cause());
      }
    });
    async.await(5000 /* ms */);
    c1.closeClient(context.asyncAssertSuccess());
  }

  private void deleteTransaction(TestContext context) {
    PostgresClient c1 = PostgresClient.getInstance(vertx, tenant);
    Async async = context.async();
    //create connection
    c1.startTx( handler -> {
      if(handler.succeeded()){
        Criteria c = new Criteria();
        c.addField("'name'").setOperation("=").setVal("me");
        c1.delete(handler, table, new Criterion(c) , reply -> {
            if(reply.succeeded()){
              //make sure record is deleted when querying using this connection
              //but since not committed yet we should still get it back
              //when sending the query on a different connection
              c1.get(handler, "z", SimplePojo.class , new Criterion(c) ,
                  true, true, reply2 -> {
                if (! reply2.succeeded()) {
                  context.fail(reply2.cause());
                  async.complete();
                }
                else{
                  try {
                    int size = reply2.result().getResults().size();
                    context.assertEquals(0, size);
                    //call get() without the connection. since we did not commit yet
                    //this should still return the deleted record
                    c1.get(table, SimplePojo.class , new Criterion(c) ,
                      true, true, reply3 -> {
                        if (! reply3.succeeded()) {
                          context.fail(reply3.cause());
                          async.complete();
                        }
                        else{
                          int size2 = reply3.result().getResults().size();
                          context.assertEquals(1, size2);
                          //end transaction / commit
                          //doesnt seem like a good idea to reuse the handler within the get()
                          //which lives outside of this connection, but for testing ok.
                          c1.endTx(handler, done -> {
                            if(done.succeeded()){
                              //record should have been deleted, so only one record should return
                              //not both
                              c1.select("SELECT jsonb FROM " + fullTable, selectReply -> {
                                if (! selectReply.succeeded()) {
                                  context.fail(selectReply.cause());
                                  async.complete();
                                }
                                else{
                                  try {
                                    int size3 = selectReply.result().size();
                                    context.assertEquals(0, size3);
                                   } catch (Exception e) {
                                     e.printStackTrace();
                                     context.fail(e.getMessage());
                                   }
                                  async.complete();
                                }
                              });
                            }
                            else{
                              context.fail(done.cause());
                            }
                          });
                        }
                    });
                   } catch (Exception e) {
                     e.printStackTrace();
                     context.fail(e.getMessage());
                     async.complete();
                   }
                }
              });
            }
            else{
              context.fail(reply.cause());
            }
          });
      }
      else{
        context.fail(handler.cause());
      }
    });
    async.await(5000 /* ms */);
    c1.closeClient(context.asyncAssertSuccess());
  }

  @Test
  public void test(TestContext context) {
    fillTable(context, "d");
    updateTransaction(context);
    rollback(context);
    deleteTransaction(context);
  }

  @Test
  public void testWithTransactionSuccess(TestContext context) {
    AtomicInteger open = new AtomicInteger();
    AtomicInteger active = new AtomicInteger();
    PostgresClient c1 = postgresClientMonitor(open, active);
    {
      Async async = context.async();
      c1.withTransaction(f -> f
          .query("INSERT INTO " + fullTable + " VALUES (2, '{\"name\": \"a2\"}');")
          .execute()
          .onComplete(x -> {
            context.assertEquals(1, open.get());
            context.assertEquals(1, active.get());
          })
          .flatMap(res -> f
              .query("INSERT INTO " + fullTable + " VALUES (3, '{\"name\": \"a3\"}');")
              .execute().map("inserted 2")
          )
      ).onComplete(context.asyncAssertSuccess(res -> {
        context.assertEquals("inserted 2", res);
        async.complete();
      }));
      async.await();
    }
    context.assertEquals(0, open.get());
    context.assertEquals(0, active.get());
    c1.select("SELECT jsonb->>'name' FROM " + fullTable + " WHERE ID=2 OR ID=3",
        context.asyncAssertSuccess(res -> context.assertEquals(2, res.size())));
  }

  @Test
  public void testWithTransactionFailure(TestContext context) {
    AtomicInteger open = new AtomicInteger();
    AtomicInteger active = new AtomicInteger();
    PostgresClient c1 = postgresClientMonitor(open, active);
    {
      Async async = context.async();
      c1.withTransaction(f -> f
          .query("INSERT INTO " + fullTable + " VALUES (4, '{\"name\": \"a4\"}');")
          .execute()
          .onComplete(x -> {
            context.assertEquals(1, open.get());
            context.assertEquals(1, active.get());
          })
          .flatMap(res -> f
              .query("INSERT INTO " + fullTable + " VALUES (5,")
              .execute().map("inserted 2")
          )).onComplete(context.asyncAssertFailure(cause -> {
        context.assertTrue(cause.getMessage().contains("syntax error at end of input"));
        async.complete();
      }));
      async.await();
    }
    context.assertEquals(0, open.get());
    context.assertEquals(0, active.get());
    // first one rolled back
    c1.select("SELECT jsonb->>'name' FROM " + fullTable + " WHERE ID=4",
        context.asyncAssertSuccess(res -> context.assertEquals(0, res.size())));
  }

  @Test
  public void testWithConnectionSuccess(TestContext context) {
    AtomicInteger open = new AtomicInteger();
    AtomicInteger active = new AtomicInteger();
    PostgresClient c1 = postgresClientMonitor(open, active);
    {
      Async async = context.async();
      c1.withConnection(f -> f
          .query("INSERT INTO " + fullTable + " VALUES (6, '{\"name\": \"a6\"}');")
          .execute()
          .onComplete(x -> {
            context.assertEquals(1, open.get());
            context.assertEquals(0, active.get());
          })
          .flatMap(res -> f
              .query("INSERT INTO " + fullTable + " VALUES (7, '{\"name\": \"a7\"}');")
              .execute().map("inserted 2")
          )).onComplete(context.asyncAssertSuccess(res -> {
        context.assertEquals("inserted 2", res);
        async.complete();
      }));
      async.await();
    }
    context.assertEquals(0, open.get());
    context.assertEquals(0, active.get());
    c1.select("SELECT jsonb->>'name' FROM " + fullTable + " WHERE ID=6 OR ID=7",
        context.asyncAssertSuccess(res -> context.assertEquals(2, res.size())));
  }

  @Test
  public void testWithConnectionFailure(TestContext context) {
    AtomicInteger open = new AtomicInteger();
    AtomicInteger active = new AtomicInteger();
    PostgresClient c1 = postgresClientMonitor(open, active);
    context.assertEquals(0, open.get());
    {
      Async async = context.async();
      c1.withConnection(f -> f
          .query("INSERT INTO " + fullTable + " VALUES (8, '{\"name\": \"a8\"}');")
          .execute()
          .onComplete(x -> {
            context.assertEquals(1, open.get());
            context.assertEquals(0, active.get());
          })
          .flatMap(res -> f
              .query("INSERT INTO " + fullTable + " VALUES (9,")
              .execute().map("inserted 2")
          )).onComplete(context.asyncAssertFailure(cause -> {
        context.assertTrue(cause.getMessage().contains("syntax error at end of input"));
        async.complete();
      }));
      async.await();
    }
    context.assertEquals(0, open.get());
    context.assertEquals(0, active.get());
    // no rollback, so first one was inserted..
    c1.select("SELECT jsonb->>'name' FROM " + fullTable + " WHERE ID=8",
        context.asyncAssertSuccess(res -> context.assertEquals(1, res.size())));
  }

  class MonitorTransaction implements Transaction {
    final Transaction transaction;
    final AtomicInteger active;

    MonitorTransaction(Transaction transaction, AtomicInteger active) {
      this.transaction = transaction;
      this.active = active;
    }

    @Override
    public Future<Void> commit() {
      active.decrementAndGet();
      return transaction.commit();
    }

    @Override
    public void commit(Handler<AsyncResult<Void>> handler) {
      commit().onComplete(handler);
    }

    @Override
    public Future<Void> rollback() {
      active.decrementAndGet();
      return transaction.rollback();
    }

    @Override
    public void rollback(Handler<AsyncResult<Void>> handler) {
      rollback().onComplete(handler);
    }

    @Override
    public void completion(Handler<AsyncResult<Void>> handler) {
      transaction.completion(handler);
    }

    @Override
    public Future<Void> completion() {
      return transaction.completion();
    }
  }

  class MonitorPgConnection implements PgConnection {
    final PgConnection conn;
    final AtomicInteger open;
    final AtomicInteger active;

    MonitorPgConnection(PgConnection conn, AtomicInteger open, AtomicInteger active) {
      this.open = open;
      this.active = active;
      open.incrementAndGet();
      this.conn = conn;
    }

    @Override
    public PgConnection notificationHandler(Handler<PgNotification> handler) {
      conn.notificationHandler(handler);
      return this;
    }

    @Override
    public PgConnection cancelRequest(Handler<AsyncResult<Void>> handler) {
      conn.cancelRequest(handler);
      return this;
    }

    @Override
    public int processId() {
      return conn.processId();
    }

    @Override
    public int secretKey() {
      return conn.secretKey();
    }

    @Override
    public PgConnection prepare(String s, Handler<AsyncResult<PreparedStatement>> handler) {
      conn.prepare(s, handler);
      return this;
    }

    @Override
    public Future<PreparedStatement> prepare(String s) {
      return conn.prepare(s);
    }

    @Override
    public PgConnection exceptionHandler(Handler<Throwable> handler) {
      conn.exceptionHandler(handler);
      return this;
    }

    @Override
    public PgConnection closeHandler(Handler<Void> handler) {
      conn.closeHandler(handler);
      return this;
    }

    @Override
    public void begin(Handler<AsyncResult<Transaction>> handler) {
      begin().onComplete(handler);
    }

    @Override
    public Future<Transaction> begin() {
      active.incrementAndGet();
      return conn.begin().map(trans -> new MonitorTransaction(trans, active));
    }

    @Override
    public boolean isSSL() {
      return conn.isSSL();
    }

    @Override
    public Query<RowSet<Row>> query(String s) {
      return conn.query(s);
    }

    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
      return conn.preparedQuery(s);
    }

    @Override
    public void close(Handler<AsyncResult<Void>> handler) {
      close().onComplete(handler);
    }

    @Override
    public Future<Void> close() {
      open.decrementAndGet();
      return conn.close();
    }

    @Override
    public DatabaseMetadata databaseMetadata() {
      return conn.databaseMetadata();
    }
  }

  private PostgresClient postgresClientMonitor(AtomicInteger open, AtomicInteger active) {
    try {
      PostgresClient postgresClient = new PostgresClient(vertx, tenant);
      PgPool ePool = postgresClient.getClient();
      PgPool client = new PgPoolBase() {

        @Override
        public Future<SqlConnection> getConnection() {
          return ePool.getConnection().map(conn -> new MonitorPgConnection((PgConnection) conn, open, active));
        }

        @Override
        public Query<RowSet<Row>> query(String s) {
          return ePool.query(s);
        }

        @Override
        public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
          return ePool.preparedQuery(s);
        }

        @Override
        public Future<Void> close() {
          return ePool.close();
        }
      };
      postgresClient.setClient(client);
      return postgresClient;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
