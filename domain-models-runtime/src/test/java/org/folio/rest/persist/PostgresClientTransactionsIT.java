package org.folio.rest.persist;

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
    PostgresClient c1 = PostgresClient.getInstance(vertx, tenant);

    c1.withTransaction(f ->
      f.query("INSERT INTO " + fullTable + " VALUES (2, '{ \"name\": \"a1\"}');\n").execute().map("inserted")
    ).onComplete(context.asyncAssertSuccess(res -> context.assertEquals("inserted", res)));
  }

  @Test
  public void testWithTransactionFailure(TestContext context) {
    PostgresClient c1 = PostgresClient.getInstance(vertx, tenant);

    c1.withTransaction(f ->
        f.query("INSERT INTO " + fullTable + " VALUES (2,").execute().map("inserted")
    ).onComplete(context.asyncAssertFailure(cause ->
        context.assertTrue(cause.getMessage().contains("syntax error at end of input"))));
  }

  @Test
  public void testWithConnectionSuccess(TestContext context) {
    PostgresClient c1 = PostgresClient.getInstance(vertx, tenant);

    c1.withConnection(f ->
        f.query("INSERT INTO " + fullTable + " VALUES (3, '{ \"name\": \"a1\"}');\n").execute().map("inserted")
    ).onComplete(context.asyncAssertSuccess(res -> context.assertEquals("inserted", res)));
  }

  @Test
  public void testWithConnectionFailure(TestContext context) {
    PostgresClient c1 = PostgresClient.getInstance(vertx, tenant);

    c1.withConnection(f ->
        f.query("INSERT INTO " + fullTable + " VALUES (3,").execute().map("inserted")
    ).onComplete(context.asyncAssertFailure(cause ->
        context.assertTrue(cause.getMessage().contains("syntax error at end of input"))));
  }

}
