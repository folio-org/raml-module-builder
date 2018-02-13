package org.folio.rest.persist;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.helpers.SimplePojo;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.tools.utils.VertxUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;
import org.z3950.zing.cql.cql2pgjson.FieldException;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class PostgresClientTransactionsIT {
  static private final String TENANT = "tenants";
  static private Vertx vertx;

  @BeforeClass
  public static void setUpClass() throws Exception {
    vertx = VertxUtils.getVertxWithExceptionHandler();
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  private void dropSchemaRole(TestContext context, String schema) {
    Async async = context.async();
    String sql =
        "REASSIGN OWNED BY " + schema + " TO postgres;\n"
      + "DROP OWNED BY " + schema + " CASCADE;\n"
      + "DROP ROLE IF EXISTS " + schema + ";\n";
    PostgresClient.getInstance(vertx).runSQLFile(sql, true, reply -> {
      context.assertTrue(reply.succeeded());
      async.complete();
    });
    async.await();
  }

  private void execute(TestContext context, String sql) {
    Async async = context.async();
    PostgresClient.getInstance(vertx).runSQLFile(sql, false, reply -> {
      context.assertTrue(reply.succeeded());
      for (String result : reply.result()) {
        context.fail(result);
      }
      async.complete();
    });
    async.await();
  }

  private void createSchema(TestContext context, String schema) {
    execute(context,
      "CREATE ROLE " + schema + " PASSWORD '" + TENANT + "' NOSUPERUSER NOCREATEDB INHERIT LOGIN;\n");
    execute(context, "CREATE SCHEMA IF NOT EXISTS " + schema + " AUTHORIZATION " + schema + ";\n");
    execute(context, "GRANT ALL PRIVILEGES ON SCHEMA " + schema + " TO " + schema + ";\n");
    execute(context, "CREATE TABLE IF NOT EXISTS " + schema + ".z (_id SERIAL PRIMARY KEY, jsonb jsonb);\n");
    execute(context, "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA " + schema + " TO " + schema + ";\n");
  }

  private void fillTable(TestContext context, String schema) {
    execute(context,
      "INSERT INTO " + schema + ".z (jsonb) VALUES (" + "'{\"id\": 1,\"name\": \"d\" }'" + ");\n");
  }

  private void transaction(TestContext context, String schema) {
    PostgresClient c1 = PostgresClient.getInstance(vertx, TENANT);
    Async async = context.async();
    //create connection
    c1.startTx( handler -> {
      if(handler.succeeded()){
        SimplePojo z = new SimplePojo();
        z.setId("99");
        z.setName("me");
        //update record
        CQL2PgJSON cql2pgJson = null;
        try {
          cql2pgJson = new CQL2PgJSON("z.jsonb");
        } catch (FieldException e1) {
          e1.printStackTrace();
          context.fail(e1);
        }
        CQLWrapper cql = new CQLWrapper(cql2pgJson, "id==1");
        c1.update(handler,
          "z", z, cql, true, reply -> {
            if(reply.succeeded()){
              //make sure record is not updated since not committed yet
              c1.select("SELECT jsonb FROM " + schema + ".z;", reply2 -> {
                if (! reply2.succeeded()) {
                  context.fail(reply2.cause());
                }
                try {
                 SimplePojo sp =  ObjectMapperTool.getMapper().readValue(
                    reply2.result().getResults().get(0).getString(0), SimplePojo.class);
                 context.assertEquals(sp.getName(), "d", "Name property should not have been changed");
                } catch (Exception e) {
                  e.printStackTrace();
                  context.fail(e.getMessage());
                }
                //end transaction / commit
                c1.endTx(handler, done -> {
                  if(done.succeeded()){
                    //record should have been updated
                    c1.select("SELECT jsonb FROM " + schema + ".z;", selectReply -> {
                      if (! selectReply.succeeded()) {
                        context.fail(selectReply.cause());
                      }
                      else{
                        try {
                          SimplePojo sp =  ObjectMapperTool.getMapper().readValue(
                            selectReply.result().getResults().get(0).getString(0), SimplePojo.class);
                          context.assertEquals(sp.getName(), "me", "Name property should have been changed");
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
              System.out.println("BBBBB");
              context.fail(reply.cause());
            }
          });
      }
      else{
        context.fail(handler.cause());
      }
    });
    async.await();
    c1.closeClient(context.asyncAssertSuccess());
  }

  private void rollback(TestContext context, String schema){
    PostgresClient c1 = PostgresClient.getInstance(vertx, TENANT);
    Async async = context.async();
    c1.startTx( handler -> {
      if(handler.succeeded()){
        SimplePojo z = new SimplePojo();
        z.setId("99");
        z.setName("me");
        c1.update(handler,
          "z", z, "jsonb", "where (jsonb->>'id')::numeric = 1", true, reply -> {
            if(reply.succeeded()){
              c1.rollbackTx(handler, done -> {
                if(done.succeeded()){
                  c1.select("SELECT jsonb FROM " + schema + ".z;", reply2 -> {
                    if (! reply2.succeeded()) {
                      context.fail(reply2.cause());
                    }
                    else{
                      try {
                        SimplePojo sp =  ObjectMapperTool.getMapper().readValue(
                           reply2.result().getResults().get(0).getString(0), SimplePojo.class);
                        context.assertEquals(sp.getName(), "me", "Name property should not have been changed");
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
    async.await();
    c1.closeClient(context.asyncAssertSuccess());
  }

  @Test
  public void test(TestContext context) {
    // don't log expected access violation errors
    LogManager.getRootLogger().setLevel(Level.FATAL);

    String schema = PostgresClient.convertToPsqlStandard(TENANT);

    dropSchemaRole(context, schema);

    createSchema(context, schema);

    fillTable(context, schema);

    transaction(context, schema);

    rollback(context, schema);
  }

}
