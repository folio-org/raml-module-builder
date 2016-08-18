package com.sling;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.sling.rest.RestVerticle;
import com.sling.rest.persist.PostgresClient;

/**
 * This is our JUnit test for our verticle. VERY UGLY - TO FIX - TODO - move setup into @Test
 * 
 * The test inserts and checks that insert was successful - then opens a transaction - inserts 2 records - selects to verify inserts are not
 * returned then commits and verifies that they are returned
 */
@RunWith(VertxUnitRunner.class)
public class EmbeddedPostgresTest {

  private Vertx             vertx;
  TestContext               context;
  Async                     async = null;

  /**
   * Before executing our test, need to deploy our 2 verticles - the persistence verticle to write to mongo anf then the rest verticle.
   * <p/>
   * 
   * @param context
   *          the test context.
   */
  @Before
  public void setUp(TestContext context) throws IOException {
    vertx = Vertx.vertx();
    this.context = context;
    vertx.deployVerticle(RestVerticle.class.getName(), context.asyncAssertSuccess());

    async = context.async();
    try {
      PostgresClient.setIsEmbedded(true);
      PostgresClient.getInstance(vertx).startEmbeddedPostgres();
      // PostgresClient.getInstance(vertx).importFile(
      // "\"C:\\Git\\lsp-apis-impl\\domain-models-poc\\domain-models-api\\src\\test\\resources\\import.sql\"");//System.getProperty("postgres_import_path"));
      PostgresClient.getInstance(vertx).mutate(
        "create table customers (id SERIAL PRIMARY KEY,name VARCHAR(16) NOT NULL,jsonb JSONB NOT NULL)",
        res -> {
          System.out.println("--------------- testing embedded postgres --------------------");
          try {
            PostgresClient.getInstance(vertx).mutate(
              "insert into customers (name, jsonb) values('joe', '{\"name\": {\"given\": \"joe\", \"family\": \"shmoe\"}}')",
              res2 -> {
                try {
                  PostgresClient.getInstance(vertx).select(
                    "SELECT * FROM customers",
                    res3 -> {
                      System.out.println(res3.result().getNumRows());
                      context.assertEquals(1, res3.result().getNumRows());

                      PostgresClient.getInstance(vertx).startTx(
                        transaction -> {
                          System.out.println("------ testing embedded postgres with transactions -----------");
                          PostgresClient.getInstance(vertx).mutate(
                            transaction,
                            "insert into customers (name, jsonb) values('joe1', '{\"name\": {\"given\": \"joe2\", \"family\": \"shmoe\"}}')",
                            ress -> {
                              //System.out.println(ress.result());
                              PostgresClient.getInstance(vertx).mutate(
                                transaction,
                                "insert into customers (name, jsonb) values('joe2', '{\"name\": {\"given\": \"joe2\", \"family\": \"shmoe\"}}')",
                                ress2 -> {
                                  //System.out.println(ress2.result());
                                  PostgresClient.getInstance(vertx).select("SELECT * FROM customers", ress3 -> {
                                    if (ress3.result() == null) {
                                      context.fail();
                                      async.complete();
                                    } else {
                                      //System.out.println(ress3.result().getNumRows());
                                      context.assertNotEquals(3, ress3.result().getNumRows());
                                      PostgresClient.getInstance(vertx).endTx(transaction, done -> {
                                        //System.out.println(done);
                                        PostgresClient.getInstance(vertx).select("SELECT * FROM customers", resss3 -> {
                                          if (resss3.result() == null) {
                                            context.fail();
                                            async.complete();
                                          } else {
                                            //System.out.println(resss3.result().getNumRows());
                                            context.assertEquals(3, resss3.result().getNumRows());
                                            async.complete();
                                          }
                                        });
                                      });
                                    }
                                  });

                                });
                            });
                        });
                    });
                } catch (Exception e) {
                  e.printStackTrace();
                  context.fail();
                  async.complete();
                }
              });
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
    } catch (Exception e) {
      context.fail(e.getMessage());
      async.complete();
    }
  }

  @Test
  public void test() {
    System.out.println("DONE!");
  }

  /**
   * This method, called after our test, just cleanup everything by closing the vert.x instance
   *
   * @param context
   *          the test context
   */
  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

}
