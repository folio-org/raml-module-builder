package org.folio.rest.persist;

import java.io.IOException;

/**
 * Stop a PostgresStarter by sending a POST request to the port of PostgresStarter.
 *
 * Example invocation:
 *
 * java -cp target/postgres-runner-fat.jar org.folio.rest.persist.PostgresStopper 5434
 */
public class PostgresStopper {
  public static void main(String [] args) throws IOException, InterruptedException {
    PostgresWaiter.callRunner(args, "POST");
  }
}
