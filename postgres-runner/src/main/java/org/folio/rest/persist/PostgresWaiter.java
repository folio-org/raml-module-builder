package org.folio.rest.persist;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Wait until the PostgresStarter has started Postgres so that Postgres is ready.
 * This is done by sending a GET request to the port of PostgresStarter.
 *
 * Example invocation:
 *
 * java -cp target/postgres-runner-fat.jar org.folio.rest.persist.PostgresWaiter 5434
 */
public class PostgresWaiter {
  private PostgresWaiter() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  @SuppressWarnings("squid:S106")  // "Replace this usage of System.out or System.err by a logger."
  public static void main(String [] args) throws IOException, InterruptedException {
    if (args.length != 1) {
      System.out.println("Invoke with a single argument: the port of PostgresStarter");
      return;
    }

    int port = Integer.parseUnsignedInt(args[0]);
    URL url = new URL("http://localhost:" + port);
    for (int i=10; i>=0; i--) {
      try {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        // default is a GET request
        // conn.getResponseCode() waits for the response from the PostgresRunner
        System.out.println(conn.getResponseCode() + " " + conn.getResponseMessage());
        conn.disconnect();
        break;  // waiting ended without exception, so end program
      }
      catch (IOException e) {
        // we might get "java.net.ConnectException: Connection refused: connect" when
        // the listener is not ready. So we try several times with sleeping 1 second
        // in between.
        if (i == 0) {
          // last time throw exception to give user some feedback
          throw e;
        }
      }
      TimeUnit.SECONDS.sleep(1);
    }
  }
}
