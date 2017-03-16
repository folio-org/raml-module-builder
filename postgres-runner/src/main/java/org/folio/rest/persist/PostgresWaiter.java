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

  public static void main(String [] args) throws IOException, InterruptedException {
    if (args.length != 1) {
      throw new IllegalArgumentException("Invoke with a single argument: the port of PostgresStarter");
    }

    int port = Integer.parseUnsignedInt(args[0]);
    String urlString = "http://localhost:" + port;
    URL url = new URL(urlString);
    for (int i=10; i>=0; i--) {
      HttpURLConnection conn = null;
      try {
        conn = (HttpURLConnection) url.openConnection();
        // HttpURLConnection's default is a GET request
        // conn.getResponseCode() waits until PostgresRunner has responded
        if (conn.getResponseCode() != 200) {
          throw new IllegalStateException(
              "HTTP response code=" + conn.getResponseCode() + " " + conn.getResponseMessage());
        }
        break;  // waiting ended without exception, so end program
      }
      catch (IOException e) {
        // we might get "java.net.ConnectException: Connection refused: connect" when
        // the listener is not ready. So we try several times with sleeping 1 second
        // in between.
        if (i == 0) {
          // last time throw exception to give user some feedback
          throw new IOException(e.getMessage() + ": " + urlString, e);
        }
      }
      finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
      TimeUnit.SECONDS.sleep(1);
    }
  }
}
