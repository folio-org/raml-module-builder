package org.folio.rest.persist;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Wait until the PostgresStarter has started Postgres so that Postgres is ready.
 * This is done by sending a GET request to the port of PostgresStarter.
 *
 * <p>Example invocation:
 *
 * <p>java -cp target/postgres-runner-fat.jar org.folio.rest.persist.PostgresWaiter 5434
 */
public class PostgresWaiter {
  static int secondsToSleep = 1;

  static int runnerPort(String [] args, Map<String,String> env) {
    if (args.length == 1) {
      return Integer.parseInt(args[0]);
    }
    if (args.length == 0) {
      String port = PostgresRunner.getenv(env, "DB_RUNNER_PORT");
      if (port == null) {
        return 6001;
      }
      return Integer.parseInt(port);
    }
    throw new IllegalArgumentException("Found " + args.length + " command line parameters, "
        + "expected 0, or only the port of PostgresStarter");
  }

  static void callRunner(String [] args, String method) throws IOException, InterruptedException {
    int port = runnerPort(args, System.getenv());
    String urlString = "http://localhost:" + port;
    URL url = new URL(urlString);
    for (int i = 10; i >= 0; i--) {
      HttpURLConnection conn = null;
      try {
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        // conn.getResponseCode() waits until PostgresRunner has responded
        if (conn.getResponseCode() != 200) {
          throw new IllegalStateException(
              "HTTP response code=" + conn.getResponseCode() + " " + conn.getResponseMessage());
        }
        break;  // waiting ended without exception, so end program
      } catch (IOException e) {
        // we might get "java.net.ConnectException: Connection refused: connect" when
        // the listener is not ready. So we try several times with sleeping 1 second
        // in between.
        if (i == 0) {
          // last time throw exception to give user some feedback
          throw new IOException(e.getMessage() + ": " + urlString, e);
        }
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
      TimeUnit.SECONDS.sleep(secondsToSleep);
    }
  }

  public static void main(String [] args) throws IOException, InterruptedException {
    callRunner(args, "GET");
  }
}
