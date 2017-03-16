package org.folio.rest.persist;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Stop a PostgresStarter by sending a POST request to the port of PostgresStarter.
 *
 * Example invocation:
 *
 * java -cp target/postgres-runner-fat.jar org.folio.rest.persist.PostgresStopper 5434
 */
public class PostgresStopper {
  private PostgresStopper() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  public static void main(String [] args) throws IOException {
    if (args.length != 1) {
      throw new IllegalArgumentException("Invoke with a single argument: the port of PostgresStarter");
    }

    int port = Integer.parseUnsignedInt(args[0]);
    String urlString = "http://localhost:" + port;
    HttpURLConnection conn = null;
    try {
      URL url = new URL(urlString);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      // conn.getResponseCode() waits until PostgresRunner has responded
      if (conn.getResponseCode() != 200) {
        throw new IOException("HTTP response code=" + conn.getResponseCode() + " " + conn.getResponseMessage());
      }
    }
    catch (IOException e) {
      throw new IOException(e.getMessage() + ": " + urlString, e);
    }
    finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }
}
