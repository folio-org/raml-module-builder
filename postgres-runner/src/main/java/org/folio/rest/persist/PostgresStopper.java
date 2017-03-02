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

  @SuppressWarnings("squid:S106")  // "Replace this usage of System.out or System.err by a logger."
  public static void main(String [] args) throws IOException {
    if (args.length != 1) {
      System.out.println("Invoke with a single argument: the port of PostgresStarter");
      return;
    }

    int port = Integer.parseUnsignedInt(args[0]);
    URL url = new URL("http://localhost:" + port);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    System.out.println(conn.getResponseCode() + " " + conn.getResponseMessage());
    conn.disconnect();
  }
}
