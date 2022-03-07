package org.folio.rest.tools.utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.PrimitiveIterator.OfInt;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author shale
 *
 */
public final class NetworkUtils {

  private NetworkUtils() {
    throw new UnsupportedOperationException("Don't instantiate utility class");
  }

  @SuppressWarnings("java:S2245")//Suppress "Weak Cryptography" warning, ThreadLocalRandom is good enough for finding a free port
  public static int nextFreePort() {
    return nextFreePort(10000, ThreadLocalRandom.current().ints(49152 , 65535).iterator());
  }

  static int nextFreePort(int maxTries, OfInt portsToTry) {
    for (int i = 0; i < maxTries; i++) {
      int port = portsToTry.nextInt();
      if (isLocalPortFree(port)) {
        return port;
      }
    }
    return 8081;
  }

  /**
   * Check a local TCP port.
   * @param port  the TCP port number, must be from 1 ... 65535
   * @return true if the port is free (unused), false if the port is already in use
   */
  public static boolean isLocalPortFree(int port) {
      try {
          new ServerSocket(port).close();
          return true;
      } catch (IOException e) {
          return false;
      }
  }

}
