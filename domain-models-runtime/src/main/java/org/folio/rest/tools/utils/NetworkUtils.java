package org.folio.rest.tools.utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author shale
 *
 */
public class NetworkUtils {

  public static int nextFreePort() {
    int maxTries = 10000;
    int port = ThreadLocalRandom.current().nextInt(49152 , 65535);
    while (true) {
        if (isLocalPortFree(port)) {
            return port;
        } else {
            port = ThreadLocalRandom.current().nextInt(49152 , 65535);
        }
        maxTries--;
        if(maxTries == 0){
          return 8081;
        }
    }
  }
  
  private static boolean isLocalPortFree(int port) {
      try {
          new ServerSocket(port).close();
          return true;
      } catch (IOException e) {
          return false;
      }
  }
  
  
}
