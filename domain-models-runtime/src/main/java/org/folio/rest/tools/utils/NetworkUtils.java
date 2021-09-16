package org.folio.rest.tools.utils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.URL;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author shale
 *
 */
public class NetworkUtils {

  /**
 *  ThreadLocalRandom has a Random instance per thread and safeguards against contention.
 */
 @SuppressWarnings("all")
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

  public static byte[] object2Bytes(Object obj) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = null;
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(obj);
      byte[] bytes = bos.toByteArray();
      return bytes;
    } finally {
      try {
        if (out != null) { out.close(); }
      } catch (IOException ex) {/*ignore*/}
      try {
        bos.close();
      } catch (IOException ex) {/*ignore*/}
    }
  }

  public static String readURL(String path) throws Exception {
    URL url = new URL(path);
    BufferedReader in = null;
    StringBuffer content = new StringBuffer();
    String input;
    try {
      in = new BufferedReader(new InputStreamReader(url.openStream()));
      while ((input = in.readLine()) != null)
        content.append(input);
      return content.toString();
    } finally {
      if(in != null){
        in.close();
      }
    }
  }

  public static boolean isValidURL(String path){
    try {
      new URL(path);
      return true;
    }
    catch(Exception e){
      return false;
    }
  }

}
