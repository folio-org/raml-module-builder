package org.folio.util;

import java.io.Closeable;
import org.testcontainers.containers.Network;

public interface PostgresTester extends Closeable {
  /**
   * start tester.
   * @param database Postgres database
   * @param username Postgres username
   * @param password Postgres password
   */
  void start(String database, String username, String password) throws PostgresTesterStartException;

  /**
   * return listening port for spawned tester.
   * @return port
   */
  Integer getPort();

  /**
   * return host for tester.
   * @return host
   */
  String getHost();

  /**
   * return read-only host for tester.
   * @return host
   */
  String getReadHost();

  /**
   * return read-only listening port for spawned tester.
   * @return port
   */
  Integer getReadPort();

  /**
   * The network that the read and write hosts are part of.
   */
  Network getNetwork();

  /**
   * has tester started.
   * @return true if start has been invoked; false otherwise
   */
  boolean isStarted();

  /**
   * close tester.
   */
  @Override
  void close();
}
