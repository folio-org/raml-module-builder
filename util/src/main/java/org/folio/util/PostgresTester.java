package org.folio.util;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public interface PostgresTester extends Closeable {
  /**
   * start tester.
   * @param database Postgres database
   * @param username Postgres username
   * @param password Postgres password
   */
  void start(String database, String username, String password) throws SQLException, IOException, InterruptedException;

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
