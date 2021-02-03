package org.folio.util;

public interface PostgresTester {
  void start(String database, String username, String password);

  int getPort();

  String getHost();

  boolean isStarted();

  boolean stop();
}
