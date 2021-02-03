package org.folio.util;

import java.io.Closeable;

public interface PostgresTester extends Closeable {
  void start(String database, String username, String password);

  Integer getPort();

  String getHost();

  boolean isStarted();

  @Override
  void close();
}
