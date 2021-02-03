package org.folio.rest.persist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.util.PostgresTester;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;
import ru.yandex.qatools.embed.postgresql.distribution.Version;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

public class PostgresTesterEmbedded implements PostgresTester {
  private static final int EMBEDDED_POSTGRES_PORT = 6000;
  private static Logger log = LogManager.getLogger(PostgresTesterEmbedded.class);
  private EmbeddedPostgres embeddedPostgres;

  @Override
  public void start(String database, String username, String password) {
    if (!NetworkUtils.isLocalPortFree(EMBEDDED_POSTGRES_PORT)) {
      log.info("Port {} is already in use, assume embedded Postgres is already running", EMBEDDED_POSTGRES_PORT);
      return;
    }
    log.info("Starting embedded postgres on port {}", EMBEDDED_POSTGRES_PORT);
    String locale = "en_US.UTF-8";
    String operatingSystem = System.getProperty("os.name").toLowerCase();
    if (operatingSystem.contains("win")) {
      locale = "american_usa";
    }
    embeddedPostgres = new EmbeddedPostgres(Version.Main.V10);
    try {
      embeddedPostgres.start("localhost", EMBEDDED_POSTGRES_PORT, database, username, password,
          Arrays.asList("-E", "UTF-8", "--locale", locale));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Integer getPort() {
    return EMBEDDED_POSTGRES_PORT;
  }

  @Override
  public String getHost() {
    return "localhost";
  }

  @Override
  public boolean isStarted() {
    return embeddedPostgres != null;
  }

  public void close() {
    if (embeddedPostgres == null) {
      return;
    }
    embeddedPostgres.stop();
    embeddedPostgres = null;
  }
}
