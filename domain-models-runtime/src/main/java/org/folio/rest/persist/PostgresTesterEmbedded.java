package org.folio.rest.persist;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.util.PostgresTester;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;
import ru.yandex.qatools.embed.postgresql.distribution.Version;

/**
 * This implements testing with EmbeddedPostgres.
 * @deprecated will be removed in next major version; replaced with {@link org.folio.postgres.testing.PostgresTesterContainer}.
 */
@Deprecated
class PostgresTesterEmbedded implements PostgresTester {
  private static Logger log = LogManager.getLogger(PostgresTesterEmbedded.class);
  private EmbeddedPostgres embeddedPostgres;
  private final int embeddedPort;

  PostgresTesterEmbedded(int embeddedPort) {
    this.embeddedPort = embeddedPort;
  }

  @Override
  public void start(String database, String username, String password) {
    if (!NetworkUtils.isLocalPortFree(embeddedPort)) {
      log.info("Port {} is already in use, assume embedded Postgres is already running", embeddedPort);
      return;
    }
    log.info("Starting embedded postgres on port {}", embeddedPort);
    String locale = "en_US.UTF-8";
    String operatingSystem = System.getProperty("os.name").toLowerCase();
    if (operatingSystem.contains("win")) {
      locale = "american_usa";
    }
    embeddedPostgres = new EmbeddedPostgres(Version.Main.V10);
    try {
      embeddedPostgres.start("localhost", embeddedPort, database, username, password,
          Arrays.asList("-E", "UTF-8", "--locale", locale));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Integer getPort() {
    return embeddedPort;
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
