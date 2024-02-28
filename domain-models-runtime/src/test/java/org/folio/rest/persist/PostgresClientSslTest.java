package org.folio.rest.persist;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.tools.utils.Envs;
import org.folio.util.ResourceUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Timeout(5000)
@Testcontainers
@ExtendWith(VertxExtension.class)
class PostgresClientSslTest {

  static final String KEY_PATH = "/var/lib/postgresql/data/server.key";
  static final String CRT_PATH = "/var/lib/postgresql/data/server.crt";
  static final String CONF_PATH = "/var/lib/postgresql/data/postgresql.conf";
  static final String CONF_BAK_PATH = "/var/lib/postgresql/data/postgresql.conf.bak";
  @Container
  static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>(PostgresTesterContainer.DEFAULT_IMAGE_NAME);

  static void exec(String... command) {
    try {
      POSTGRES.execInContainer(command);
    } catch (InterruptedException | IOException | UnsupportedOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Configure the connection to check the server certificate against crtFile.
   * Append each of the configEntries to postgresql.conf and reload it into postgres.
   * Appending a key=value entry has precedence over any previous entries of the same key.
   */
  static void configure(String crtFile, String... configEntries) {
    String crtString = crtFile == null ? null : ResourceUtil.asString("ssl/" + crtFile);
    Envs.setEnv(config(crtString));

    exec("cp", "-p", CONF_BAK_PATH, CONF_PATH);  // start with unaltered config
    for (String configEntry : configEntries) {
      exec("sh", "-c", "echo '" + configEntry + "' >> " + CONF_PATH);
    }
    exec("su-exec", "postgres", "pg_ctl", "reload");
  }

  static private Map<String,String> config(String serverCrt) {
    Map<String,String> map = new HashMap<>(6);
    map.put("DB_HOST", POSTGRES.getHost());
    map.put("DB_PORT", POSTGRES.getFirstMappedPort() + "");
    map.put("DB_DATABASE", POSTGRES.getDatabaseName());
    map.put("DB_USERNAME", POSTGRES.getUsername());
    map.put("DB_PASSWORD", POSTGRES.getPassword());
    if (serverCrt != null) {
      map.put("DB_SERVER_PEM", serverCrt);
    }
    return map;
  }

  @BeforeAll
  static void beforeAll() {
    MountableFile serverKeyFile = MountableFile.forClasspathResource("ssl/server.key");
    MountableFile serverCrtFile = MountableFile.forClasspathResource("ssl/server.crt");
    POSTGRES.copyFileToContainer(serverKeyFile, KEY_PATH);
    POSTGRES.copyFileToContainer(serverCrtFile, CRT_PATH);
    exec("chown", "postgres.postgres", KEY_PATH, CRT_PATH);
    exec("chmod", "400", KEY_PATH, CRT_PATH);

    exec("cp", "-p", CONF_PATH, CONF_BAK_PATH);
  }

  @AfterAll
  static void afterAll() {
    Envs.setEnv(System.getenv());
  }

  @Test
  @DisplayName("Basic connectivity test without encryption")
  void connectWithoutSsl(Vertx vertx, VertxTestContext vtc) throws Exception {
    configure(null, "ssl = off");
    PostgresClient.getInstance(vertx).getConnection().onComplete(vtc.succeedingThenComplete());
  }

  @Test
  void rejectWhenServerWithoutSsl(Vertx vertx, VertxTestContext vtc) throws Exception {
    configure("server.crt", "ssl = off");
    PostgresClient.getInstance(vertx).getConnection().onComplete(vtc.failingThenComplete());
  }

  @Test
  void tlsv1_3(Vertx vertx, VertxTestContext vtc) throws Exception {
    configure("server.crt", "ssl = on");
    PostgresClient.getInstance(vertx).getConnection().onComplete(vtc.succeeding(connection -> {
      assertThat(connection.isSSL(), is(true));
      connection.query("SELECT version FROM pg_stat_ssl WHERE pid = pg_backend_pid()").execute()
      .onComplete(vtc.succeeding(rowset -> {
        assertThat(rowset.iterator().next().getString(0), is("TLSv1.3"));
        vtc.completeNow();
      }));
    }));
  }

  @Test
  void rejectTlsv1_2(Vertx vertx, VertxTestContext vtc) throws Exception {
    configure("server.crt", "ssl = on", "ssl_min_protocol_version = TLSv1.2", "ssl_max_protocol_version = TLSv1.2");
    PostgresClient.getInstance(vertx).getConnection().onComplete(vtc.failing(e -> {
      assertThat(e.getMessage(), containsString("handshake failed"));
      vtc.completeNow();
    }));
  }

  @Test
  void rejectWrongHostname(Vertx vertx, VertxTestContext vtc) throws Exception {
    configure("server-folio.org.crt", "ssl = on");
    PostgresClient.getInstance(vertx).getConnection().onComplete(vtc.failing(e -> {
      assertThat(e.getMessage(), containsString("handshake failed"));
      vtc.completeNow();
    }));
  }
}
