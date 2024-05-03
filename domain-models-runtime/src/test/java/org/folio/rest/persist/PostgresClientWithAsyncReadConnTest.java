package org.folio.rest.persist;

import io.vertx.core.Vertx;
import org.folio.rest.tools.utils.Envs;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class PostgresClientWithAsyncReadConnTest {

  @Test
  public void getInstanceTest() {
    Envs.setEnv(Map.of(
        "DB_HOST_READER_ASYNC", "myhost_reader_async",
        "DB_PORT_READER_ASYNC", "54321",
        "DB_HOST_READER", "myhost_reader",
        "DB_PORT_READER", "12345",
        "DB_HOST", "myhost",
        "DB_PORT", "5433",
        "DB_USERNAME", "myuser",
        "DB_PASSWORD", "mypassword",
        "DB_DATABASE", "mydatabase",
        "DB_CONNECTIONRELEASEDELAY", "1000"
    ));
    var vertx = Vertx.vertx();
    var tenant = "testTenant";
    var client = PostgresClientWithAsyncReadConn.getInstance(vertx, tenant);
    var initializer = client.getPostgresClientInitializer();
    assertNotNull(client);
    assertNotNull(client.getReaderClient());
    assertNotEquals(client.getClient(), client.getReaderClient());
    assertEquals(client.getReaderClient(), initializer.getReadClientAsync());
  }
}
