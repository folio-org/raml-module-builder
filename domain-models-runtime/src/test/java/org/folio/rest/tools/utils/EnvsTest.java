package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.json.JsonObject;

public class EnvsTest {
  @AfterClass
  public static void restoreEnv() {
    Envs.setEnv(System.getenv());
  }

  @Before
  public void setUp() {
    Map<String, String> map = new HashMap<>();
    map.put("DB_HOST", "example.com");
    map.put("DB_QUERYTIMEOUT", "8");
    map.put("DB_MAXPOOLSIZE", "5");
    map.put("DB_CONNECTIONRELEASEDELAY", "12345");
    map.put("DB_EXPLAIN_QUERY_THRESHOLD", "100");
    // we dropped support for dot form. check that it is ignored
    map.put("db.username", "superwoman");
    map.put("DB.USERNAME", "superwoman");
    map.put("DB_RUNNER_PORT", "5444");  // not used by PostgresClient
    Envs.setEnv(Collections.unmodifiableMap(map));
  }

  @Test
  public void host() {
    assertEquals("example.com", Envs.getEnv(Envs.DB_HOST));
  }

  @Test
  public void user() {
    assertNull(Envs.getEnv(Envs.DB_USERNAME));  // because db.username
  }

  @Test
  public void querytimeout() {
    assertEquals("8", Envs.getEnv(Envs.DB_QUERYTIMEOUT));
  }

  @Test
  public void maxpoolsize() {
    assertEquals("5", Envs.getEnv(Envs.DB_MAXPOOLSIZE));
  }

  @Test
  public void connectionReleaseDelay() {
    assertEquals("12345", Envs.getEnv(Envs.DB_CONNECTIONRELEASEDELAY));
  }

  @Test
  public void database() {
    assertNull(Envs.getEnv(Envs.DB_DATABASE));
  }

  @Test
  public void threshold() {
    assertEquals("100", Envs.getEnv(Envs.DB_EXPLAIN_QUERY_THRESHOLD));
  }

  @Test
  public void allDBConfs() {
    JsonObject json = Envs.allDBConfs();
    assertEquals(5, json.size());
    assertEquals("example.com", json.getValue("host"));
    assertEquals(Integer.valueOf(8), json.getValue("queryTimeout"));
    assertEquals(Integer.valueOf(5), json.getValue("maxPoolSize"));
    assertEquals(Integer.valueOf(12345), json.getValue("connectionReleaseDelay"));
    assertEquals(Long.valueOf(100), json.getValue(Envs.DB_EXPLAIN_QUERY_THRESHOLD.name()));
  }

  @Test
  public void set5Envs() {
    Envs.setEnv("127.0.0.1", 5432, "foo", "bar", "devnull");
    assertEquals("127.0.0.1", Envs.getEnv(Envs.DB_HOST));
    assertEquals("5432", Envs.getEnv(Envs.DB_PORT));
    assertEquals("foo", Envs.getEnv(Envs.DB_USERNAME));
    assertEquals("bar", Envs.getEnv(Envs.DB_PASSWORD));
    assertEquals("devnull", Envs.getEnv(Envs.DB_DATABASE));
    assertEquals(null, Envs.getEnv(Envs.DB_MAXPOOLSIZE));
  }

  @Test
  public void numberFormatException() {
    Envs.setEnv(Collections.singletonMap("DB_PORT", "qqq"));
    Exception e = Assert.assertThrows(NumberFormatException.class, () -> Envs.allDBConfs());
    assertTrue(e.getMessage().contains("DB_PORT"));
    assertTrue(e.getMessage().contains("qqq"));
  }

  @Test
  public void explainThresholdException() {
    Envs.setEnv(Collections.singletonMap("DB_EXPLAIN_QUERY_THRESHOLD", "qqq"));
    Exception e = Assert.assertThrows(NumberFormatException.class, () -> Envs.allDBConfs());
    assertTrue(e.getMessage().contains("DB_EXPLAIN_QUERY_THRESHOLD"));
    assertTrue(e.getMessage().contains("qqq"));
  }
}
