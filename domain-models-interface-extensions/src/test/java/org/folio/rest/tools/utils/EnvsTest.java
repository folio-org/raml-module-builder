package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.vertx.core.json.JsonObject;

public class EnvsTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    Map<String, String> map = new HashMap<>();
    map.put("DB_HOST", "example.com");
    map.put("DB_QUERYTIMEOUT", "8");
    map.put("DB_MAXPOOLSIZE", "5");
    map.put("DB_CONNECTION_RELEASE_DELAY", "12345");
    map.put("DB_EXPLAIN_QUERY_THRESHOLD", "100");
    // we dropped support for dot form. check that it is ignored
    map.put("db.username", "superwoman");
    map.put("DB.USERNAME", "superwoman");
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
    assertEquals("12345", Envs.getEnv(Envs.DB_CONNECTION_RELEASE_DELAY));
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
    assertEquals(Long.valueOf(100), json.getValue(Envs.DB_EXPLAIN_QUERY_THRESHOLD.name()));
  }

  @Test
  public void numberFormatException() {
    Envs.setEnv(Collections.singletonMap("DB_PORT", "qqq"));

    thrown.expect(NumberFormatException.class);
    thrown.expectMessage("DB_PORT");
    thrown.expectMessage("qqq");

    Envs.allDBConfs();
  }

  @Test
  public void explainThresholdException() {
    Envs.setEnv(Collections.singletonMap("DB_EXPLAIN_QUERY_THRESHOLD", "qqq"));

    thrown.expect(NumberFormatException.class);
    thrown.expectMessage("DB_EXPLAIN_QUERY_THRESHOLD");
    thrown.expectMessage("qqq");

    Envs.allDBConfs();
  }

}
