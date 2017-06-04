package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.vertx.core.json.JsonObject;

public class EnvsTest {
  @Before
  public void setUp() {
    Map<String, String> map = new HashMap<>();
    map.put("DB_HOST", "example.com");
    map.put("DB_QUERYTIMEOUT", "8");
    map.put("DB_MAXPOOLSIZE", "5");
    // deprecated key db.username for allDBConfs()
    map.put("db.username", "superwoman");
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
  public void database() {
    assertNull(Envs.getEnv(Envs.DB_DATABASE));
  }

  @Test
  public void allDBConfs() {
    JsonObject json = Envs.allDBConfs();
    assertEquals(4, json.size());
    assertEquals("example.com", json.getValue("host"));
    assertEquals(Integer.valueOf(8), json.getValue("queryTimeout"));
    assertEquals(Integer.valueOf(5), json.getValue("maxPoolSize"));
    assertEquals("superwoman", json.getValue("username"));
  }

}
