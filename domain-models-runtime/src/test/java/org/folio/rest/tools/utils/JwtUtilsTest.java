package org.folio.rest.tools.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import io.vertx.core.json.JsonObject;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.folio.rest.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;

class JwtUtilsTest {
  @Test
  void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(JwtUtils.class);
  }

  @Test
  void getJson() {
    assertThat(JwtUtils.getJson("cGxlYXN1cmUu"), is("pleasure."));
  }

  @Test
  void get() {
    String json = new JsonObject().put("key1", "value1").put("key2", "value2").put("key3", 3).encode();
    String token = "header." + Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8)) + ".signature";
    assertThat(JwtUtils.get("key1", token), is("value1"));
    assertThat(JwtUtils.get("key2", token), is("value2"));
    assertThat(JwtUtils.get("key3", token), is("3"));  // check Integer to String conversion
    assertThat(JwtUtils.get("key4", token), is(nullValue()));
  }

  @Test
  void getFromNullToken() {
    assertThat(JwtUtils.get("key", null), is(nullValue()));
  }

  @Test
  void getFromUnparsableToken() {
    assertThat(JwtUtils.get("key", "header.unparsable.signature"), is(nullValue()));
  }
}
