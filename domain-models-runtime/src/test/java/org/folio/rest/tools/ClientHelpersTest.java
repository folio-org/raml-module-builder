package org.folio.rest.tools;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.Test;

class ClientHelpersTest implements WithAssertions {
  @Test
  void nullException() {
    assertThatThrownBy(() -> ClientHelpers.pojo2json(null))
    .hasMessageContaining("null");
  }

  @Test
  void jsonProcessingException() {
    assertThatThrownBy(() -> ClientHelpers.pojo2json(new Object() {}))
    .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void json() {
    JsonObject json = new JsonObject().put("abc", "x\"y\"z").put("array", new JsonArray());
    JsonObject json2 = new JsonObject(ClientHelpers.pojo2json(json));
    assertThat(json2).isEqualTo(json);
  }

  @Test
  void pojo() {
    class Foo {
      @JsonProperty String bar;
      @JsonProperty String baz;
    }
    Foo foo = new Foo();
    foo.bar = "1";
    foo.baz = "2";
    JsonObject json = new JsonObject(ClientHelpers.pojo2json(foo));
    assertThat(json).isEqualTo(new JsonObject().put("bar", "1").put("baz", "2"));
  }


}
