package org.folio.rest.tools.utils;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.condition.OS.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.vertx.core.json.JsonObject;

class SchemaDereferencerTest {
  @ParameterizedTest
  @ValueSource(strings = {
      "{ \"$ref\": 1 }",              // number
      "{ \"$ref\": true }",           // boolean
      "{ \"$ref\": { \"foo\" : \"bar\" } }", // JSON Object
  })
  void fixupRefInvalid(String json) {
    Exception exception = assertThrows(ClassCastException.class,
        () -> SchemaDereferencer.fixupRef(new File("/").toPath(), new JsonObject(json)));
    // $ref must be of type String
    assertThat(exception.getMessage(), allOf(containsString("$ref"), containsString("String")));
  }

  @Test
  void fixupRef() throws IOException {
    JsonObject json = new JsonObject("{ \"$ref\": \"dir/a.json\", "
        + "\"b\": { \"$ref\": \"file:///tmp/b.json\" }, "
        + "\"c\": { \"$ref\": \"dir/c.json\" } }");
    SchemaDereferencer.fixupRef(new File("/home/peter").toPath(), json);
    assertThat(json.encodePrettily(), json.getString("$ref"), allOf(
        containsString("file:///"), containsString("dir"), containsString("a.json")));
    assertThat(json.encodePrettily(), json.getJsonObject("b").getString("$ref"), is("file:///tmp/b.json"));
    assertThat(json.encodePrettily(), json.getJsonObject("c").getString("$ref"), allOf(
        containsString("file:///"), containsString("dir"), containsString("c.json")));
  }

  @EnabledOnOs(LINUX)
  @ParameterizedTest
  @CsvSource({
    "dir/a.json    , file:///home/dir/a.json",
    "./dir/a.json  , file:///home/dir/a.json",
    "././dir/a.json, file:///home/dir/a.json",
    "../dir/a.json,  file:///dir/a.json",
  })
  void toFileUriLinux(String file, String expectedUri) {
    Path basePathFile = new File("/home/peter.json").toPath();
    assertThat(SchemaDereferencer.toFileUri(basePathFile, file), is(expectedUri));
  }

  @EnabledOnOs(WINDOWS)
  @ParameterizedTest
  @CsvSource({
    "dir\\a.json,       file:///C:/Users/dir/a.json",
    ".\\dir\\a.json,    file:///C:/Users/dir/a.json",
    ".\\.\\dir\\a.json, file:///C:/Users/dir/a.json",
    "..\\dir\\a.json,   file:///C:/dir/a.json",
  })
  void toFileUriWin(String file, String expectedUri) {
    Path basePathFile = new File("C:\\Users\\peter.json").toPath();
    assertThat(SchemaDereferencer.toFileUri(basePathFile, file), is(expectedUri));
  }
}
