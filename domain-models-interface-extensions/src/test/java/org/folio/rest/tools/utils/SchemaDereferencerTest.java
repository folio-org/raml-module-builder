package org.folio.rest.tools.utils;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.text.StringContainsInOrder.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;

import org.folio.util.IoUtil;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class SchemaDereferencerTest {
  private Path basePath = Paths.get("src/test/resources/schemas");

  private JsonObject dereferencedSchema(String inputFile) throws IOException {
    return new SchemaDereferencer().dereferencedSchema(basePath.resolve(inputFile));
  }

  private JsonObject readJson(String filename) throws IOException {
    File file = basePath.resolve(filename).toFile();
    try (InputStream reader = new FileInputStream(file)) {
      return new JsonObject(IoUtil.toStringUtf8(reader));
    }
  }

  @Test
  @Parameters({"parameters.schema, parameters.schema",
               "message.schema, message.schema.deref",
               "messages.schema, messages.schema.deref",
               "a/b/message.schema, message.schema.deref",
               "x/y/messages.schema, messages.schema.deref",
               "usergroups.json, usergroups.json.deref",
              })
  public void deref(String inputFile, String expectedFile) throws IOException {
    assertThat(dereferencedSchema(inputFile), is(readJson(expectedFile)));
  }

  @Test
  public void loop() throws IOException {
    try {
      dereferencedSchema("../badschemas/loop.schema");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), allOf(
          startsWith("$ref chain has a loop: "),
          stringContainsInOrder("loop.schema", "loop.schema")));
    }
  }

  @Test
  public void loop1() throws IOException {
    try {
      dereferencedSchema("../badschemas/loop1.schema");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), allOf(
          startsWith("$ref chain has a loop: "),
          stringContainsInOrder("loop1.schema", "loop.schema")));
    }
  }

  @Test
  public void loopA() throws IOException {
    try {
      dereferencedSchema("../badschemas/loop_a.schema");
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), allOf(
          startsWith("$ref chain has a loop: "),
          stringContainsInOrder("loop_a.schema", "loop_b.schema", "loop_a.schema")));
    }
  }

  @Test
  public void fileNotFound() {
    String filename = "non-existing.file";
    try {
      new SchemaDereferencer().dereferencedSchema(Paths.get(filename));
      fail();
    } catch (IOException e) {
      assertThat(e.getMessage(), containsString(filename));
    }
  }

  @Test
  public void cache() throws IOException {
    SchemaDereferencer dereferencer = new SchemaDereferencer() {
      private Deque<Path> stack = new ArrayDeque<>();

      @Override
      public JsonObject dereferencedSchema(Path path) throws IOException {
        // first invocation: use a proper stack.
        // all following invocations: stack=null
        Deque<Path> oldStack = stack;
        stack = null;
        return dereferencedSchema(path, oldStack);
      }
    };

    // read the file
    dereferencer.dereferencedSchema(basePath.resolve("parameters.schema"));
    // use the cache (if it doesn't then NullPointerException)
    dereferencer.dereferencedSchema(basePath.resolve("parameters.schema"));
    try {
      // prove that not reading from cache causes NullPointerException
      dereferencer.dereferencedSchema(basePath.resolve("message.schema"));
      fail();
    } catch (NullPointerException e) {
      // null stack
    }
  }
}
