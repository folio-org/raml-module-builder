package org.folio.rest.tools.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.folio.util.IoUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.json.JsonObject;

public class RamlDirCopierTest {
  private Path source = Paths.get("src/test/resources/schemas");
  private Path target = Paths.get("target/RamlDirCopierTest");

  @Before
  @After
  public void deleteTarget() throws IOException {
    if (! target.toFile().exists()) {
      return;
    }
    Files.walk(target)
      .map(Path::toFile)
      .sorted(Collections.reverseOrder())  // delete the dir's content before deleting the dir
      .forEach(File::delete);
  }

  private JsonObject readJson(Path path) throws IOException {
    try (InputStream reader = new FileInputStream(path.toFile())) {
      return new JsonObject(IoUtil.toStringUtf8(reader));
    }
  }

  private void assertJsonEquals(String filename1, String filename2) throws IOException {
    JsonObject json1 = readJson(target.resolve(filename1));
    JsonObject json2 = readJson(source.resolve(filename2));
    assertThat(filename1 + " and " + filename2, json1, is(json2));
  }

  @Test
  public void test() throws IOException {
    RamlDirCopier.copy(source, target);
    assertJsonEquals("message.schema", "message.schema.deref");
    assertJsonEquals("a/b/message.schema", "message.schema.deref");
    assertJsonEquals("x/y/messages.schema", "messages.schema.deref");
  }

}
