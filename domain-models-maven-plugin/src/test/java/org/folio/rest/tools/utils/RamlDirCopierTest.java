package org.folio.rest.tools.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.folio.rest.tools.ClientGenerator;
import org.folio.util.IoUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import io.vertx.core.json.JsonObject;

public class RamlDirCopierTest {
  private Path source = Paths.get("src/test/resources/schemas");
  private Path target = Paths.get("target/RamlDirCopierTest");

  @Before
  @After
  public void deleteTarget() throws IOException {
    ClientGenerator.makeCleanDir(target.toString());
  }

  private JsonObject readJson(Path path) throws IOException {
    try (InputStream reader = new FileInputStream(path.toFile())) {
      return new JsonObject(IoUtil.toStringUtf8(reader));
    }
  }

  private void assertJsonExist(String filename)  throws IOException {
    JsonObject json1 = readJson(target.resolve(filename));
    JsonObject json2 = readJson(source.resolve(filename));
  }

  private void assertSchemas() throws IOException {
    assertJsonExist("message.schema");
    assertJsonExist("a/b/msg.schema");
    assertJsonExist("x/y/msgs.schema");
    assertJsonExist("usergroups.json");
  }

  @Test
  public void plain() throws IOException {
    RamlDirCopier.copy(source, target);
    assertSchemas();
  }

  @Test
  public void directoryCreation() throws IOException {
    target.toFile().delete();
    assertThat(target.toFile().exists(), is(false));
    RamlDirCopier.copy(source, target);
    assertSchemas();
  }

  @Test
  public void overwriteOnSecondRun() throws IOException {
    RamlDirCopier.copy(source, target);
    RamlDirCopier.copy(source, target);
    assertSchemas();
  }

}
