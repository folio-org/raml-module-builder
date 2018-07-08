package org.folio.rest.tools.utils;

import java.nio.file.Path;
import java.nio.file.Paths;

public class RamlDirCopierTest {
  private Path source = Paths.get("src/test/resources/schemas");
  private Path target = Paths.get("target/RamlDirCopierTest");

/*  @Before
  @After
  public void deleteTarget() throws IOException {
    ClientGenerator.makeCleanDir(target.toString());
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

  private void assertSchemas() throws IOException {
    assertJsonEquals("message.schema",  "message.schema.deref");
    assertJsonEquals("a/b/msg.schema",  "msg.schema.deref");
    assertJsonEquals("x/y/msgs.schema", "msgs.schema.deref");
    assertJsonEquals("usergroups.json", "usergroups.json.deref");
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
  }*/

}
