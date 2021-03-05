package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import io.vertx.core.json.JsonObject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.folio.rest.testing.UtilityClassTester;
import org.junit.jupiter.api.Test;

public class LoadConfsTest {

  @Test
  void utilityClass() {
    UtilityClassTester.assertUtilityClass(LoadConfs.class);
  }

  @Test
  void loadConfig() throws IOException {
    File file = Files.createTempFile("LoadConfsTest", ".json").toFile();
    Files.write(file.toPath(), "{}".getBytes());
    String path = file.getAbsolutePath();
    assertThat(LoadConfs.loadConfig(path), is(new JsonObject()));
    assertThat(LoadConfs.loadConfig(path.substring(1)), is(nullValue()));
    assertThat(LoadConfs.loadConfig("/nonexisting/absolute/path.json"), is(nullValue()));
    assertThat(LoadConfs.loadConfig("/log4j2.properties"), is(nullValue()));
    assertThat(LoadConfs.loadConfig("/my-postgres-conf.json").toString(), containsString("5433"));
  }
}
