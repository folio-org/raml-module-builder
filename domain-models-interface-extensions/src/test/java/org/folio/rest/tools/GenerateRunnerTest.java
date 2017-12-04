package org.folio.rest.tools;

import org.apache.commons.io.FileUtils;
import org.folio.util.IoUtil;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;

public class GenerateRunnerTest {
  private String userDir = System.getProperty("user.dir");
  private String resourcesDir = userDir + "/src/test/resources/schemas";
  private String baseDir = userDir + "/target/GenerateRunnerTest";
  private String jobJava  = baseDir + "/src/main/java/org/folio/rest/jaxrs/model/Job.java";
  private String testJava = baseDir + "/src/main/java/org/folio/rest/jaxrs/model/TestSchema.java";
  private String messagesSchema = baseDir + "/target/classes/schemas/x/y/msgs.schema";

  @Before
  public void cleanDir() throws IOException {
    ClientGenerator.makeCleanDir(baseDir);
    System.clearProperty("raml_files");
    System.clearProperty("project.basedir");
  }

  @Test
  public void canRunMain() throws Exception {
    System.setProperty("raml_files", resourcesDir);
    System.setProperty("project.basedir", baseDir);
    GenerateRunner.main(null);
    assertThat(IoUtil.toStringUtf8(jobJava), allOf(
        containsString("String getModule("),
        containsString("setModule(String"),
        containsString("withModule(String")));
    assertThat(IoUtil.toStringUtf8(messagesSchema), containsString("\"value\""));
  }

  @Test(expected=IOException.class)
  public void invalidInputDirectory() throws Exception {
    new GenerateRunner(baseDir).generate(resourcesDir + "/job.schema");
  }

  @Test
  public void defaultRamlFilesDir() throws Exception {
    // create ramls dir at default location
    FileUtils.copyDirectory(new File(userDir + "/ramls/"), new File(baseDir + "/ramls/"));
    System.setProperty("project.basedir", baseDir);
    GenerateRunner.main(null);
    assertThat(IoUtil.toStringUtf8(testJava), allOf(
        containsString("String getName("),
        containsString("setName(String"),
        containsString("withName(String")));
  }
}
