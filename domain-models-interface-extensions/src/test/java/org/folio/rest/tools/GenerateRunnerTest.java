package org.folio.rest.tools;

import org.apache.commons.io.FileUtils;
import org.folio.util.IoUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;

public class GenerateRunnerTest {
  private static String userDir = System.getProperty("user.dir");
  private static File jaxrs    = new File(userDir + "/src/main/java/org/folio/rest/jaxrs");
  private static File jaxrsBak = new File(userDir + "/src/main/java/org/folio/rest/jaxrs.bak");
  private String resourcesDir = userDir + "/src/test/resources/schemas";
  private String baseDir = userDir + "/target/GenerateRunnerTest";

  @BeforeClass
  public static void saveJaxrs() {
    jaxrs.renameTo(jaxrsBak);  // ignore any error
  }

  @AfterClass
  public static void restoreJaxrs() throws IOException {
    FileUtils.deleteDirectory(jaxrs);
    jaxrsBak.renameTo(jaxrs);  // ignore any error
  }

  @Before
  public void cleanDir() throws IOException {
    ClientGenerator.makeCleanDir(baseDir);
    System.clearProperty("raml_files");
    System.clearProperty("project.basedir");
  }

  private String jobJava() throws IOException {
    return IoUtil.toStringUtf8(System.getProperty("project.basedir", userDir)
        + "/src/main/java/org/folio/rest/jaxrs/model/Job.java");
  }

  private String testJava() throws IOException {
    return IoUtil.toStringUtf8(System.getProperty("project.basedir", userDir)
        + "/src/main/java/org/folio/rest/jaxrs/model/TestSchema.java");
  }

  private String msgsSchema() throws IOException {
    return IoUtil.toStringUtf8(System.getProperty("project.basedir", userDir)
        + "/target/classes/ramls/x/y/msgs.schema");
  }

  private String testSchema() throws IOException {
    return IoUtil.toStringUtf8(System.getProperty("project.basedir", userDir)
        + "/target/classes/ramls/test.schema");
  }

  private void assertTest() throws IOException {
    assertThat(testSchema(), containsString("\"name\""));
    assertThat(testJava(), allOf(
        containsString("String getName("),
        containsString("setName(String"),
        containsString("withName(String")));
  }

  @Test
  public void canRunMainDefaultDirs() throws Exception {
    GenerateRunner.main(null);
    assertTest();
  }

  private void assertJobMsgs() throws IOException {
    assertThat(jobJava(), allOf(
        containsString("String getModule("),
        containsString("setModule(String"),
        containsString("withModule(String")));
    assertThat(msgsSchema(), containsString("\"value\""));
  }

  @Test
  public void canRunMain() throws Exception {
    System.setProperty("raml_files", resourcesDir);
    System.setProperty("project.basedir", baseDir);
    FileUtils.copyDirectory(new File(resourcesDir), new File(baseDir + "/ramls/"));
    GenerateRunner.main(null);
    assertJobMsgs();
  }

  @Test
  public void canRunMainParentDir() throws Exception {
    System.setProperty("raml_files", resourcesDir);
    System.setProperty("project.basedir", baseDir + "/submodule");
    FileUtils.copyDirectory(new File(resourcesDir), new File(baseDir + "/ramls/"));
    GenerateRunner.main(null);
    assertJobMsgs();
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
    assertTest();
  }
}
