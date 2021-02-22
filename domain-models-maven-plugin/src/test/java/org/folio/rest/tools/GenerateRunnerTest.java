package org.folio.rest.tools;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.folio.util.IoUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GenerateRunnerTest {

  private static String userDir = System.getProperty("user.dir");
  private static String jaxrsDir = "/target/generated-sources/raml-jaxrs/org/folio/rest/jaxrs";
  private static File jaxrs    = new File(userDir + jaxrsDir);
  private static File jaxrsBak = new File(userDir + jaxrsDir + ".bak");
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

  @After
  @Before
  public void cleanDir() throws IOException {
    ClientGenerator.makeCleanDir(baseDir);
    System.clearProperty("raml_files");
    System.clearProperty("project.basedir");
    System.clearProperty("maven.multiModuleProjectDirectory");
  }

  private String jobJava() throws IOException {
    return IoUtil.toStringUtf8(System.getProperty("project.basedir", userDir)
        + jaxrsDir + "/model/Job.java");
  }

  private String testJava() throws IOException {
    return IoUtil.toStringUtf8(System.getProperty("project.basedir", userDir)
        + jaxrsDir + "/model/Test.java");
  }

  private String elementAnnotationTestJava() throws IOException {
    return IoUtil.toStringUtf8(System.getProperty("project.basedir", userDir)
        + jaxrsDir + "/model/ElementAnnotationTest.java");
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
    assertThat(msgsSchema(), containsString("\"messages\""));
  }

  private void assertElementAnnotations() throws IOException {
    assertThat(elementAnnotationTestJava(), allOf(
        containsString("@ElementsPattern(regexp = \".\")"),
        containsString("@ElementsNotNull")));
  }

  @Test
  public void canRunMain() throws Exception {
    System.setProperty("raml_files", resourcesDir);
    System.setProperty("project.basedir", baseDir);
    FileUtils.copyDirectory(new File(resourcesDir), new File(baseDir + "/ramls/"));
    GenerateRunner.main(null);
    assertJobMsgs();
    assertElementAnnotations();
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
    System.setProperty("maven.multiModuleProjectDirectory", baseDir + "/foobar");
    GenerateRunner.main(null);
    assertTest();
  }

  @Test
  public void testPropertiesForConfiguringSchemaGeneration() throws Exception {
    System.setProperty("project.basedir", baseDir);
    System.setProperty("jsonschema2pojo.config.includeToString", "true");
    System.setProperty("jsonschema2pojo.config.includeHashcodeAndEquals", "true");
    GenerateRunner.main(null);
    assertThat(testJava(), allOf(
      containsString("public String toString"),
      containsString("public int hashCode"),
      containsString("public boolean equals")));
  }

  @Test
  public void testCreateRamlsLookupList() throws Exception {
    List<String> actualRamls = testCreateLookupList(GenerateRunner.RAML_LIST, Collections.singletonList(".raml"));
    Assert.assertThat(actualRamls, containsInAnyOrder("test.raml"));
  }

  @Test
  public void testCreateJsonSchemasLookupList() throws Exception {
    List<String> actualJsonSchemas = testCreateLookupList(GenerateRunner.JSON_SCHEMA_LIST, Arrays.asList(".json", ".schema"));
    Assert.assertThat(actualJsonSchemas, containsInAnyOrder("test.schema", "object.json"));
  }

  @Test
  public void testCreateJsonSchemasLookupListFromSubfolderNotRecursively() throws Exception {
    List<String> actualJsonSchemas = testCreateLookupList(GenerateRunner.JSON_SCHEMA_LIST, Arrays.asList(".json", ".schema"),
      Collections.singletonList("test/test2"));
    Assert.assertThat(actualJsonSchemas, containsInAnyOrder("test/test2/test.schema"));
  }

  @Test
  public void testCreateJsonSchemasLookupListFromSubfolderRecursively() throws Exception {
    List<String> actualJsonSchemas = testCreateLookupList(GenerateRunner.JSON_SCHEMA_LIST, Arrays.asList(".json", ".schema"),
      Collections.singletonList("test/test2/**"));
    Assert.assertThat(actualJsonSchemas, containsInAnyOrder("test/test2/test.schema", "test/test2/sub/test.schema"));
  }

  @Test
  public void testCreateJsonSchemasLookupListFromMultipleSubfolders() throws Exception {
    List<String> actualJsonSchemas = testCreateLookupList(GenerateRunner.JSON_SCHEMA_LIST, Arrays.asList(".json", ".schema"),
      Arrays.asList("test/test1", "test/test2/**"));
    Assert.assertThat(actualJsonSchemas,
      containsInAnyOrder("test/test2/test.schema", "test/test2/sub/test.schema",
        "test/test1/test1.schema", "test/test1/test2.schema"));
  }

  @Test
  public void testCreateJsonSchemasLookupListFromCommonSubfolderRecursively() throws Exception {
    List<String> actualJsonSchemas = testCreateLookupList(GenerateRunner.JSON_SCHEMA_LIST, Arrays.asList(".json", ".schema"),
      Collections.singletonList("test/**"));
    Assert.assertThat(actualJsonSchemas,
      containsInAnyOrder("test/test2/test.schema", "test/test2/sub/test.schema",
        "test/test1/test1.schema", "test/test1/test2.schema"));
  }

  @Test
  public void testCreateJsonSchemasLookupListFromSubfolderNonRecursively() throws Exception {
    List<String> actualJsonSchemas = testCreateLookupList(GenerateRunner.JSON_SCHEMA_LIST, Arrays.asList(".json", ".schema"),
      Collections.singletonList("test"));
    Assert.assertThat(actualJsonSchemas, is(empty()));
  }

  private List<String> testCreateLookupList(String filename, List<String> exts) throws IOException {
    return testCreateLookupList(filename, exts, Collections.singletonList(""));
  }

  private List<String> testCreateLookupList(String filename, List<String> exts, List<String> subfolders) throws IOException {
    File src = new File(userDir + "/ramls/");
    assertTrue(src.exists() && src.isDirectory());
    File dest = new File(userDir + "/target/ramls/");
    FileUtils.copyDirectory(src, dest);
    GenerateRunner.createLookupList(dest, filename, exts, subfolders);
    String schemas = FileUtils.readFileToString(new File(dest, filename), StandardCharsets.UTF_8);
    if(schemas.isEmpty()) {
      return Collections.emptyList();
    }
    else{
      return Arrays.asList(schemas.split("\\r?\\n"));
    }
  }
}
