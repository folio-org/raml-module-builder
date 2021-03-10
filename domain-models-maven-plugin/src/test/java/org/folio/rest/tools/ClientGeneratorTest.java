package org.folio.rest.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.regex.Pattern;
import org.folio.util.ResourceUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClientGeneratorTest {

  private static final Pattern TRAILING_SPACE_PATTERN = Pattern.compile("\\s+", Pattern.MULTILINE);

  private static String sourceDir;

  @Before
  public void setUp() {
    System.setProperty("client.generate", "true");
    System.setProperty("project.basedir", ".");
    sourceDir = System.getProperties().getProperty("project.basedir")
      + ClientGenerator.PATH_TO_GENERATE_TO
      + AnnotationGrabber.CLIENT_GEN_PACKAGE.replace('.', '/');
  }

  @After
  public void cleanUp() throws IOException {
    System.clearProperty("client.generate");
    System.clearProperty("project.basedir");
    ClientGenerator.makeCleanDir(sourceDir);
  }

  @Test
  public void doesGenerateTestResourceClient() throws Exception {
    ClientGenerator.main(null);
    File expectedClient = new File(sourceDir + "/TestResourceClient.java");
    Assert.assertTrue(expectedClient.exists());

    // IDEs always removes trailing spaces from edited files, but java code generator adds then,
    // so we need to remove them before the comparison

    String actual = removeTrailingSpaces(Files.readString(expectedClient.toPath()).trim());

    String expected = removeTrailingSpaces(ResourceUtil.asString("/clients/TestClient.txt", this.getClass()));

    Assert.assertEquals(expected, actual);
  }


  private String removeTrailingSpaces(String str) {
    return TRAILING_SPACE_PATTERN.matcher(str).replaceAll("");
  }

}
